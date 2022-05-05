#include <iostream>
#include <chrono>
#include <random>
#include "tbb/tbb.h"
#include "plog.cpp"


int (*which_memalign)(void **memptr, size_t alignment, size_t size);
void (*which_memfree)(void *ptr);
void *(*which_malloc)(size_t size);
void (*which_free)(void *ptr);

#define die(msg, args...) \
   do {                         \
      fprintf(stderr,"(%s,%d) " msg "\n", __FUNCTION__ , __LINE__, ##args); \
      exit(-1); \
   } while(0)

using namespace std;

#include "masstree.h"
#include "ralloc.hpp"

inline int RP_memalign(void **memptr, size_t alignment, size_t size){
    *memptr=RP_malloc(size+(alignment-size%alignment));
    return 0;
}

//static constexpr uint64_t CACHE_LINE_SIZE = 64;

static inline void clflush(char *data, int len, bool front, bool back)
{
    volatile char *ptr = (char *)((unsigned long)data &~(CACHE_LINE_SIZE-1));
    if (front)
        asm volatile("sfence":::"memory");
    for(; ptr<data+len; ptr+=CACHE_LINE_SIZE){
#ifdef CLFLUSH
        asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
#elif CLFLUSH_OPT
        asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(ptr)));
#elif CLWB
        asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(ptr)));
#endif
    }
    if (back)
        asm volatile("sfence":::"memory");
}


static long items; //initialized in init_zipf_generator function
static long base; //initialized in init_zipf_generator function
static double zipfianconstant; //initialized in init_zipf_generator function
static double alpha; //initialized in init_zipf_generator function
static double zetan; //initialized in init_zipf_generator function
static double eta; //initialized in init_zipf_generator function
static double theta; //initialized in init_zipf_generator function
static double zeta2theta; //initialized in init_zipf_generator function
static long countforzeta; //initialized in init_zipf_generator function


double zetastatic(long st, long n, double initialsum){
    double sum=initialsum;
    for (long i=st; i<n; i++){
        sum+=1/(pow(i+1,theta));
    }
    return sum;
}

double zeta(long st, long n, double initialsum) {
    countforzeta=n;
    return zetastatic(st,n,initialsum);
}

static unsigned int __thread seed;

long next_long(long itemcount){
    //from "Quickly Generating Billion-Record Synthetic Databases", Jim Gray et al, SIGMOD 1994
    if (itemcount!=countforzeta){
        if (itemcount>countforzeta){
            printf("WARNING: Incrementally recomputing Zipfian distribtion. (itemcount= %ld; countforzeta= %ld)", itemcount, countforzeta);
            //we have added more items. can compute zetan incrementally, which is cheaper
            zetan = zeta(countforzeta,itemcount,zetan);
            eta = ( 1 - pow(2.0/items,1-theta) ) / (1-zeta2theta/zetan);
        }
    }

    double u = (double)(rand_r(&seed)%RAND_MAX) / ((double)RAND_MAX);
    double uz=u*zetan;
    if (uz < 1.0){
        return base;
    }

    if (uz<1.0 + pow(0.5,theta)) {
        return base + 1;
    }
    long ret = base + (long)((itemcount) * pow(eta*u - eta + 1, alpha));
    return ret;
}

long zipf_next() {
    return next_long(items);
}

/* Uniform */
long uniform_next() {
    return rand_r(&seed) % items;
}

typedef enum available_bench {
    ycsb_a_uniform,
    ycsb_b_uniform,
    ycsb_c_uniform,
    ycsb_e_uniform,
    ycsb_a_zipfian,
    ycsb_b_zipfian,
    ycsb_c_zipfian,
    ycsb_e_zipfian,
    prod1,
    prod2,
} bench_t;

/* Is the current request a get or a put? */
static int random_get_put(int test) {
    long random = uniform_next() % 100;
    switch(test) {
        case 0: // A
            return random >= 50;
        case 1: // B
            return random >= 95;
        case 2: // C
            return 0;
        case 3: // E
            return random >= 95;
    }
    die("Not a valid test\n");
}

struct item_metadata {
    size_t rdt;
    size_t key_size;
    size_t value_size;
    // key
    // value
};

void init_zipf_generator(long min, long max){
    items = max-min+1;
    base = min;
    zipfianconstant = 0.99;
    theta = zipfianconstant;
    zeta2theta = zeta(0, 2, 0);
    alpha = 1.0/(1.0-theta);
    zetan = zetastatic(0, max-min+1, 0);
    countforzeta = items;
    eta=(1 - pow(2.0/items,1-theta) )/(1-zeta2theta/zetan);

    zipf_next();
}

char *create_unique_item(size_t item_size, uint64_t uid) {
    char *item = (char*)which_malloc(item_size);
    struct item_metadata *meta = (struct item_metadata *)item;
    meta->key_size = 8;
    meta->value_size = item_size - 8 - sizeof(*meta);

    char *item_key = &item[sizeof(*meta)];
    char *item_value = &item[sizeof(*meta) + meta->key_size];
    *(uint64_t*)item_key = uid;
    *(uint64_t*)item_value = uid;
    return item;
}

/* YCSB A (or D), B, C */
static void _launch_ycsb(int test, int nb_requests, int zipfian) {

    init_zipf_generator(0, nb_requests - 1);
    long (*rand_next)(void) = zipfian?zipf_next:uniform_next;
    masstree::masstree *tree = new masstree::masstree();

    auto starttime = std::chrono::system_clock::now();
    tbb::parallel_for(tbb::blocked_range<uint64_t>(0, nb_requests), [&](const tbb::blocked_range<uint64_t> &scope) {
        auto t = tree->getThreadInfo();
        for (uint64_t i = scope.begin(); i != scope.end(); i++) {

            long fkey=rand_next();
            char* item= create_unique_item(1024,fkey);

            if(random_get_put(test)) { // In these tests we update with a given probability
                tree->put(fkey, item, t);
            } else { // or we read
                tree->get(fkey,t);
//                uint64_t *ret = reinterpret_cast<uint64_t *> (tree->get((char *)keys[i]->fkey, t));
//                if (ap == UNIFORM && (uint64_t) ret != keys[i]->value) {
//                    printf("[MASS] search key = %lu, search value = %lu\n", keys[i]->value, ret);
//                    exit(1);
//                }
            }
        }
    });
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);
    printf("Throughput: run, %f ,ops/us\n", (nb_requests * 1.0) / duration.count());
}

/* YCSB E */
static void _launch_ycsb_e(int test, int nb_requests, int zipfian) {

    init_zipf_generator(0, nb_requests - 1);
    long (*rand_next)(void) = zipfian?zipf_next:uniform_next;
    masstree::masstree *tree = new masstree::masstree();

    auto starttime = std::chrono::system_clock::now();
    tbb::parallel_for(tbb::blocked_range<uint64_t>(0, nb_requests), [&](const tbb::blocked_range<uint64_t> &scope) {
        auto t = tree->getThreadInfo();
        for (uint64_t i = scope.begin(); i != scope.end(); i++) {

            long key = rand_next();
            char* item = create_unique_item(1024,key);

            if(random_get_put(test)) {
                tree->put(key, item, t);
            }else{
                uint64_t buf[200];
                int ret = tree->scan(key, uniform_next()%99+1, buf, t);
            }

        }
    });
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);
    printf("Throughput: run, %f ,ops/us\n", (nb_requests * 1.0) / duration.count());
}

/* Generic interface */
static void launch_ycsb( bench_t b,uint64_t nb_requests) {
    switch(b) {
        case ycsb_a_uniform:
            return _launch_ycsb(0, nb_requests, 0);
        case ycsb_b_uniform:
            return _launch_ycsb(1, nb_requests, 0);
        case ycsb_c_uniform:
            return _launch_ycsb(2, nb_requests, 0);
        case ycsb_e_uniform:
            return _launch_ycsb_e(3, nb_requests, 0);
        case ycsb_a_zipfian:
            return _launch_ycsb(0, nb_requests, 1);
        case ycsb_b_zipfian:
            return _launch_ycsb(1, nb_requests, 1);
        case ycsb_c_zipfian:
            return _launch_ycsb(2, nb_requests, 1);
        case ycsb_e_zipfian:
            return _launch_ycsb_e(3, nb_requests, 1);
        default:
            die("Unsupported workload\n");
    }
}

void run(char **argv) {
    std::cout << "Simple Example of P-Masstree-New" << std::endl;

    uint64_t n = std::atoll(argv[1]);
    uint64_t *keys = new uint64_t[n];


    // Generate keys
    for (uint64_t i = 0; i < n; i++) {
        keys[i] = i + 1;
    }

    int num_thread = atoi(argv[2]);
    tbb::task_scheduler_init init(num_thread);


    // todo: make templates/cpp (modular) <- important
    which_memalign=posix_memalign;
    which_memfree=free;
    which_malloc=malloc;
    which_free=free;
    int require_RP_init=0;
    int require_log_init=0;
    int require_flush=0;
    int shuffle_keys=0;

    for (int ac=0;ac<6;ac++){
        if (strcasestr(argv[ac],"index")){
            if (strcasestr(argv[ac],"pmem")){
                which_memalign=RP_memalign;
                which_memfree=RP_free;
                require_RP_init=1;
            }else if (strcasestr(argv[ac],"log")){
                which_memalign=log_memalign;
                which_memfree=log_free;
                require_log_init=1;
            }
        }else if (strcasestr(argv[ac],"value")){
            if (strcasestr(argv[ac],"pmem")){
                which_malloc=RP_malloc;
                which_free=RP_free;
                require_RP_init=1;
                require_flush=1;
            }else if (strcasestr(argv[ac],"log")){
                which_malloc=log_malloc;
                which_free=log_free;
                require_log_init=1;
                require_flush=1;
            }
        }else if (strcasestr(argv[ac],"key")){
            if (strcasestr(argv[ac],"rand")){
                shuffle_keys=1;
            }
        }else if (strcasestr(argv[ac],"ycsb")){
            launch_ycsb(ycsb_a_uniform,n);
            exit(0);
        }
    }

    if (require_RP_init){
        RP_init("masstree",64*1024*1024*1024ULL);
    }

    // todo: add latency tracker and perf

    // (TP dropped) shuffle the array todo: random keys (make it faster)
    if (shuffle_keys){

        srand(time(NULL));
        for (uint64_t i = 0; i < n - 1; i++)
        {
            uint64_t j = i + rand() / (RAND_MAX / (n - i) + 1);
            uint64_t t = keys[j];
            keys[j] = keys[i];
            keys[i] = t;
        }
    }

    double insert_throughput;
    double lookup_throughput;


    printf("operation,n,ops/s\n");
    masstree::masstree *tree = new masstree::masstree();

    if (require_log_init){
        char const *fn = "/pmem0/masstree_log";
        log_init(fn,10240);
        log_start_gc(tree);
    }

    {
        // Build tree
        auto starttime = std::chrono::system_clock::now();
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            auto t = tree->getThreadInfo();
            for (uint64_t i = range.begin(); i != range.end(); i++) {
//                tree->put(keys[i], &keys[i], t);

                // todo: size randomize (YCSB/Facebook workload)
//                int size = rand()%2048+sizeof(uint64_t);
                int size = sizeof(uint64_t);

                uint64_t *key = (uint64_t *)which_malloc(sizeof(uint64_t)*2);
                uint64_t *value=key+1;
                *key=keys[i];

//                uint64_t * value = (uint64_t *)which_malloc(size);

                // flush value before inserting todo: should this exist for DRAM+DRAM?
                *value=keys[i];
                if (require_flush) clflush((char*)value,size,true,true);
//                tree->put(keys[i], value, t);
                tree->put(*key, value, t);
            }
        });
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: insert,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: insert,%ld,%f sec\n", n, duration.count() / 1000000.0);
        insert_throughput=(n * 1.0) / duration.count();
    }
        log_debug_print(100);
    {
        // Lookup
        auto starttime = std::chrono::system_clock::now();
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            auto t = tree->getThreadInfo();
            for (uint64_t i = range.begin(); i != range.end(); i++) {
                uint64_t *ret = reinterpret_cast<uint64_t *> (tree->get(keys[i], t));
                if (*ret != keys[i]) {
                    std::cout << "wrong value read: " << *ret << " expected:" << keys[i] << std::endl;
                    throw;
                }

                // (TP dropped) todo: free memory, is this correct?
                // todo: it should be freed in update() ALSO modify update() in masstree
                which_free(ret);
            }
        });
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: lookup,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: lookup,%ld,%f sec\n", n, duration.count() / 1000000.0);
        lookup_throughput=(n * 1.0) / duration.count();
    }

    // logging throughput to files

    FILE* insert_throughput_file=fopen("insert.csv","a");
    FILE* lookup_throughput_file=fopen("lookup.csv","a");

    fprintf(insert_throughput_file,"%.2f,", insert_throughput);
    fprintf(lookup_throughput_file,"%.2f,", lookup_throughput);

    fclose(insert_throughput_file);
    fclose(lookup_throughput_file);


    delete[] keys;
}

int main(int argc, char **argv) {
    if (argc != 6) {
        printf("usage: %s [n] [nthreads]\nn: number of keys (integer)\nnthreads: number of threads (integer)\n", argv[0]);
        return 1;
    }

    run(argv);
    return 0;
}

