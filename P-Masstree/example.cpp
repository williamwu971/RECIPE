#include <iostream>
#include <chrono>
#include <random>
#include <libpmemobj.h>
#include <fstream>
#include <set>
//#include "tbb/tbb.h"
#include "plog.cpp"

//#define PMEM_POOL_SIZE (4*1024*1024*1024ULL)
//#define FOOTER 0xdeadbeef

int (*which_memalign)(void **memptr, size_t alignment, size_t size) = posix_memalign;

void (*which_memfree)(void *ptr) =free;

int require_RP_init = 0;
int require_log_init = 0;
int require_obj_init = 0;
int shuffle_keys = 0;
int use_perf = 0;
int num_of_gc = 0;
int show_log_usage = 1;
int record_latency = 0;
int display_throughput = 1;
int use_log = 0;
int total_size = 0;
int memset_size = 0;
int base_size = 0;
int goto_lookup = 0;
char *prefix = NULL;
int interfere = 1;
uint64_t iter;
uint64_t PMEM_POOL_SIZE = 0;
uint64_t value_offset = 0;

struct functions {
    static inline void (*update_func)(masstree::masstree *, MASS::ThreadInfo, uint64_t, uint64_t, int, void *) = NULL;

    static inline void (*delete_func)(masstree::masstree *, MASS::ThreadInfo, uint64_t) = NULL;

    static inline void (*lookup_func)(masstree::masstree *, MASS::ThreadInfo, uint64_t, uint64_t, int) = NULL;

    static inline void (*scan_func)(masstree::masstree *, MASS::ThreadInfo, uint64_t, int) = NULL;
};

struct functions fs;

void *(*cpy_persist)(void *, const void *, size_t) =pmem_memcpy_persist;

uint64_t n;
int num_thread;
PMEMobjpool *pop = NULL;

POBJ_LAYOUT_BEGIN(masstree);
POBJ_LAYOUT_TOID(masstree,
                 struct masstree_obj)

POBJ_LAYOUT_END(masstree)

struct masstree_obj {
//    TOID(struct masstree_obj) objToid;
    PMEMoid ht_oid;
    uint64_t data;
};

using namespace std;

#include "masstree.h"
#include "ralloc.hpp"
#include "pfence_util.h"

//#define RP_malloc RP_counted_malloc

inline int RP_memalign(void **memptr, size_t alignment, size_t size) {

    *memptr = RP_malloc(size + (alignment - size % alignment));

    // todo: this can be removed
    uint64_t casted = (uint64_t) (*memptr);
    if (casted % alignment != 0) {
        throw;
    }

    return 0;
}

//static constexpr uint64_t CACHE_LINE_SIZE = 64;

static inline void clflush(char *data, int len, bool front, bool back) {
    volatile char *ptr = (char *) ((unsigned long) data & ~(CACHE_LINE_SIZE - 1));
    if (front)
        asm volatile("sfence":: :"memory");
    for (; ptr < data + len; ptr += CACHE_LINE_SIZE) {
#ifdef CLFLUSH
        asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
#elif CLFLUSH_OPT
        asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *) (ptr)));
#elif CLWB
        //        asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *) (ptr)));
                FLUSH(ptr);
#endif
    }
    if (back)
        asm volatile("sfence":: :"memory");
}

void *memcpy_then_persist(void *pmemdest, const void *src, size_t len) {
    memcpy(pmemdest, src, len);
    pmem_persist(pmemdest, len);
    return pmemdest;
}

void dump_latencies(const char *fn, u_int64_t *numbers, uint64_t length) {
    FILE *latency_file = fopen(fn, "w");
    for (uint64_t idx = 0; idx < length; idx++) {
        fprintf(latency_file, "%lu\n", numbers[idx]);
    }
    fclose(latency_file);
}

struct section_arg {
    uint64_t start;
    uint64_t end;
    masstree::masstree *tree;
    uint64_t *keys;
    uint64_t *rands;
    u_int64_t *latencies;
};

std::vector<uint64_t> ycsb_init_keys;
std::vector<uint64_t> ycsb_keys;
std::vector<int> ycsb_ranges;
std::vector<int> ycsb_ops;

char *wl = NULL;

enum {
    OP_INSERT,
    OP_UPDATE,
    OP_READ,
    OP_SCAN,
    OP_DELETE,
};


static long items; //initialized in init_zipf_generator function
static long base; //initialized in init_zipf_generator function
static double zipfianconstant; //initialized in init_zipf_generator function
static double alpha; //initialized in init_zipf_generator function
static double zetan; //initialized in init_zipf_generator function
static double eta; //initialized in init_zipf_generator function
static double theta; //initialized in init_zipf_generator function
static double zeta2theta; //initialized in init_zipf_generator function
static long countforzeta; //initialized in init_zipf_generator function
static unsigned int __thread seed;

double zetastatic(long st, long zn, double initialsum) {
    double sum = initialsum;
    for (long i = st; i < zn; i++) {
        sum += 1 / (pow(i + 1, theta));
    }
    return sum;
}

double zeta(long st, long zn, double initialsum) {
    countforzeta = zn;
    return zetastatic(st, zn, initialsum);
}

long next_long(long itemcount) {
    //from "Quickly Generating Billion-Record Synthetic Databases", Jim Gray et al, SIGMOD 1994
    if (itemcount != countforzeta) {
        if (itemcount > countforzeta) {
            printf("WARNING: Incrementally recomputing Zipfian distribtion. (itemcount= %ld; countforzeta= %ld)",
                   itemcount, countforzeta);
            //we have added more items. can compute zetan incrementally, which is cheaper
            zetan = zeta(countforzeta, itemcount, zetan);
            eta = (1 - pow(2.0 / items, 1 - theta)) / (1 - zeta2theta / zetan);
        }
    }

    double u = (double) (rand_r(&seed) % RAND_MAX) / ((double) RAND_MAX);
    double uz = u * zetan;
    if (uz < 1.0) {
        return base;
    }

    if (uz < 1.0 + pow(0.5, theta)) {
        return base + 1;
    }
    long ret = base + (long) ((itemcount) * pow(eta * u - eta + 1, alpha));
    return ret;
}

long zipf_next() {
    return next_long(items);
}

/* Uniform */
long uniform_next() {
    return rand_r(&seed) % items;
}

void init_zipf_generator(long min, long max) {
    items = max - min + 1;
    base = min;
    zipfianconstant = 0.99;
    theta = zipfianconstant;
    zeta2theta = zeta(0, 2, 0);
    alpha = 1.0 / (1.0 - theta);
    zetan = zetastatic(0, max - min + 1, 0);
    countforzeta = items;
    eta = (1 - pow(2.0 / items, 1 - theta)) / (1 - zeta2theta / zetan);

    zipf_next();
}

int random_get_put(int test) {
    long random = uniform_next() % 100;
    switch (test) {
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

void bap_ycsb_load() {


    int test = 0;
    int zipfian = 0;

    if (wl[0] == 'a') {
        test = 0;
    } else if (wl[0] == 'b') {
        test = 1;
    } else if (wl[0] == 'c') {
        test = 2;
    } else if (wl[0] == 'e') {
        test = 3;
    }


    if (wl[1] == 'z') {
        zipfian = 1;
    }

    init_zipf_generator(0, n - 1);
    long (*rand_next)(void) = zipfian ? zipf_next : uniform_next;


    ycsb_init_keys.reserve(n);
    ycsb_keys.reserve(n);
    ycsb_ranges.reserve(n);
    ycsb_ops.reserve(n);

    memset(&ycsb_init_keys[0], 0x00, n * sizeof(uint64_t));
    memset(&ycsb_keys[0], 0x00, n * sizeof(uint64_t));
    memset(&ycsb_ranges[0], 0x00, n * sizeof(int));
    memset(&ycsb_ops[0], 0x00, n * sizeof(int));

    uint64_t count = 0;
    while ((count < n)) {

        uint64_t key = rand_next();
        ycsb_init_keys.push_back(key);
        count++;
    }

    fprintf(stderr, "Loaded %lu keys\n", count);


    uint64_t count_OP_INSERT = 0;
    uint64_t count_OP_UPDATE = 0;
    uint64_t count_OP_READ = 0;
    uint64_t count_OP_SCAN = 0;

    count = 0;
    while ((count < n)) {

        long key = rand_next();
        int op_type = random_get_put(test);

        if (op_type) {
            ycsb_ops.push_back(OP_UPDATE);
            ycsb_keys.push_back(key);
            ycsb_ranges.push_back(1);
            count_OP_UPDATE++;
        } else {

            if (test != 3) {
                ycsb_ops.push_back(OP_READ);
                ycsb_keys.push_back(key);
                ycsb_ranges.push_back(1);
                count_OP_READ++;
            } else {
                int range = uniform_next() % 99 + 1;
                ycsb_ops.push_back(OP_SCAN);
                ycsb_keys.push_back(key);
                ycsb_ranges.push_back(range);
                count_OP_SCAN++;
            }


        }
        count++;
    }

    printf("\nINSERT: %lu %5.2f \n", count_OP_INSERT, (double) count_OP_INSERT / (double) count * 100.0f);
    printf("UPDATE: %lu %5.2f \n", count_OP_UPDATE, (double) count_OP_UPDATE / (double) count * 100.0f);
    printf("READ  : %lu %5.2f \n", count_OP_READ, (double) count_OP_READ / (double) count * 100.0f);
    printf("SCAN  : %lu %5.2f \n", count_OP_SCAN, (double) count_OP_SCAN / (double) count * 100.0f);

}

void ycsb_load() {


    std::string init_file;
    std::string txn_file;


    if (strcmp(wl, "a") == 0) {
        init_file = "../../index-microbench/workloads/loada_unif_int.dat";
        txn_file = "../../index-microbench/workloads/txnsa_unif_int.dat";
    } else if (strcmp(wl, "b") == 0) {
        init_file = "../../index-microbench/workloads/loadb_unif_int.dat";
        txn_file = "../../index-microbench/workloads/txnsb_unif_int.dat";
    } else if (strcmp(wl, "c") == 0) {
        init_file = "../../index-microbench/workloads/loadc_unif_int.dat";
        txn_file = "../../index-microbench/workloads/txnsc_unif_int.dat";
    } else if (strcmp(wl, "d") == 0) {
        init_file = "../../index-microbench/workloads/loadd_unif_int.dat";
        txn_file = "../../index-microbench/workloads/txnsd_unif_int.dat";
    } else if (strcmp(wl, "e") == 0) {
        init_file = "../../index-microbench/workloads/loade_unif_int.dat";
        txn_file = "../../index-microbench/workloads/txnse_unif_int.dat";
    }


    std::ifstream infile_load(init_file);

    std::string op;
    uint64_t key;
    int range;

    std::string insert("INSERT");
    std::string update("UPDATE");
    std::string read("READ");
    std::string scan("SCAN");

    ycsb_init_keys.reserve(n);
    ycsb_keys.reserve(n);
    ycsb_ranges.reserve(n);
    ycsb_ops.reserve(n);

    memset(&ycsb_init_keys[0], 0x00, n * sizeof(uint64_t));
    memset(&ycsb_keys[0], 0x00, n * sizeof(uint64_t));
    memset(&ycsb_ranges[0], 0x00, n * sizeof(int));
    memset(&ycsb_ops[0], 0x00, n * sizeof(int));

    uint64_t count = 0;
    while ((count < n) && infile_load.good()) {
        infile_load >> op >> key;
        if (op.compare(insert) != 0) {
            std::cout << "READING LOAD FILE FAIL!\n";
            return;
        }
        ycsb_init_keys.push_back(key);
        count++;
    }

    fprintf(stderr, "Loaded %lu keys\n", count);

    std::ifstream infile_txn(txn_file);


    uint64_t count_OP_INSERT = 0;
    uint64_t count_OP_UPDATE = 0;
    uint64_t count_OP_READ = 0;
    uint64_t count_OP_SCAN = 0;

    count = 0;
    while ((count < n) && infile_txn.good()) {
        infile_txn >> op >> key;
        if (op.compare(insert) == 0) {
            ycsb_ops.push_back(OP_INSERT);
            ycsb_keys.push_back(key);
            ycsb_ranges.push_back(1);
            count_OP_INSERT++;
        } else if (op.compare(update) == 0) {
            ycsb_ops.push_back(OP_UPDATE);
            ycsb_keys.push_back(key);
            ycsb_ranges.push_back(1);
            count_OP_UPDATE++;
        } else if (op.compare(read) == 0) {
            ycsb_ops.push_back(OP_READ);
            ycsb_keys.push_back(key);
            ycsb_ranges.push_back(1);
            count_OP_READ++;
        } else if (op.compare(scan) == 0) {
            infile_txn >> range;
            ycsb_ops.push_back(OP_SCAN);
            ycsb_keys.push_back(key);
            ycsb_ranges.push_back(range);
            count_OP_SCAN++;
        } else {
            std::cout << "UNRECOGNIZED CMD!\n";
            return;
        }
        count++;
    }

    printf("\nINSERT: %lu %5.2f \n", count_OP_INSERT, (double) count_OP_INSERT / (double) count * 100.0f);
    printf("UPDATE: %lu %5.2f \n", count_OP_UPDATE, (double) count_OP_UPDATE / (double) count * 100.0f);
    printf("READ  : %lu %5.2f \n", count_OP_READ, (double) count_OP_READ / (double) count * 100.0f);
    printf("SCAN  : %lu %5.2f \n", count_OP_SCAN, (double) count_OP_SCAN / (double) count * 100.0f);

    std::atomic<int> range_complete, range_incomplete;
    range_complete.store(0);
    range_incomplete.store(0);
}

//#define TAILER (0xdeadbeef)

static inline uint64_t masstree_getsum(void *value) {

    uint64_t *numbers = (uint64_t *) value;
    uint64_t sum = 0;

    for (uint64_t i = 0; i < iter + 1; i++) {
        sum += numbers[i];
    }

    return sum;
}

pthread_mutex_t ralloc_recover_stats_lock = PTHREAD_MUTEX_INITIALIZER;
uint64_t ralloc_recovered = 0;
uint64_t ralloc_abandoned = 0;
uint64_t ralloc_total_time_tree = 0;
uint64_t ralloc_total_time_read = 0;
uint64_t ralloc_total_time_meta = 0;

struct ralloc_ptr_list {
    void *ptr;
    struct ralloc_ptr_list *next;
};

void ralloc_ptr_list_add(struct ralloc_ptr_list **head, void *ptr) {

    struct ralloc_ptr_list *new_node = (struct ralloc_ptr_list *) malloc(sizeof(struct ralloc_ptr_list));

    new_node->ptr = ptr;
    new_node->next = *head;
    *head = new_node;
}

void *ralloc_recover_scan_thread(void *raw) {

    masstree::masstree *tree = (masstree::masstree *) raw;
    auto t = tree->getThreadInfo();

    uint64_t valid = 0;
    uint64_t invalid = 0;

    uint64_t time_tree = 0;
    uint64_t time_read = 0;
    uint64_t time_meta = 0;

    struct ralloc_ptr_list *ptrs = NULL;

    while (1) {

        uint64_t a, b;

        struct RP_scan_pack pack = RP_scan_next();
        if (pack.curr == NULL)break;
        if (pack.block_size < (uint32_t) total_size) throw;

        while (pack.curr < pack.end) {

            rdtscll(a)
            void *res = masstree_checksum(pack.curr, SUM_LOG, 0, iter, 0);
            rdtscll(b)
            time_read += b - a;

            if (res != NULL) {
                valid++;

                // record pointer
                rdtscll(a)
                ralloc_ptr_list_add(&ptrs, pack.curr);
                rdtscll(b)
                time_meta += b - a;

                rdtscll(a)
                uint64_t key = ((uint64_t *) pack.curr)[1];
                void *returned = tree->put_and_return(key, pack.curr, 1, 0, t);
                rdtscll(b)
                time_tree += b - a;


                if (returned != NULL) {
                    puts("detected replacing keys");
                    throw;
                }

            } else {
                invalid++;
            }
            pack.curr += pack.block_size;
        }

    }
//    printf("valid: %lu scanned: %fgb\n", valid, (double) scanned_bytes / 1024.0 / 1024.0 / 1024.0);
    pthread_mutex_lock(&ralloc_recover_stats_lock);
    ralloc_recovered += valid;
    ralloc_abandoned += invalid;
    ralloc_total_time_meta += time_meta;
    ralloc_total_time_read += time_read;
    ralloc_total_time_tree += time_tree;
    pthread_mutex_unlock(&ralloc_recover_stats_lock);

    return ptrs;
}

void ralloc_recover_scan(masstree::masstree *tree) {

    const char *section_name = "ralloc_recover_scan";
    char perf_fn[256];
    sprintf(perf_fn, "%s-%s.perf", prefix == NULL ? "" : prefix, section_name);
    printf("\n");

    pthread_t *threads = (pthread_t *) calloc(num_thread, sizeof(pthread_t));
    struct ralloc_ptr_list **ptr_lists = (struct ralloc_ptr_list **)
            calloc(num_thread, sizeof(struct ralloc_ptr_list *));
    RP_scan_init();

    if (use_perf)log_start_perf(perf_fn);
    auto starttime = std::chrono::system_clock::now();

    for (int i = 0; i < num_thread; i++) {
        cpu_set_t cpu;
        CPU_ZERO(&cpu);

        // reserving CPU 0
        CPU_SET(i + 1, &cpu);

        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpu);

        pthread_create(threads + i, &attr, ralloc_recover_scan_thread, tree);
    }

    for (int i = 0; i < num_thread; i++) {
        pthread_join(threads[i], (void **) (ptr_lists + i));
    }

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);

    if (use_perf) {
        log_stop_perf();
        log_print_pmem_bandwidth(perf_fn, duration.count() / 1000000.0, NULL);
    }

    if (display_throughput) {
        printf("Throughput: %s,%ld,%.2f ops/us %.2f sec\n",
               section_name, ralloc_recovered, (ralloc_recovered * 1.0) / duration.count(),
               duration.count() / 1000000.0);
    }


    // push pointers to ralloc's list single threaded
    starttime = std::chrono::system_clock::now();

    uint64_t meta_here = 0;

    uint64_t a, b;
    rdtscll(a)
    for (int i = 0; i < num_thread; i++) {
        struct ralloc_ptr_list *curr = ptr_lists[i];

        while (curr != NULL) {

            RP_recover_xiaoxiang_insert(curr->ptr);

            struct ralloc_ptr_list *next = curr->next;
            free(curr);
            curr = next;
        }
    }
    rdtscll(b)
    meta_here += b - a;

    duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);

    if (display_throughput) {
        printf("Throughput: %s,%ld,%.2f ops/us %.2f sec\n",
               "set-insert", ralloc_recovered, (ralloc_recovered * 1.0) / duration.count(),
               duration.count() / 1000000.0);
    }

    starttime = std::chrono::system_clock::now();
    rdtscll(a)
    RP_recover_xiaoxiang_go();
    rdtscll(b)
    meta_here += b - a;
    duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);

    if (display_throughput) {
        printf("Throughput: %s,%ld,%.2f ops/us %.2f sec\n",
               "ralloc-build", ralloc_recovered, (ralloc_recovered * 1.0) / duration.count(),
               duration.count() / 1000000.0);
    }

    printf("recovered: %lu abandoned: %lu\n", ralloc_recovered, ralloc_abandoned);

    printf("time slides:\n");
    printf("read: %.2fs\n", (double) ralloc_total_time_read / (double) num_thread / 2000000000.0);
    printf("tree: %.2fs\n", (double) ralloc_total_time_tree / (double) num_thread / 2000000000.0);
    printf("meta: %.2fs\n", (double) ralloc_total_time_meta / (double) num_thread / 2000000000.0
                            + (double) meta_here / 2000000000.0);

}

#define REACH_T (LEAF_WIDTH+2)

void *ralloc_reachability_scan_thread(void *raw) {

    if (raw == NULL) {
        printf("thread %lu returning\n", pthread_self());
        return NULL;
    }

    struct ralloc_ptr_list *list = NULL;
    masstree::leafnode **to_visit = (masstree::leafnode **) calloc(1, sizeof(masstree::leafnode *));
    to_visit[0] = (masstree::leafnode *) raw;
    int to_visit_size = 1;

    int recovered_values = 0;

    while (to_visit_size) {

        masstree::leafnode **to_visit_next = NULL;
        int to_visit_next_size = 0;

        for (int i = 0; i < to_visit_size; i++) {

            masstree::leafnode *curr = to_visit[i];

            if (curr == NULL) {
                continue;
            }

            ralloc_ptr_list_add(&list, curr);


            if (curr->level() != 0) {

                to_visit_next = (masstree::leafnode **) realloc(to_visit_next,
                                                                (to_visit_next_size + REACH_T) *
                                                                sizeof(masstree::leafnode *));
                for (int kv_idx = 0; kv_idx < LEAF_WIDTH; kv_idx++) {
                    to_visit_next[to_visit_next_size + kv_idx] = (masstree::leafnode *) curr->value(kv_idx);
                }

                to_visit_next_size += LEAF_WIDTH;

                to_visit_next[to_visit_next_size] = (masstree::leafnode *) curr->leftmost();
//                to_visit_next[to_visit_next_size] = NULL; // todo
                to_visit_next[to_visit_next_size + 1] = (masstree::leafnode *) curr->next_();
                to_visit_next[to_visit_next_size + 1] = NULL; // todo
                to_visit_next_size += 2;

            } else {
                for (int kv_idx = 0; kv_idx < LEAF_WIDTH; kv_idx++) {

//                    uint64_t k = curr->key(kv_idx);
                    void *v = curr->value(kv_idx);

                    if (v != NULL) {
                        ralloc_ptr_list_add(&list, v);
                        recovered_values++;
                    }

                }
            }
        }

        free(to_visit);

        to_visit_size = to_visit_next_size;
        to_visit = to_visit_next;
    }

    if (to_visit != NULL) {
        printf("weird, should be NULL here, %p %d", to_visit, to_visit_size);
        free(to_visit);
    }

    pthread_mutex_lock(&ralloc_recover_stats_lock);
    ralloc_recovered += recovered_values;
//    ralloc_abandoned += invalid;
    pthread_mutex_unlock(&ralloc_recover_stats_lock);

    return list;
}


void ralloc_reachability_scan(masstree::masstree *tree) {

    puts("\tbegin Ralloc reachability scan");

    printf("tree root: %p\n", tree->root());
    masstree::leafnode *root = (masstree::leafnode *) tree->root();

    const char *section_name = "ralloc_reachability_scan";
    char perf_fn[256];
    sprintf(perf_fn, "%s-%s.perf", prefix == NULL ? "" : prefix, section_name);
    printf("\n");

    pthread_t *threads = (pthread_t *) calloc(REACH_T, sizeof(pthread_t));
    struct ralloc_ptr_list **ptr_lists = (struct ralloc_ptr_list **)
            calloc(REACH_T, sizeof(struct ralloc_ptr_list *));
    RP_scan_init();

    if (use_perf)log_start_perf(perf_fn);
    auto starttime = std::chrono::system_clock::now();

    for (int i = 0; i < REACH_T; i++) {
        cpu_set_t cpu;
        CPU_ZERO(&cpu);

        // reserving CPU 0
        CPU_SET(i + 1, &cpu);

        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpu);

        void *curr_arg = NULL;

        if (i == LEAF_WIDTH) {
            curr_arg = root->leftmost();
        } else if (i == LEAF_WIDTH + 1) {
            continue;
        } else if (i < LEAF_WIDTH) {
            curr_arg = root->value(i);
        }

        if (curr_arg == NULL) {
            printf("index %d is NULL\n", i);
            continue;
        }

        pthread_create(threads + i, &attr, ralloc_reachability_scan_thread, curr_arg);

    }

    for (int i = 0; i < REACH_T; i++) {
        if (threads[i] != 0) {
            pthread_join(threads[i], (void **) (ptr_lists + i));
        }
    }

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);

    if (use_perf) {
        log_stop_perf();
        log_print_pmem_bandwidth(perf_fn, duration.count() / 1000000.0, NULL);
    }

    if (display_throughput) {
        printf("Throughput: %s,%ld,%.2f ops/us %.2f sec\n",
               section_name, ralloc_recovered, (ralloc_recovered * 1.0) / duration.count(),
               duration.count() / 1000000.0);
    }

    // push pointers to ralloc's list single threaded
    starttime = std::chrono::system_clock::now();

    RP_recover_xiaoxiang_insert(root);

    for (int i = 0; i < REACH_T; i++) {
        struct ralloc_ptr_list *curr = ptr_lists[i];

        while (curr != NULL) {

            RP_recover_xiaoxiang_insert(curr->ptr);

            struct ralloc_ptr_list *next = curr->next;
            free(curr);
            curr = next;
        }
    }

    duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);

    if (display_throughput) {
        printf("Throughput: %s,%ld,%.2f ops/us %.2f sec\n",
               "set-insert", ralloc_recovered, (ralloc_recovered * 1.0) / duration.count(),
               duration.count() / 1000000.0);
    }

    starttime = std::chrono::system_clock::now();
    RP_recover_xiaoxiang_go();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);

    if (display_throughput) {
        printf("Throughput: %s,%ld,%.2f ops/us %.2f sec\n",
               "ralloc-build", ralloc_recovered, (ralloc_recovered * 1.0) / duration.count(),
               duration.count() / 1000000.0);
    }

    printf("recovered: %lu abandoned: %lu\n", ralloc_recovered, ralloc_abandoned);

}

int ralloc_extra = 0;

static inline void masstree_ralloc_update(masstree::masstree *tree,
                                          MASS::ThreadInfo t,
                                          uint64_t u_key,
                                          uint64_t u_value,
                                          int no_allow_prev_null,
                                          void *tplate) {
    *((uint64_t *) tplate) = u_value;
    if (ralloc_extra) {
        ((uint64_t *) tplate)[1] = u_key;
    }
    if (!masstree_checksum(tplate, SUM_WRITE, u_value, iter, value_offset)) throw;


    void *value = RP_malloc(total_size);

    cpy_persist(value, tplate, total_size);

    uint64_t *returned = (uint64_t *) tree->put_and_return(u_key, value, !no_allow_prev_null, 0, t);

    if (no_allow_prev_null || returned != NULL) {


        RP_free(returned);
        if (ralloc_extra) {
            pmem_persist(returned, sizeof(void *));
        }
    }
}

static inline void masstree_ralloc_delete(
        masstree::masstree *tree,
        MASS::ThreadInfo t,
        uint64_t d_key
) {


    uint64_t *returned = (uint64_t *) tree->del_and_return(d_key, 0, 0,
                                                           NULL, t);

    RP_free(returned);

    if (ralloc_extra) {
        pmem_persist(returned, sizeof(void *));
    }

}

static inline void masstree_log_update(masstree::masstree *tree,
                                       MASS::ThreadInfo t,
                                       uint64_t u_key,
                                       uint64_t u_value,
                                       int no_allow_prev_null,
                                       void *tplate) {


    struct log_cell *lc = (struct log_cell *) tplate;
    lc->key = u_key;
    rdtscll(lc->version)
    *((uint64_t *) (lc + 1)) = u_value;
    if (!masstree_checksum(tplate, SUM_WRITE, u_value, iter, value_offset)) throw;

    char *raw = (char *) log_malloc(total_size);
    cpy_persist(raw, tplate, total_size);

    void *returned = tree->put_and_return(u_key, raw, !no_allow_prev_null, 0, t);

    if (no_allow_prev_null || returned != NULL) {
        log_free(returned);
    }
}

static inline void masstree_log_delete(
        masstree::masstree *tree,
        MASS::ThreadInfo t,
        uint64_t d_key
) {

    log_free(tree->del_and_return(d_key, 0, 0, log_get_tombstone, t));

}

static inline void masstree_obj_update(masstree::masstree *tree,
                                       MASS::ThreadInfo t,
                                       uint64_t u_key,
                                       uint64_t u_value,
                                       int no_allow_prev_null,
                                       void *tplate) {


    TX_BEGIN(pop) {

                    PMEMoid
                            ht_oid = pmemobj_tx_alloc(total_size, TOID_TYPE_NUM(
                            struct masstree_obj));

                    struct masstree_obj *o = (struct masstree_obj *) tplate;
                    o->data = u_value;
                    o->ht_oid = ht_oid;
                    if (!masstree_checksum(tplate, SUM_WRITE, u_value, iter, value_offset)) throw;

                    pmemobj_tx_add_range(ht_oid, 0, total_size);
                    struct masstree_obj *mo = (struct masstree_obj *) pmemobj_direct(ht_oid);
                    memcpy(mo, tplate, total_size);

                    struct masstree_obj *old_obj = (struct masstree_obj *)
                            tree->put_and_return(u_key, mo, !no_allow_prev_null, 0, t);


                    if (no_allow_prev_null || old_obj != NULL) {


                        pmemobj_tx_add_range(old_obj->ht_oid, sizeof(struct masstree_obj) + memset_size,
                                             sizeof(uint64_t));
                        if (!masstree_checksum(old_obj, SUM_INVALID, u_value, iter, value_offset)) throw;
                        pmemobj_tx_free(old_obj->ht_oid);

                    }

                }
                    TX_ONABORT {
                    throw;
                }
    TX_END
}

static inline void masstree_obj_delete(
        masstree::masstree *tree,
        MASS::ThreadInfo t,
        uint64_t d_key
) {

    struct masstree_obj *old_obj = (struct masstree_obj *)
            tree->del_and_return(d_key, 0, 0,
                                 NULL, t);


    TX_BEGIN(pop) {

                    pmemobj_tx_add_range(old_obj->ht_oid, sizeof(struct masstree_obj) + memset_size,
                                         sizeof(uint64_t));
                    if (!masstree_checksum(old_obj, SUM_INVALID, d_key, iter, value_offset))throw;
                    pmemobj_tx_free(old_obj->ht_oid);
                }
                    TX_ONABORT {
                    throw;
                }
    TX_END


}


static inline void masstree_universal_lookup(
        masstree::masstree *tree,
        MASS::ThreadInfo t,
        uint64_t g_key,
        uint64_t g_value,
        int check_value
) {
    void *raw = tree->get(g_key, t);
    if (raw != NULL || check_value) {

        if (raw == NULL) {
            printf("key %lu returned NULL\n", g_key);
            throw;
        }


        if (!masstree_checksum(raw, SUM_CHECK, g_value, iter, value_offset)) {

            printf("error key %lu value %lu pointer %p\n", g_key, g_value, raw);
            throw;
        }
    }
}

static inline void masstree_ycsb_lookup(
        masstree::masstree *tree,
        MASS::ThreadInfo t,
        uint64_t g_key,
        uint64_t g_value,
        int check_value
) {

    (void) g_value;
    (void) check_value;

    void *raw = tree->get(g_key, t);
    if (raw != NULL) {
        if (masstree_getsum(raw) == 0) {
            printf("ycsb lookup sum 0\n");
            throw;
        }
    }
}

static inline void masstree_ycsb_scan(
        masstree::masstree *tree,
        MASS::ThreadInfo t,
        uint64_t s_min,
        int s_num
) {

    uint64_t buf[200];
    memset(buf, 0, sizeof(uint64_t) * 200);

    int ret = tree->scan(s_min, s_num, buf, t);
    for (int ret_idx = 0; ret_idx < ret; ret_idx++) {

        void *raw = (void *) buf[ret_idx];

        if (raw != NULL) {
            if (masstree_getsum(raw) == 0) {
                printf("ycsb scan sum 0\n");
                throw;
            }
        }
    }
}


void *section_ycsb_run(void *arg) {


    struct section_arg *sa = (struct section_arg *) arg;

    masstree::masstree *tree = sa->tree;
    uint64_t start = sa->start;
    uint64_t end = sa->end;
    u_int64_t *latencies = sa->latencies;

    void *tplate = malloc(total_size);
    memset(tplate, 7, total_size);

    if (use_log) {

        struct log_cell *lc = (struct log_cell *) tplate;
        lc->value_size = total_size - sizeof(struct log_cell);
        lc->is_delete = 0;

    }

    auto t = tree->getThreadInfo();

    u_int64_t rdt;

    for (uint64_t i = start; i < end; i++) {

        if (ycsb_ops[i] == OP_INSERT || ycsb_ops[i] == OP_UPDATE) {
            fs.update_func(tree, t, ycsb_keys[i], ycsb_keys[i], 0, tplate);
        } else if (ycsb_ops[i] == OP_READ) {
            fs.lookup_func(tree, t, ycsb_keys[i], ycsb_keys[i], 0);
        } else if (ycsb_ops[i] == OP_SCAN) {
            fs.scan_func(tree, t, ycsb_keys[i], ycsb_ranges[i]);
        } else if (ycsb_ops[i] == OP_DELETE) {
            fs.delete_func(tree, t, ycsb_keys[i]);
        }

        rdtscll(rdt)

        if (start == 0) {
            latencies[i] = rdt;
        }
    }


    return NULL;
}

void *section_insert(void *arg) {

    struct section_arg *sa = (struct section_arg *) arg;


    uint64_t start = sa->start;
    uint64_t end = sa->end;
    masstree::masstree *tree = sa->tree;
    uint64_t *keys = sa->keys;
    uint64_t *rands = sa->rands;
    u_int64_t *latencies = sa->latencies;


    void *tplate = malloc(total_size);
    memset(tplate, 7, total_size);


    if (use_log) {

        struct log_cell *lc = (struct log_cell *) tplate;
        lc->value_size = total_size - sizeof(struct log_cell);
        lc->is_delete = 0;


    }

    auto t = tree->getThreadInfo();

    u_int64_t rdt;

    for (uint64_t i = start; i < end; i++) {

        fs.update_func(tree, t, keys[i], rands[i], 0, tplate);
        rdtscll(rdt)

        if (start == 0) {
            latencies[i] = rdt;
        }
    }


    return NULL;
}

void *section_update(void *arg) {

    struct section_arg *sa = (struct section_arg *) arg;


    uint64_t start = sa->start;
    uint64_t end = sa->end;
    masstree::masstree *tree = sa->tree;
    uint64_t *keys = sa->keys;
    u_int64_t *latencies = sa->latencies;

    void *tplate = malloc(total_size);
    memset(tplate, 7, total_size);

    if (use_log) {

        struct log_cell *lc = (struct log_cell *) tplate;
        lc->value_size = total_size - sizeof(struct log_cell);
        lc->is_delete = 0;

    }

    auto t = tree->getThreadInfo();

    u_int64_t rdt;

    for (uint64_t i = start; i < end; i++) {

        fs.update_func(tree, t, keys[i], keys[i], 1, tplate);
        rdtscll(rdt)

        if (start == 0) {
            latencies[i] = rdt;
        }
    }


    return NULL;
}

void *section_lookup(void *arg) {

    struct section_arg *sa = (struct section_arg *) arg;


    uint64_t start = sa->start;
    uint64_t end = sa->end;
    masstree::masstree *tree = sa->tree;
    uint64_t *keys = sa->keys;
    u_int64_t *latencies = sa->latencies;


    auto t = tree->getThreadInfo();

    u_int64_t rdt;

    for (uint64_t i = start; i < end; i++) {

        fs.lookup_func(tree, t, keys[i], keys[i], 1);
        rdtscll(rdt)

        if (start == 0) {
            latencies[i] = rdt;
        }
    }


    return NULL;
}

void *section_delete(void *arg) {
    struct section_arg *sa = (struct section_arg *) arg;


    uint64_t start = sa->start;
    uint64_t end = sa->end;
    masstree::masstree *tree = sa->tree;
    uint64_t *keys = sa->keys;
    u_int64_t *latencies = sa->latencies;


    auto t = tree->getThreadInfo();

    u_int64_t rdt;

    for (uint64_t i = start; i < end; i++) {

        fs.delete_func(tree, t, keys[i]);
        rdtscll(rdt)

        if (start == 0) {
            latencies[i] = rdt;
        }
    }


    return NULL;
}

void masstree_shuffle(uint64_t *array, uint64_t size) {

    printf("shuffling array of size %lu\n", size);
    srand(time(NULL));

    for (uint64_t i = 0; i < size - 1; i++) {
        uint64_t j = i + rand() / (RAND_MAX / (size - i) + 1);
        uint64_t t = array[j];
        array[j] = array[i];
        array[i] = t;
    }
}

void run(
        const char *section_name,
        FILE *throughput_file,
        pthread_attr_t *attrs,
        struct section_arg *section_args,
        u_int64_t *latencies,
        void *(*routine)(void *)) {

    char perf_fn[256];
    sprintf(perf_fn, "%s-%s.perf", prefix == NULL ? "" : prefix, section_name);
    printf("\n");

    pthread_t *threads = (pthread_t *) calloc(num_thread, sizeof(pthread_t));

    if (use_perf)log_start_perf(perf_fn);

    // Build tree
    auto starttime = std::chrono::system_clock::now();

    for (int i = 0; i < num_thread; i++) {
        pthread_create(threads + i, attrs + i, routine, section_args + i);
    }
    for (int i = 0; i < num_thread; i++) {
        pthread_join(threads[i], NULL);
    }


    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);
    if (throughput_file != NULL)log_debug_print(throughput_file, require_log_init);

    log_wait_all_gc();
    auto duration_with_gc = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);
    if (throughput_file != NULL)log_debug_print(throughput_file, require_log_init);

    if (use_perf) {
        log_stop_perf();
        log_print_pmem_bandwidth(perf_fn, duration.count() / 1000000.0, throughput_file);
    }

    if (display_throughput)
        printf("Throughput: %s,%ld,%.2f ops/us %.2f sec\n",
               section_name, n, (n * 1.0) / duration.count(), duration.count() / 1000000.0);

    sprintf(perf_fn, "%s-%s.rdtsc", prefix, section_name);

    if (record_latency) {
        dump_latencies(perf_fn, latencies,
                       section_args[0].end > 1000000 ? 1000000 : section_args[0].end);
    }


    if (throughput_file != NULL) {
        fprintf(throughput_file, "%.2f,%.2f,",
                (n * 1.0) / duration.count(),
                (n * 1.0) / duration_with_gc.count());
    }
}

int main(int argc, char **argv) {

    cpu_set_t fcpu;
    CPU_ZERO(&fcpu);
    CPU_SET(0, &fcpu);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &fcpu);

    if (argc < 3) {
        printf("usage: %s [n] [nthreads]\nn: number of keys (integer)\nnthreads: number of threads (integer)\n",
               argv[0]);
        return 1;
    }
    for (int ac = 0; ac < argc; ac++) printf("%s ", argv[ac]);
    puts("");

    for (int ac = 0; ac < argc; ac++) {

        if (ac == 1) {
            n = std::atoll(argv[1]);
            printf("n:%lu ", n);

        } else if (ac == 2) {
            num_thread = atoi(argv[2]);
            printf("num_thread:%d ", num_thread);

        } else if (strcasestr(argv[ac], "index=")) {
            if (strcasestr(argv[ac], "ralloc")) {
                which_memalign = RP_memalign;
                which_memfree = RP_free;
                require_RP_init = 1;
                printf("index=ralloc ");

            } else if (strcasestr(argv[ac], "obj")) {
                which_memalign = masstree::obj_memalign;
                which_memfree = masstree::obj_free;
                require_obj_init = 1;
                printf("index=obj ");

            } else {
                printf("index=dram ");

            }
        } else if (strcasestr(argv[ac], "value=")) {

            fs.lookup_func = masstree_universal_lookup;

            if (strcasestr(argv[ac], "ralloc")) {
                require_RP_init = 1;
                base_size = sizeof(uint64_t) * 2;

                if (which_memalign == posix_memalign) {
                    base_size += sizeof(uint64_t);
                    ralloc_extra = 1;
                }

                printf("value=ralloc ");

                fs.update_func = masstree_ralloc_update;
                fs.delete_func = masstree_ralloc_delete;

            } else if (strcasestr(argv[ac], "log")) {
                require_log_init = 1;
                use_log = 1;
                base_size = sizeof(struct log_cell) + sizeof(uint64_t) * 2;
                printf("value=log ");


                value_offset = sizeof(struct log_cell);
                assert(value_offset % sizeof(uint64_t) == 0);
                value_offset = value_offset / sizeof(uint64_t);

                fs.update_func = masstree_log_update;
                fs.delete_func = masstree_log_delete;

                if (strcasestr(argv[ac], "best")) {
                    interfere = 0;
                }

                printf("interfere=%d ", interfere);

            } else if (strcasestr(argv[ac], "obj")) {
                require_obj_init = 1;
                base_size = sizeof(struct masstree_obj) + sizeof(uint64_t);
                printf("value=obj ");


                struct masstree_obj obj_tmp;
                value_offset = ((char *) (&obj_tmp.data) - (char *) (&obj_tmp));
                assert(value_offset % sizeof(uint64_t) == 0);
                value_offset = value_offset / sizeof(uint64_t);

                fs.update_func = masstree_obj_update;
                fs.delete_func = masstree_obj_delete;

            } else {
                die("what?");
            }
        } else if (strcasestr(argv[ac], "key=")) {
            if (strcasestr(argv[ac], "rand")) {
                shuffle_keys = 1;
                printf("key=rand ");

            } else {
                printf("key=seq ");

            }
        } else if (strcasestr(argv[ac], "perf=")) {
            if (strcasestr(argv[ac], "y")) {
                use_perf = 1;
                printf("perf=y ");

            } else {
                printf("perf=n ");

            }
        } else if (strcasestr(argv[ac], "gc=")) {
            if (require_log_init)num_of_gc = atoi(strcasestr(argv[ac], "=") + 1);
            printf("gc=%d ", num_of_gc);

        } else if (strcasestr(argv[ac], "latency=")) {
            if (strcasestr(argv[ac], "yes")) {
                record_latency = 1;
                printf("latency=yes ");

            } else {
                printf("latency=no ");

            }
        } else if (strcasestr(argv[ac], "extra_size=")) {
            memset_size = atoi(strcasestr(argv[ac], "=") + 1);
            printf("memset_size=%d ", memset_size);

        } else if (strcasestr(argv[ac], "total_size=")) {
            total_size = atoi(strcasestr(argv[ac], "=") + 1);
            printf("total_size=%d ", total_size);

        } else if (strcasestr(argv[ac], "ycsb=")) {
            wl = strcasestr(argv[ac], "=") + 1;

            if (wl[0] != 'a' && wl[0] != 'b' && wl[0] != 'c' && wl[0] != 'd' && wl[0] != 'e') {
                wl = NULL;
                continue;
            }

            fs.lookup_func = masstree_ycsb_lookup;
            fs.scan_func = masstree_ycsb_scan;

            printf("ycsb=%s ", wl);

//            ycsb_load();
            bap_ycsb_load();

        } else if (strcasestr(argv[ac], "prefix=")) {
            char *prefix_ptr = strcasestr(argv[ac], "=") + 1;
            prefix = (char *) malloc(sizeof(char) * (strlen(prefix_ptr) + 1));
            strcpy(prefix, prefix_ptr);
        } else if (strcasestr(argv[ac], "persist=")) {
            if (strcasestr(argv[ac], "flush")) {
                cpy_persist = memcpy_then_persist;
                printf("persist=flush ");
            } else {
                printf("persist=non-temporal ");
            }
        }
    }

    if (total_size == 0) {
        total_size = memset_size + base_size;
    } else {

        if (total_size < base_size) total_size = base_size;

        memset_size = total_size - base_size;
    }


    iter = total_size / sizeof(uint64_t) - 1;
    printf("total_size=%d ", total_size);

    puts("");

    puts("\tbegin generating keys");
    uint64_t *keys = new uint64_t[n];
    uint64_t *rands = new uint64_t[n];


    // Generate keys
    for (uint64_t i = 0; i < n; i++) {
        rands[i] = rand();
    }

    // Generate keys
    for (uint64_t i = 0; i < n; i++) {
        keys[i] = i + 1;
    }

    // (TP dropped) shuffle the array

//#ifdef CLFLUSH
//    puts("\tdetected CLFLUSH");
//#elif CLFLUSH_OPT
//    puts("\tdetected CLFLUSH_OPT");
//#elif CLWB
//    puts("\tdetected CLWB");
//#else
//    puts("\tno available cache line write back found")
//#endif


    PMEM_POOL_SIZE = total_size * n * 3;
    uint64_t size_round = 4ULL * 1024ULL * 1024ULL * 1024ULL;
    while (size_round < PMEM_POOL_SIZE) {
        size_round += 4ULL * 1024ULL * 1024ULL * 1024ULL;
    }
    PMEM_POOL_SIZE = size_round;


    masstree::masstree *tree = NULL;

    if (require_RP_init) {

        int preset = 0;

        puts("\tbegin preparing Ralloc");
        int should_recover = RP_init("masstree", PMEM_POOL_SIZE, &preset);

//        int should_recover=(access("/pmem0/masstree_sb", F_OK) != -1);
//        RP_init("masstree", PMEM_POOL_SIZE, &preset);

        if (should_recover && which_memalign == RP_memalign) {

            tree = new masstree::masstree(RP_get_root<masstree::leafnode>(0));
            ralloc_reachability_scan(tree);

//            throw;
            goto_lookup = 1;

        } else if (should_recover) {
            tree = new masstree::masstree();
            ralloc_recover_scan(tree);
            goto_lookup = 1;
        }
    }

    if (tree == NULL) {
        puts("\t creating new tree");
        tree = new masstree::masstree();
    }


    if (require_obj_init) {

        puts("\tbegin preparing Obj");

        // Enable prefault
        int arg_open = 1, arg_create = 1;
        if ((pmemobj_ctl_set(pop, "prefault.at_open", &arg_open)) != 0)
            perror("failed to configure prefaults at open\n");
        if ((pmemobj_ctl_set(pop, "prefault.at_create", &arg_create)) != 0)
            perror("failed to configure prefaults at create\n");


        if (access("/pmem0/masstree_obj", F_OK) != -1) {
            pop = pmemobj_open("/pmem0/masstree_obj", POBJ_LAYOUT_NAME(masstree));
        } else {
            pop = pmemobj_create("/pmem0/masstree_obj", POBJ_LAYOUT_NAME(masstree),
                                 PMEM_POOL_SIZE, 0666);
        }

        masstree::obj_init(pop);
    }

//    tbb::task_scheduler_init init(num_thread);
    srand(time(NULL));

    FILE *throughput_file = fopen("perf.csv", "a");
    u_int64_t *latencies = NULL;

    pthread_attr_t *attrs = (pthread_attr_t *) calloc(num_thread, sizeof(pthread_attr_t));
    cpu_set_t *cpus = (cpu_set_t *) calloc(num_thread, sizeof(cpu_set_t));
    struct section_arg *section_args = (struct section_arg *) calloc(num_thread, sizeof(struct section_arg));

    uint64_t n_per_thread = n / num_thread;
    uint64_t n_remainder = n % num_thread;
    int numberOfProcessors = sysconf(_SC_NPROCESSORS_ONLN);
    printf("\tNumber of processors: %d\n", numberOfProcessors);


    for (int i = 0; i < num_thread; i++) {

        CPU_ZERO(cpus + i);
        CPU_SET(i + 1, cpus + i); // reserving CPU 0

        pthread_attr_init(attrs + i);
        pthread_attr_setaffinity_np(attrs + i, sizeof(cpu_set_t), cpus + i);

        if (i == 0) {
            section_args[i].start = 0;
        } else {
            section_args[i].start = section_args[i - 1].end;
        }

//        section_args[i].start = i;
        section_args[i].end = section_args[i].start + n_per_thread;
        if (n_remainder > 0) {
            n_remainder--;
            section_args[i].end++;
        }

//        printf("thread %d from %lu to %lu\n", i, section_args[i].start, section_args[i].end - 1);

        section_args[i].tree = tree;
        section_args[i].keys = keys;
        section_args[i].rands = rands;
        if (wl != NULL) section_args[i].rands = keys;

        if (i == 0) {
            latencies = (u_int64_t *) malloc(sizeof(u_int64_t) * section_args[i].end);
            section_args[i].latencies = latencies;
        } else {
            section_args[i].latencies = NULL;
        }

    }

    if (require_log_init) {

        puts("\tbegin preparing Log");

        if (
//                access(INODE_FN, F_OK) == 0 &&
//                access(META_FN, F_OK) == 0 &&
                access(LOG_FN, F_OK) == 0

                ) {
            log_recover(tree, num_thread);
            goto_lookup = 1;
        } else {
            log_init(PMEM_POOL_SIZE);
        }

        if (num_of_gc > 0) {
            puts("\tbegin creating Gc");

            int start_cpu;
            int end_cpu;

            if (interfere) {
                start_cpu = 1;
                end_cpu = 1 + num_thread;
            } else {
                start_cpu = 1 + num_thread;
                end_cpu = start_cpu + num_of_gc;
            }

            for (int gcc = 0; gcc < num_of_gc; gcc++) {
                log_start_gc(tree, start_cpu, end_cpu);
            }
        }
    }


    std::cout << "Simple Example of P-Masstree-New" << std::endl;
    printf("operation,n,ops/s\n");

    if (goto_lookup) goto lookup;

    {
        /**
         * section YCSB
         */
        if (wl != NULL) {
            puts("\t\t\t *** YCSB workload ***");
//            run("ycsb_load", throughput_file, attrs, section_args, latencies, section_ycsb_load);
            run("ycsb_load", throughput_file, attrs, section_args, latencies, section_insert);
            run("ycsb_run", throughput_file, attrs, section_args, latencies, section_ycsb_run);
            goto end;
        }
    }

    {
        /**
         * section INSERT
         */
        if (shuffle_keys) masstree_shuffle(keys, n);
        run("insert", throughput_file, attrs, section_args, latencies, section_insert);
    }

//    printf("count RP_MALLOC %lu\n", RP_lock_count);

    {
        /**
         * section UPDATE
         */
        if (shuffle_keys) masstree_shuffle(keys, n);
        run("update", throughput_file, attrs, section_args, latencies, section_update);
    }


    lookup:
    {
        /**
         * section LOOKUP
         */
        if (shuffle_keys) masstree_shuffle(keys, n);
        run("lookup", throughput_file, attrs, section_args, latencies, section_lookup);
    }


    if (which_memalign == RP_memalign) {
        RP_set_root(tree->root(), 0);
        printf("RP set root: %p\n", tree->root());
    }

    {
        /**
         * section DELETE
         */
        throw;
        if (shuffle_keys) masstree_shuffle(keys, n);
        run("delete", throughput_file, attrs, section_args, latencies, section_delete);
    }


    end:

    // logging throughput to files
    fclose(throughput_file);


    if (use_log) {
        log_join_all_gc();
    }


    delete[] keys;

    return 0;
}

