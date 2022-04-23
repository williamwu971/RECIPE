#include <iostream>
#include <chrono>
#include <random>
#include "tbb/tbb.h"

using namespace std;

#include "masstree.h"
#include "ralloc.hpp"

inline int RP_memalign(void **memptr, size_t alignment, size_t size){
    *memptr=RP_malloc(size+(alignment-size%alignment));
    return 0;
}

int (*which_memalign)(void **memptr, size_t alignment, size_t size);
void *(*which_malloc)(size_t size);
void (*which_free)(void *ptr);

void run(char **argv) {
    std::cout << "Simple Example of P-Masstree" << std::endl;

    uint64_t n = std::atoll(argv[1]);
    uint64_t *keys = new uint64_t[n]; // todo: insert random keys


    // Generate keys todo: random keys
    for (uint64_t i = 0; i < n; i++) {
//        keys[i] = i + 1;
        keys[i]=rand();
    }

    int num_thread = atoi(argv[2]);
    tbb::task_scheduler_init init(num_thread);


    which_memalign=posix_memalign;
    which_malloc=malloc;
    which_free=free;
    int require_init=0;

    for (int ac=0;ac<5;ac++){
        if (strcasestr(argv[ac],"index")){
            if (strcasestr(argv[ac],"pmem")){
                which_memalign=RP_memalign;
                require_init=1;
            }
        }else if (strcasestr(argv[ac],"value")){
            if (strcasestr(argv[ac],"pmem")){
                which_malloc=RP_malloc;
                which_free=RP_free;
                require_init=1;
            }
        }
    }

    if (require_init){
        RP_init("masstree",64*1024*1024*1024ULL);
    }

    double insert_throughput;
    double lookup_throughput;


    printf("operation,n,ops/s\n");
    masstree::masstree *tree = new masstree::masstree();

    {
        // Build tree
        auto starttime = std::chrono::system_clock::now();
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            auto t = tree->getThreadInfo();
            for (uint64_t i = range.begin(); i != range.end(); i++) {
//                tree->put(keys[i], &keys[i], t);


                // todo: if RP_malloc is used here, values will be stored in PMEM
                // todo: free memory, make templates/cpp (modular) <- important

                // todo: size randomize (YCSB/Facebook workload)
//                int size = rand()%2048+sizeof(uint64_t);
                int size = sizeof(uint64_t);


                uint64_t * value = (uint64_t *)which_malloc(size);

                // flush value before inserting
                *value=keys[i];
                asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *)(value)));
                asm volatile("sfence":::"memory");

                tree->put(keys[i], value, t);
            }
        });
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: insert,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: insert,%ld,%f sec\n", n, duration.count() / 1000000.0);
        insert_throughput=(n * 1.0) / duration.count();
    }

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
    if (argc != 5) {
        printf("usage: %s [n] [nthreads]\nn: number of keys (integer)\nnthreads: number of threads (integer)\n", argv[0]);
        return 1;
    }

    run(argv);
    return 0;
}