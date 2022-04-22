#include <iostream>
#include <chrono>
#include <random>
#include "tbb/tbb.h"

using namespace std;

#include "masstree.h"
#include "ralloc.hpp"

void run(char **argv) {
    std::cout << "Simple Example of P-Masstree" << std::endl;

    uint64_t n = std::atoll(argv[1]);
    uint64_t *keys = new uint64_t[n];


    // init Ralloc with 64G pool
    RP_init("masstree",64*1024*1024*1024ULL);


    // Generate keys
    for (uint64_t i = 0; i < n; i++) {
        keys[i] = i + 1;
    }

    int num_thread = atoi(argv[2]);
    tbb::task_scheduler_init init(num_thread);

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

//                int size = rand()%2048+sizeof(uint64_t);
                int size = sizeof(uint64_t);

                uint64_t * value = (uint64_t *)RP_malloc(size);
                uint64_t * value = (uint64_t *)RP_malloc(size);


                *value=keys[i];
                tree->put(keys[i], value, t);
            }
        });
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: insert,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: insert,%ld,%f sec\n", n, duration.count() / 1000000.0);
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
            }
        });
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);
        printf("Throughput: lookup,%ld,%f ops/us\n", n, (n * 1.0) / duration.count());
        printf("Elapsed time: lookup,%ld,%f sec\n", n, duration.count() / 1000000.0);
    }

    delete[] keys;
}

int main(int argc, char **argv) {
    if (argc != 3) {
        printf("usage: %s [n] [nthreads]\nn: number of keys (integer)\nnthreads: number of threads (integer)\n", argv[0]);
        return 1;
    }

    run(argv);
    return 0;
}