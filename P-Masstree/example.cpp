#include <iostream>
#include <chrono>
#include <random>
#include <libpmemobj.h>
#include "tbb/tbb.h"
#include "plog.cpp"


int (*which_memalign)(void **memptr, size_t alignment, size_t size);

void (*which_memfree)(void *ptr);

void *(*which_malloc)(size_t size);

void (*which_free)(void *ptr);

using namespace std;

#include "masstree.h"
#include "ralloc.hpp"

inline int RP_memalign(void **memptr, size_t alignment, size_t size) {
    *memptr = RP_malloc(size + (alignment - size % alignment));
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
        asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(ptr)));
#elif CLWB
        asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *) (ptr)));
#endif
    }
    if (back)
        asm volatile("sfence":: :"memory");
}

int main(int argc, char **argv) {

    if (argc != 10 && argc != 3) {
        printf("usage: %s [n] [nthreads]\nn: number of keys (integer)\nnthreads: number of threads (integer)\n",
               argv[0]);
        return 1;
    }

    // todo: make templates/cpp (modular) <- important
    which_memalign = posix_memalign;
    which_memfree = free;
    which_malloc = malloc;
    which_free = free;
    int require_RP_init = 0;
    int require_log_init = 0;
    int require_flush = 0;
    int shuffle_keys = 0;
    int use_perf = 0;
    int num_of_gc = 0;
    int show_log_usage = 1;
    int record_latency = 0;
    int display_throughput = 1;
    int use_obj = 0;
    int value_size = sizeof(struct log_cell) + sizeof(uint64_t);

    uint64_t n;
    int num_thread;


    for (int ac = 0; ac < argc; ac++) {

        if (ac == 1) {
            n = std::atoll(argv[1]);
            printf("\t\t\tn:%lu\n", n);

        } else if (ac == 2) {
            num_thread = atoi(argv[2]);
            printf("\t\t\tnum_thread:%d\n", num_thread);

        } else if (strcasestr(argv[ac], "index=")) {
            if (strcasestr(argv[ac], "pmem")) {
                which_memalign = RP_memalign;
                which_memfree = RP_free;
                require_RP_init = 1;
                printf(" === index using Ralloc === \n");

            } else if (strcasestr(argv[ac], "log")) {
                which_memalign = log_memalign;
                which_memfree = log_free;
                require_log_init = 1;
                printf(" === index using Log === \n");

            } else if (strcasestr(argv[ac], "obj")) {
                which_memalign = masstree::obj_memalign;
                which_memfree = masstree::obj_free;
                printf(" === index using Obj === \n");

            } else {
                printf(" === index using Dram === \n");

            }
        } else if (strcasestr(argv[ac], "value=")) {
            if (strcasestr(argv[ac], "pmem")) {
                which_malloc = RP_malloc;
                which_free = RP_free;
                require_RP_init = 1;
                require_flush = 1;
            } else if (strcasestr(argv[ac], "log")) {
                which_malloc = log_malloc;
                which_free = log_free;
                require_log_init = 1;
                require_flush = 1;
            } else if (strcasestr(argv[ac], "obj")) {
                use_obj = 1;
            }
        } else if (strcasestr(argv[ac], "key=")) {
            if (strcasestr(argv[ac], "rand")) {
                shuffle_keys = 1;
            }
        } else if (strcasestr(argv[ac], "perf=")) {
            if (strcasestr(argv[ac], "y")) {
                use_perf = 1;
            }
        } else if (strcasestr(argv[ac], "gc=")) {
            num_of_gc = atoi(strcasestr(argv[ac], "=") + 1);
        } else if (strcasestr(argv[ac], "latency=")) {
            if (strcasestr(argv[ac], "yes")) {
                record_latency = 1;
            }
        } else if (strcasestr(argv[ac], "value_size=")) {
            int desired_size = atoi(strcasestr(argv[ac], "=") + 1);
            if (desired_size > value_size) value_size = desired_size;
        }
    }
    printf("\n");


    std::cout << "Simple Example of P-Masstree-New" << std::endl;


    tbb::task_scheduler_init init(num_thread);

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


    FILE *throughput_file = fopen("perf.csv", "a");


#ifdef CLFLUSH
    puts(" === USING CLFLUSH === ");
#elif CLFLUSH_OPT
    puts(" === USING CLFLUSH_OPT === ");
#elif CLWB
    puts(" === USING CLWB === ");
#endif

#define PMEM_POOL_SIZE (48*1024*1024*1024ULL)

    if (require_RP_init) {
        printf("init RP... ");
        int preset = 0;
        RP_init("masstree", PMEM_POOL_SIZE, &preset);
    }


    // todo: obj

    POBJ_LAYOUT_BEGIN(masstree);
    POBJ_LAYOUT_TOID(masstree, struct masstree_obj);
    POBJ_LAYOUT_END(masstree);

    struct masstree_obj {
        TOID(struct masstree_obj) objToid;
        PMEMoid ht_oid;
        uint64_t data;
    };

#define OBJ_FN "/pmem0/masstree_obj"
    PMEMobjpool *pop = NULL;

    if (use_obj) {

        // Enable prefault
        int arg_open = 1, arg_create = 1;
        if ((pmemobj_ctl_set(pop, "prefault.at_open", &arg_open)) != 0)
            perror("failed to configure prefaults at open\n");
        if ((pmemobj_ctl_set(pop, "prefault.at_create", &arg_create)) != 0)
            perror("failed to configure prefaults at create\n");


        if (access(OBJ_FN, F_OK) != -1) {
            pop = pmemobj_open(OBJ_FN, POBJ_LAYOUT_NAME(masstree));
        } else {
            pop = pmemobj_create(OBJ_FN, POBJ_LAYOUT_NAME(masstree),
                                 PMEM_POOL_SIZE, 0666);
        }

        masstree::obj_init(pop);

    }


    if (use_perf)printf("WARNING: PERF is enabled!\n");
    if (num_of_gc)printf("WARNING: GC is enabled %d\n", num_of_gc);

    // todo: add latency tracker and perf

    // (TP dropped) shuffle the array todo: random keys (make it faster)
    if (shuffle_keys) {
        printf("shuffle keys... ");
        fflush(stdout);

        srand(time(NULL));
        for (uint64_t i = 0; i < n - 1; i++) {
            uint64_t j = i + rand() / (RAND_MAX / (n - i) + 1);
            uint64_t t = keys[j];
            keys[j] = keys[i];
            keys[i] = t;
        }
    }

    double insert_throughput;
    double update_throughput;
    double lookup_throughput;
    u_int64_t *latencies = (u_int64_t *) malloc(sizeof(u_int64_t) * n);

    masstree::masstree *tree = new masstree::masstree();

    if (require_log_init) {
        printf("init log... ");
        fflush(stdout);

        if (
                access(INODE_FN, F_OK) == 0 &&
                access(LOG_FN, F_OK) == 0 &&
                access(META_FN, F_OK) == 0
                ) {
            log_recover(tree, 20);
            goto lookup;
        } else {
            log_init(PMEM_POOL_SIZE, num_thread);
        }

        if (which_malloc == log_malloc) {
            printf("spawn GC... ");
            fflush(stdout);
            for (int gcc = 0; gcc < num_of_gc; gcc++) {
                log_start_gc(tree);
            }
        }
    }


    printf("\n");
    printf("operation,n,ops/s\n");

    {
        const char *perf_fn = "insert.perf";
        if (use_perf)log_start_perf(perf_fn);

        // Build tree
        auto starttime = std::chrono::system_clock::now();
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            auto t = tree->getThreadInfo();
            for (uint64_t i = range.begin(); i != range.end(); i++) {


                if (use_obj) {


                    PMEMoid ht_oid;
                    if (pmemobj_alloc(pop, &ht_oid,
                                      value_size, TOID_TYPE_NUM(struct masstree_obj),
                                      0, 0)) {
                        fprintf(stderr, "pmemobj_alloc failed for obj_memalign\n");
                        assert(0);
                    }
                    struct masstree_obj *mo = (struct masstree_obj *) pmemobj_direct(ht_oid);
                    mo->data = rands[i];
                    mo->ht_oid = ht_oid;
                    tree->put_and_return(keys[i], mo, 1, 0, t);
                    continue;


                    TX_BEGIN(pop) {

                                    TOID(struct masstree_obj) objToid =
                                            TX_ALLOC(struct masstree_obj, value_size);

                                            D_RW(objToid)->objToid = objToid;
                                            D_RW(objToid)->data = rands[i];

//                                    memset(((uint64_t *) (&D_RW(objToid)->data)) + 1, 7,
//                                           value_size - sizeof(struct masstree_obj)
//                                    );


                                    tree->put_and_return(keys[i], D_RW(objToid), 1, 0, t);


                                }
                                    TX_ONABORT {
                                    throw;
                                }
                    TX_END

                    continue;
                }

//                tree->put(keys[i], &keys[i], t);

                // todo: size randomize (YCSB/Facebook workload)
//                int raw_size = 1024;
//                int raw_size = sizeof(struct log_cell) + sizeof(uint64_t);
                int raw_size = value_size;

                char *raw = (char *) which_malloc(raw_size);

                struct log_cell *lc = (struct log_cell *) raw;
                lc->value_size = raw_size - sizeof(struct log_cell);
                lc->is_delete = 0;
                lc->key = keys[i];
                rdtscll(lc->version);
//                lc->reference = 0;

                uint64_t *value = (uint64_t *) (raw + sizeof(struct log_cell));
                *value = rands[i];
//                memset(value + 1, 7, raw_size - sizeof(struct log_cell) - sizeof(uint64_t));

                // flush value before inserting todo: should this exist for DRAM+DRAM?

                if (require_flush) {
//                    clflush(raw, raw_size, true, true);
//                    pmem_persist(raw, raw_size);
                    pmem_persist(raw, sizeof(struct log_cell) + sizeof(uint64_t));
                    pmem_memset_persist(value + 1, 7, raw_size - sizeof(struct log_cell) - sizeof(uint64_t));

                }
                tree->put_and_return(keys[i], raw, 1, 0, t);
            }
        });
        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);

        if (use_perf) {
            log_stop_perf();
            log_print_pmem_bandwidth(perf_fn, duration.count() / 1000000.0, throughput_file);
        }
        if (display_throughput)
            printf("Throughput: insert,%ld,%.2f ops/us %.2f sec\n",
                   n, (n * 1.0) / duration.count(), duration.count() / 1000000.0);
        insert_throughput = (n * 1.0) / duration.count();


        fprintf(throughput_file, "%.2f,", insert_throughput);
    }
    if (which_malloc == log_malloc) log_debug_print(1, show_log_usage);
    {

        const char *perf_fn = num_of_gc ? "update_gc.perf" : "update.perf";
        if (use_perf)log_start_perf(perf_fn);

        // Update
        auto starttime = std::chrono::system_clock::now();
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            auto t = tree->getThreadInfo();
            for (uint64_t i = range.begin(); i != range.end(); i++) {

                if (use_obj) {

                    PMEMoid ht_oid;
                    if (pmemobj_alloc(pop, &ht_oid,
                                      value_size, TOID_TYPE_NUM(struct masstree_obj),
                                      0, 0)) {
                        fprintf(stderr, "pmemobj_alloc failed for obj_memalign\n");
                        assert(0);
                    }
                    struct masstree_obj *mo = (struct masstree_obj *) pmemobj_direct(ht_oid);
                    mo->data = keys[i];
                    mo->ht_oid = ht_oid;
                    struct masstree_obj *old_obj =
                            (struct masstree_obj *)
                                    tree->put_and_return(keys[i], mo, 1, 0, t);

//                    pmemobj_free(&old_obj->ht_oid);


                    continue;

                    TX_BEGIN(pop) {

                                    TOID(struct masstree_obj) objToid =
                                            TX_ALLOC(struct masstree_obj, value_size);

                                            D_RW(objToid)->objToid = objToid;
                                            D_RW(objToid)->data = keys[i];

//                                    memset(((uint64_t *) (&D_RW(objToid)->data)) + 1, 7,
//                                           value_size - sizeof(struct masstree_obj)
//                                    );

//                                    printf("key: %lu pointer: %p\n", keys[i],
//                                           tree->get(keys[i], t));

                                    struct masstree_obj *obj = (struct masstree_obj *)
                                            tree->put_and_return(keys[i], D_RW(objToid), 0, 0, t);


//                                    printf("key: %lu pointer: %p\n", keys[i], obj);

                                    TX_FREE(obj->objToid);

                                }
                                    TX_ONABORT {
                                    throw;
                                }
                    TX_END

                    continue;
                }

                u_int64_t a = 0;
                u_int64_t b = 0;

//                if (record_latency) rdtscll(a);

//                char* raw =(char*) tree->get(keys[i], t);
//                char* raw = (char*)tree->del_and_return(keys[i],0,0,t);
//                uint64_t *ret = reinterpret_cast<uint64_t *> (raw+sizeof(struct log_cell));
//                uint64_t *ret = reinterpret_cast<uint64_t *> (tree->get(keys[i], t));

//                int raw_size = 1024;
//                int raw_size = sizeof(struct log_cell) + sizeof(uint64_t);
                int raw_size = value_size;
                char *raw = (char *) which_malloc(raw_size);

                struct log_cell *lc = (struct log_cell *) raw;
                lc->value_size = raw_size - sizeof(struct log_cell);
                lc->is_delete = 0;
                lc->key = keys[i];
                rdtscll(lc->version);
//                lc->reference = 0;

                uint64_t *value = (uint64_t *) (raw + sizeof(struct log_cell));
                *value = keys[i];
//                memset(value + 1, 7, raw_size - sizeof(struct log_cell) - sizeof(uint64_t));

//                if (record_latency) rdtscll(b);
//                if (record_latency) rdtscll(a);

                // flush value before inserting todo: should this exist for DRAM+DRAM?

                if (require_flush) {
//                    clflush(raw, raw_size, true, true);
//                    pmem_persist(raw, raw_size);
                    pmem_persist(raw, sizeof(struct log_cell) + sizeof(uint64_t));
                    pmem_memset_persist(value + 1, 7, raw_size - sizeof(struct log_cell) - sizeof(uint64_t));

                }
//                if (record_latency) rdtscll(b);
                if (record_latency) rdtscll_fence(a);

                void *old = (char *) tree->put_and_return(keys[i], raw, 0, 0, t);

                if (record_latency) rdtscll_fence(b);

                which_free(old);

                if (record_latency)latencies[i] = b - a;

            }
        });

        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);

        if (use_perf) {
            log_stop_perf();
            log_print_pmem_bandwidth(perf_fn, duration.count() / 1000000.0, throughput_file);
        }

        if (display_throughput)
            printf("Throughput: update,%ld,%.2f ops/us %.2f sec\n",
                   n, (n * 1.0) / duration.count(), duration.count() / 1000000.0);
        update_throughput = (n * 1.0) / duration.count();

        fprintf(throughput_file, "%.2f,", update_throughput);
    }

    lookup:
    if (which_malloc == log_malloc) log_debug_print(0, show_log_usage);

    {
        const char *perf_fn = "lookup.perf";
        if (use_perf)log_start_perf(perf_fn);

        // Lookup
        auto starttime = std::chrono::system_clock::now();
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            auto t = tree->getThreadInfo();
            for (uint64_t i = range.begin(); i != range.end(); i++) {

                if (use_obj) {

                    struct masstree_obj *obj = (struct masstree_obj *) tree->get(keys[i], t);
                    if (obj->data != keys[i]) {
                        std::cout
                                << "wrong value read: " << obj->data
                                << " expected:" << keys[i]
                                << std::endl;
//                    printf("version:%lu, key:%lu, value_size:%lu, is_delete:%lu\n",
//                           lc->version, lc->key, lc->value_size, lc->is_delete);
                        throw;
                    }

                    continue;
                }

                char *raw = (char *) tree->get(keys[i], t);

                struct log_cell *lc = (struct log_cell *) raw;

                uint64_t *ret = reinterpret_cast<uint64_t *> (raw + sizeof(struct log_cell));
                if (*ret != keys[i]) {
                    std::cout
                            << "wrong value read: " << *ret
                            << " expected:" << keys[i]
                            << " version:" << lc->version
                            << " key:" << lc->key
                            << " value_size:" << lc->value_size
                            << " is_delete:" << lc->is_delete
                            << std::endl;
//                    printf("version:%lu, key:%lu, value_size:%lu, is_delete:%lu\n",
//                           lc->version, lc->key, lc->value_size, lc->is_delete);
                    throw;
                }
            }
        });

        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);

        if (use_perf) {
            log_stop_perf();
            log_print_pmem_bandwidth(perf_fn, duration.count() / 1000000.0, throughput_file);
        }

        if (display_throughput)
            printf("Throughput: lookup,%ld,%.2f ops/us %.2f sec\n",
                   n, (n * 1.0) / duration.count(), duration.count() / 1000000.0);
        lookup_throughput = (n * 1.0) / duration.count();

        fprintf(throughput_file, "%.2f,", lookup_throughput);
    }

    if (which_malloc == log_malloc) log_debug_print(0, show_log_usage);

    {

        const char *perf_fn = "delete.perf";
        if (use_perf)log_start_perf(perf_fn);

        void *(*tombstone_callback_func)(uint64_t key) = NULL;
        if (which_malloc == log_malloc) {
            tombstone_callback_func = log_get_tombstone;
        }

        // Delete
        auto starttime = std::chrono::system_clock::now();
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            auto t = tree->getThreadInfo();
            for (uint64_t i = range.begin(); i != range.end(); i++) {


                if (use_obj) {

                    struct masstree_obj *old_obj = (struct masstree_obj *)
                            tree->del_and_return(keys[i], 0, 0,
                                                 tombstone_callback_func, t);
                    pmemobj_free(&old_obj->ht_oid);
                    continue;


                    TX_BEGIN(pop) {

                                    struct masstree_obj *obj = (struct masstree_obj *)
                                            tree->del_and_return(keys[i], 0, 0,
                                                                 tombstone_callback_func, t);

                                    TX_FREE(obj->objToid);

                                }
                                    TX_ONABORT {
                                    throw;
                                }
                    TX_END

                    continue;
                }

                void *old = tree->del_and_return(keys[i], 0, 0,
                                                 tombstone_callback_func, t);

                which_free(old);

                // todo: write -1 here
            }
        });

        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);

        if (use_perf) {
            log_stop_perf();
            log_print_pmem_bandwidth(perf_fn, duration.count() / 1000000.0, throughput_file);
        }

        if (display_throughput)
            printf("Throughput: delete,%ld,%.2f ops/us %.2f sec\n",
                   n, (n * 1.0) / duration.count(), duration.count() / 1000000.0);
        update_throughput = (n * 1.0) / duration.count();

        fprintf(throughput_file, "%.2f,", update_throughput);
    }
    if (which_malloc == log_malloc) log_debug_print(0, show_log_usage);


    // logging throughput to files
    fclose(throughput_file);


    if (record_latency) {
        FILE *latency_file = fopen("latency.csv", "w");
        for (uint64_t idx = 0; idx < n; idx++) {
            fprintf(latency_file, "%lu\n", latencies[idx]);
        }
        fclose(latency_file);
    }

    if (num_of_gc > 0 && which_malloc == log_malloc)
        log_join_all_gc();
    if (which_malloc == log_malloc) log_debug_print(2, show_log_usage);

    delete[] keys;

    return 0;
}

