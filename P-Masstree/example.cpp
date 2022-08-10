#include <iostream>
#include <chrono>
#include <random>
#include <libpmemobj.h>
#include "tbb/tbb.h"
#include "plog.cpp"

// todo: make templates/cpp (modular) <- important
int (*which_memalign)(void **memptr, size_t alignment, size_t size) = posix_memalign;

void (*which_memfree)(void *ptr) =free;

int require_RP_init = 0;
int require_log_init = 0;
int require_obj_init = 0;
//int require_flush = 0;
int shuffle_keys = 0;
int use_perf = 0;
int num_of_gc = 0;
int show_log_usage = 1;
int record_latency = 0;
int display_throughput = 1;
int use_obj = 0;
int use_ralloc = 0;
int use_log = 0;
int value_size = sizeof(struct log_cell) + sizeof(uint64_t);
int memset_size = 0;

uint64_t n;
int num_thread;
PMEMobjpool *pop = NULL;

POBJ_LAYOUT_BEGIN(masstree);
POBJ_LAYOUT_TOID(masstree, struct masstree_obj);
POBJ_LAYOUT_END(masstree);

struct masstree_obj {
    TOID(struct masstree_obj) objToid;
    PMEMoid ht_oid;
    uint64_t data;
};

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


void dump_latencies(const char *fn, u_int64_t *numbers, uint64_t length) {
    FILE *latency_file = fopen(fn, "w");
    for (uint64_t idx = 0; idx < length; idx++) {
        fprintf(latency_file, "%lu\n", numbers[idx]);
    }
    fclose(latency_file);
}

int main(int argc, char **argv) {

    if (argc != 10 && argc != 3) {
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
            if (strcasestr(argv[ac], "pmem")) {
                which_memalign = RP_memalign;
                which_memfree = RP_free;
                require_RP_init = 1;
                printf("index=pmem ");

            } else if (strcasestr(argv[ac], "log")) {
                which_memalign = log_memalign;
                which_memfree = log_free;
                require_log_init = 1;
                printf("index=log ");

            } else if (strcasestr(argv[ac], "obj")) {
                which_memalign = masstree::obj_memalign;
                which_memfree = masstree::obj_free;
                require_obj_init = 1;
                printf("index=obj ");

            } else {
                printf("index=dram ");

            }
        } else if (strcasestr(argv[ac], "value=")) {
            if (strcasestr(argv[ac], "pmem")) {
                require_RP_init = 1;
                use_ralloc = 1;
                memset_size = value_size - sizeof(uint64_t);
                printf("value=pmem ");

            } else if (strcasestr(argv[ac], "log")) {
                require_log_init = 1;
                use_log = 1;
                memset_size = value_size - sizeof(uint64_t) - sizeof(struct log_cell);
                printf("value=log ");

            } else if (strcasestr(argv[ac], "obj")) {
                use_obj = 1;
                require_obj_init = 1;
                memset_size = value_size - sizeof(struct masstree_obj);
                printf("value=obj ");


            } else {
                printf("value=dram ");

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
        } else if (strcasestr(argv[ac], "value_size=")) {
            int desired_size = atoi(strcasestr(argv[ac], "=") + 1);
            if (desired_size > value_size) value_size = desired_size;
            printf("value_size=%d ", value_size);

        }
    }
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
    if (shuffle_keys) {

        for (uint64_t i = 0; i < n - 1; i++) {
            uint64_t j = i + rand() / (RAND_MAX / (n - i) + 1);
            uint64_t t = keys[j];
            keys[j] = keys[i];
            keys[i] = t;
        }
    }

#ifdef CLFLUSH
    puts("\tdetected CLFLUSH");
#elif CLFLUSH_OPT
    puts("\tdetected CLFLUSH_OPT");
#elif CLWB
    puts("\tdetected CLWB");
#else
    puts("\tno available cache line write back found")
#endif

#ifdef MASSTREE_FLUSH
    puts("\tMASSTREE_FLUSH");
#else
    puts("\ttesting eADR");
#endif

#define PMEM_POOL_SIZE (8*1024*1024*1024ULL)

    if (require_RP_init) {
        puts("\tbegin preparing Ralloc");
        int preset = 0;
        RP_init("masstree", PMEM_POOL_SIZE, &preset);
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

    tbb::task_scheduler_init init(num_thread);
    srand(time(NULL));
    masstree::masstree *tree = new masstree::masstree();

    FILE *throughput_file = fopen("perf.csv", "a");
    u_int64_t *latencies = (u_int64_t *) malloc(sizeof(u_int64_t) * n);

    if (require_log_init) {

        puts("\tbegin preparing Log");

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

        if (num_of_gc > 0) {
            puts("\tbegin creating Gc");
            for (int gcc = 0; gcc < num_of_gc; gcc++) {
                log_start_gc(tree);
            }
        }
    }


    std::cout << "Simple Example of P-Masstree-New" << std::endl;
    printf("operation,n,ops/s\n");


    {

        /**
         * section INSERT
         */
        const char *perf_fn = "insert.perf";
        if (use_perf)log_start_perf(perf_fn);


        // Build tree
        auto starttime = std::chrono::system_clock::now();
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            auto t = tree->getThreadInfo();
            u_int64_t a, b;

            for (uint64_t i = range.begin(); i != range.end(); i++) {

                rdtscll(a);

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
#ifdef MASSTREE_FLUSH
                    pmemobj_persist(pop, mo, sizeof(struct masstree_obj));
                    pmemobj_memset_persist(pop, mo + 1, 7, memset_size);
#endif

                    tree->put_and_return(keys[i], mo, 1, 0, t);


//                    TX_BEGIN(pop) {
//
//                                    TOID(struct masstree_obj) objToid =
//                                            TX_ALLOC(struct masstree_obj, value_size);
//
//                                            D_RW(objToid)->objToid = objToid;
//                                            D_RW(objToid)->data = rands[i];
//
//                                    memset(((uint64_t *) (&D_RW(objToid)->data)) + 1, 7,
//                                           value_size - sizeof(struct masstree_obj)
//                                    );
//
//
//                                    tree->put_and_return(keys[i], D_RW(objToid), 1, 0, t);
//
//
//                                }
//                                    TX_ONABORT {
//                                    throw;
//                                }
//                    TX_END

                } else if (use_log) {

                    char *raw = (char *) log_malloc(value_size);

                    struct log_cell *lc = (struct log_cell *) raw;
                    lc->value_size = value_size - sizeof(struct log_cell);
                    lc->is_delete = 0;
                    lc->key = keys[i];
                    rdtscll(lc->version);

                    uint64_t *value = (uint64_t *) (raw + sizeof(struct log_cell));
                    *value = rands[i];
//                memset(value + 1, 7, raw_size - sizeof(struct log_cell) - sizeof(uint64_t));
//                    pmem_persist(raw, raw_size);

#ifdef MASSTREE_FLUSH

                    pmem_persist(raw, sizeof(struct log_cell) + sizeof(uint64_t));
                    pmem_memset_persist(value + 1, 7, memset_size);
#endif


                    tree->put_and_return(keys[i], raw, 1, 0, t);

                } else if (use_ralloc) {

                    uint64_t *value = (uint64_t *) RP_malloc(value_size);
                    *value = rands[i];
                    memset(value + 1, 7, memset_size);
#ifdef MASSTREE_FLUSH
                    clflush((char *) value, value_size, true, true);
#endif
                    tree->put_and_return(keys[i], value, 1, 0, t);

                } else {

                    uint64_t *value = (uint64_t *) malloc(value_size);
                    *value = rands[i];
                    memset(value + 1, 7, value_size - sizeof(uint64_t));
                    tree->put_and_return(keys[i], value, 1, 0, t);

                }


                rdtscll(b);
                latencies[i] = b - a;
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

        if (record_latency) dump_latencies("insert.latencies", latencies, n);

        fprintf(throughput_file, "%.2f,", (n * 1.0) / duration.count());
        if (use_log) log_debug_print(1, show_log_usage);
    }


    {
        /**
         * section UPDATE
         */

        const char *perf_fn = num_of_gc ? "update_gc.perf" : "update.perf";
        if (use_perf)log_start_perf(perf_fn);

        // Update
        auto starttime = std::chrono::system_clock::now();
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            auto t = tree->getThreadInfo();
            u_int64_t a, b;

            for (uint64_t i = range.begin(); i != range.end(); i++) {

                rdtscll(a);

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

#ifdef MASSTREE_FLUSH
                    pmemobj_persist(pop, mo, sizeof(struct masstree_obj));
                    pmemobj_memset_persist(pop, mo + 1, 7, memset_size);
#endif

                    struct masstree_obj *old_obj =
                            (struct masstree_obj *)
                                    tree->put_and_return(keys[i], mo, 1, 0, t);

                    pmemobj_free(&old_obj->ht_oid);

//                    TX_BEGIN(pop) {
//
//                                    TOID(struct masstree_obj) objToid =
//                                            TX_ALLOC(struct masstree_obj, value_size);
//
//                                            D_RW(objToid)->objToid = objToid;
//                                            D_RW(objToid)->data = keys[i];
//
//                                    memset(((uint64_t *) (&D_RW(objToid)->data)) + 1, 7,
//                                           value_size - sizeof(struct masstree_obj)
//                                    );
//
//                                    printf("key: %lu pointer: %p\n", keys[i],
//                                           tree->get(keys[i], t));
//
//                                    struct masstree_obj *obj = (struct masstree_obj *)
//                                            tree->put_and_return(keys[i], D_RW(objToid), 0, 0, t);
//
//
//                                    printf("key: %lu pointer: %p\n", keys[i], obj);
//
//                                    TX_FREE(obj->objToid);
//
//                                }
//                                    TX_ONABORT {
//                                    throw;
//                                }
//                    TX_END

                } else if (use_log) {
                    char *raw = (char *) log_malloc(value_size);

                    struct log_cell *lc = (struct log_cell *) raw;
                    lc->value_size = value_size - sizeof(struct log_cell);
                    lc->is_delete = 0;
                    lc->key = keys[i];
                    rdtscll(lc->version);

                    uint64_t *value = (uint64_t *) (raw + sizeof(struct log_cell));
                    *value = keys[i];


//                    pmem_persist(raw, raw_size);
#ifdef MASSTREE_FLUSH
                    pmem_persist(raw, sizeof(struct log_cell) + sizeof(uint64_t));
                    pmem_memset_persist(value + 1, 7, memset_size);
#endif


                    log_free(tree->put_and_return(keys[i], raw, 0, 0, t));

                } else if (use_ralloc) {

                    uint64_t *value = (uint64_t *) RP_malloc(value_size);
                    *value = keys[i];
                    memset(value + 1, 7, memset_size);
#ifdef MASSTREE_FLUSH
                    clflush((char *) value, value_size, true, true);
#endif
                    RP_free(tree->put_and_return(keys[i], value, 0, 0, t));

                } else {

                    uint64_t *value = (uint64_t *) malloc(value_size);
                    *value = keys[i];
                    memset(value + 1, 7, memset_size);
                    free(tree->put_and_return(keys[i], value, 0, 0, t));

                }


                rdtscll(b)
                latencies[i] = b - a;
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

        if (record_latency) dump_latencies("update.latencies", latencies, n);

        fprintf(throughput_file, "%.2f,", (n * 1.0) / duration.count());
        if (use_log) log_debug_print(0, show_log_usage);
    }

    lookup:
    {
        /**
         * section LOOKUP
         */
        const char *perf_fn = "lookup.perf";
        if (use_perf)log_start_perf(perf_fn);

        // Lookup
        auto starttime = std::chrono::system_clock::now();
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            auto t = tree->getThreadInfo();
            u_int64_t a, b;

            for (uint64_t i = range.begin(); i != range.end(); i++) {

                rdtscll(a);

                if (use_obj) {

                    struct masstree_obj *obj = (struct masstree_obj *) tree->get(keys[i], t);
                    if (obj->data != keys[i]) {
                        std::cout
                                << "wrong value read: " << obj->data
                                << " expected:" << keys[i]
                                << std::endl;
                        throw;
                    }

                    continue;
                } else if (use_log) {

                    struct log_cell *lc = (struct log_cell *) tree->get(keys[i], t);
                    uint64_t *ret = reinterpret_cast<uint64_t *> (lc + 1);
                    if (*ret != keys[i]) {
                        std::cout
                                << "wrong value read: " << *ret
                                << " expected:" << keys[i]
                                << " version:" << lc->version
                                << " key:" << lc->key
                                << " value_size:" << lc->value_size
                                << " is_delete:" << lc->is_delete
                                << std::endl;
                        throw;
                    }
                } else {
                    uint64_t *ret = reinterpret_cast<uint64_t *> (tree->get(keys[i], t));
                    if (*ret != keys[i]) {
                        std::cout
                                << "wrong value read: " << *ret
                                << " expected:" << keys[i]
                                << std::endl;
                        throw;
                    }
                }

                rdtscll(b);
                latencies[i] = b - a;

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

        if (record_latency) dump_latencies("lookup.latencies", latencies, n);

        fprintf(throughput_file, "%.2f,", (n * 1.0) / duration.count());
        if (use_log) log_debug_print(0, show_log_usage);
    }

    {
        /**
         * section DELETE
         */

        const char *perf_fn = "delete.perf";
        if (use_perf)log_start_perf(perf_fn);

        // Delete
        auto starttime = std::chrono::system_clock::now();
        tbb::parallel_for(tbb::blocked_range<uint64_t>(0, n), [&](const tbb::blocked_range<uint64_t> &range) {
            auto t = tree->getThreadInfo();
            u_int64_t a, b;

            for (uint64_t i = range.begin(); i != range.end(); i++) {

                rdtscll(a);

                if (use_obj) {

                    struct masstree_obj *old_obj = (struct masstree_obj *)
                            tree->del_and_return(keys[i], 0, 0,
                                                 NULL, t);
                    pmemobj_free(&old_obj->ht_oid);


//                    TX_BEGIN(pop) {
//
//                                    struct masstree_obj *obj = (struct masstree_obj *)
//                                            tree->del_and_return(keys[i], 0, 0,
//                                                                 tombstone_callback_func, t);
//
//                                    TX_FREE(obj->objToid);
//
//                                }
//                                    TX_ONABORT {
//                                    throw;
//                                }
//                    TX_END
                } else if (use_log) {
                    log_free(tree->del_and_return(keys[i], 0, 0,
                                                  log_get_tombstone, t));
                } else if (use_ralloc) {
                    RP_free(tree->del_and_return(keys[i], 0, 0,
                                                 NULL, t));
                } else {
                    free(tree->del_and_return(keys[i], 0, 0,
                                              NULL, t));
                }

                rdtscll(b);
                latencies[i] = b - a;

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

        if (record_latency) dump_latencies("delete.latencies", latencies, n);

        fprintf(throughput_file, "%.2f,", (n * 1.0) / duration.count());
        if (use_log) log_debug_print(0, show_log_usage);
    }


    // logging throughput to files
    fclose(throughput_file);


    if (use_log) {
        log_join_all_gc();
        log_debug_print(2, show_log_usage);
    }


    delete[] keys;

    return 0;
}

