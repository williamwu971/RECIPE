#include <iostream>
#include <chrono>
#include <random>
#include <cstring>
#include <vector>
#include <fstream>
#include <iostream>
#include <stdlib.h>
#include "tbb/tbb.h"

using namespace std;

#include "P-ART/Tree.h"
#include "third-party/FAST_FAIR/btree.h"
#include "third-party/CCEH/src/Level_hashing.h"
#include "third-party/CCEH/src/CCEH.h"
#include "third-party/WOART/woart.h"
#include "masstree.h"
#include "P-BwTree/src/bwtree.h"
#include "clht.h"
#include "ssmem.h"

#ifdef HOT

#include <hot/rowex/HOTRowex.hpp>
#include <idx/contenthelpers/IdentityKeyExtractor.hpp>
#include <idx/contenthelpers/OptionalValue.hpp>

#endif

using namespace wangziqi2013::bwtree;

// index types
enum {
    TYPE_MASSTREE,
};

enum {
    OP_INSERT,
    OP_UPDATE,
    OP_READ,
    OP_SCAN,
    OP_DELETE,
};

enum {
    WORKLOAD_A,
    WORKLOAD_B,
    WORKLOAD_C,
    WORKLOAD_D,
    WORKLOAD_E,
};

enum {
    RANDINT_KEY,
};

enum {
    UNIFORM,
    ZIPFIAN,
};

namespace Dummy {
    inline void mfence() { asm volatile("mfence":: :"memory"); }

    inline void clflush(char *data, int len, bool front, bool back) {
        if (front)
            mfence();
        volatile char *ptr = (char *) ((unsigned long) data & ~(64 - 1));
        for (; ptr < data + len; ptr += 64) {
#ifdef CLFLUSH
            asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
#elif CLFLUSH_OPT
            asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(ptr)));
#elif CLWB
            asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *) (ptr)));
#endif
        }
        if (back)
            mfence();
    }
}


////////////////////////Helper functions for P-BwTree/////////////////////////////
/*
 * class KeyComparator - Test whether BwTree supports context
 *                       sensitive key comparator
 *
 * If a context-sensitive KeyComparator object is being used
 * then it should follow rules like:
 *   1. There could be no default constructor
 *   2. There MUST be a copy constructor
 *   3. operator() must be const
 *
 */
class KeyComparator {
public:
    inline bool operator()(const long int k1, const long int k2) const {
        return k1 < k2;
    }

    inline bool operator()(const uint64_t k1, const uint64_t k2) const {
        return k1 < k2;
    }

    inline bool operator()(const char *k1, const char *k2) const {
        return memcmp(k1, k2, strlen(k1) > strlen(k2) ? strlen(k1) : strlen(k2)) < 0;
    }

    KeyComparator(int dummy) {
        (void) dummy;

        return;
    }

    KeyComparator() = delete;
    //KeyComparator(const KeyComparator &p_key_cmp_obj) = delete;
};

/*
 * class KeyEqualityChecker - Tests context sensitive key equality
 *                            checker inside BwTree
 *
 * NOTE: This class is only used in KeyEqual() function, and is not
 * used as STL template argument, it is not necessary to provide
 * the object everytime a container is initialized
 */
class KeyEqualityChecker {
public:
    inline bool operator()(const long int k1, const long int k2) const {
        return k1 == k2;
    }

    inline bool operator()(uint64_t k1, uint64_t k2) const {
        return k1 == k2;
    }

    inline bool operator()(const char *k1, const char *k2) const {
        if (strlen(k1) != strlen(k2))
            return false;
        else
            return memcmp(k1, k2, strlen(k1)) == 0;
    }

    KeyEqualityChecker(int dummy) {
        (void) dummy;

        return;
    }

    KeyEqualityChecker() = delete;
    //KeyEqualityChecker(const KeyEqualityChecker &p_key_eq_obj) = delete;
};
/////////////////////////////////////////////////////////////////////////////////

////////////////////////Helper functions for P-HOT/////////////////////////////
typedef struct IntKeyVal {
    uint64_t key;
    uintptr_t value;
} IntKeyVal;

template<typename ValueType = IntKeyVal *>
class IntKeyExtractor {
public:
    typedef uint64_t KeyType;

    inline KeyType operator()(ValueType const &value) const {
        return value->key;
    }
};

template<typename ValueType = Key *>
class KeyExtractor {
public:
    typedef char const *KeyType;

    inline KeyType operator()(ValueType const &value) const {
        return (char const *) value->fkey;
    }
};
/////////////////////////////////////////////////////////////////////////////////

////////////////////////Helper functions for P-CLHT/////////////////////////////
typedef struct thread_data {
    uint32_t id;
    clht_t *ht;
} thread_data_t;

typedef struct barrier {
    pthread_cond_t complete;
    pthread_mutex_t mutex;
    int count;
    int crossing;
} barrier_t;

void barrier_init(barrier_t *b, int n) {
    pthread_cond_init(&b->complete, NULL);
    pthread_mutex_init(&b->mutex, NULL);
    b->count = n;
    b->crossing = 0;
}

void barrier_cross(barrier_t *b) {
    pthread_mutex_lock(&b->mutex);
    b->crossing++;
    if (b->crossing < b->count) {
        pthread_cond_wait(&b->complete, &b->mutex);
    } else {
        pthread_cond_broadcast(&b->complete);
        b->crossing = 0;
    }
    pthread_mutex_unlock(&b->mutex);
}

barrier_t barrier;
/////////////////////////////////////////////////////////////////////////////////

static uint64_t LOAD_SIZE = 64000000;
static uint64_t RUN_SIZE = 64000000;

void loadKey(TID tid, Key &key) {
    return;
}

int (*which_memalign)(void **memptr, size_t alignment, size_t size);

void (*which_memfree)(void *ptr);

void *(*which_malloc)(size_t size);

void (*which_free)(void *ptr);

void ycsb_load_run_randint(int index_type, int wl, int kt, int ap, int num_thread,
                           std::vector<uint64_t> &init_keys,
                           std::vector<uint64_t> &keys,
                           std::vector<int> &ranges,
                           std::vector<int> &ops) {
    std::string init_file;
    std::string txn_file;

    if (ap == UNIFORM) {
        if (kt == RANDINT_KEY && wl == WORKLOAD_A) {
            init_file = "./index-microbench/workloads/loada_unif_int.dat";
            txn_file = "./index-microbench/workloads/txnsa_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_B) {
            init_file = "./index-microbench/workloads/loadb_unif_int.dat";
            txn_file = "./index-microbench/workloads/txnsb_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_C) {
            init_file = "./index-microbench/workloads/loadc_unif_int.dat";
            txn_file = "./index-microbench/workloads/txnsc_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_D) {
            init_file = "./index-microbench/workloads/loadd_unif_int.dat";
            txn_file = "./index-microbench/workloads/txnsd_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_E) {
            init_file = "./index-microbench/workloads/loade_unif_int.dat";
            txn_file = "./index-microbench/workloads/txnse_unif_int.dat";
        }
    } else {
        if (kt == RANDINT_KEY && wl == WORKLOAD_A) {
            init_file = "./index-microbench/workloads/loada_unif_int.dat";
            txn_file = "./index-microbench/workloads/txnsa_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_B) {
            init_file = "./index-microbench/workloads/loadb_unif_int.dat";
            txn_file = "./index-microbench/workloads/txnsb_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_C) {
            init_file = "./index-microbench/workloads/loadc_unif_int.dat";
            txn_file = "./index-microbench/workloads/txnsc_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_D) {
            init_file = "./index-microbench/workloads/loadd_unif_int.dat";
            txn_file = "./index-microbench/workloads/txnsd_unif_int.dat";
        } else if (kt == RANDINT_KEY && wl == WORKLOAD_E) {
            init_file = "./index-microbench/workloads/loade_unif_int.dat";
            txn_file = "./index-microbench/workloads/txnse_unif_int.dat";
        }
    }

    std::ifstream infile_load(init_file);

    std::string op;
    uint64_t key;
    int range;

    std::string insert("INSERT");
    std::string update("UPDATE");
    std::string read("READ");
    std::string scan("SCAN");

    int count = 0;
    while ((count < LOAD_SIZE) && infile_load.good()) {
        infile_load >> op >> key;
        if (op.compare(insert) != 0) {
            std::cout << "READING LOAD FILE FAIL!\n";
            return;
        }
        init_keys.push_back(key);
        count++;
    }

    fprintf(stderr, "Loaded %d keys\n", count);

    std::ifstream infile_txn(txn_file);

    count = 0;
    while ((count < RUN_SIZE) && infile_txn.good()) {
        infile_txn >> op >> key;
        if (op.compare(insert) == 0) {
            ops.push_back(OP_INSERT);
            keys.push_back(key);
            ranges.push_back(1);
        } else if (op.compare(update) == 0) {
            ops.push_back(OP_UPDATE);
            keys.push_back(key);
            ranges.push_back(1);
        } else if (op.compare(read) == 0) {
            ops.push_back(OP_READ);
            keys.push_back(key);
            ranges.push_back(1);
        } else if (op.compare(scan) == 0) {
            infile_txn >> range;
            ops.push_back(OP_SCAN);
            keys.push_back(key);
            ranges.push_back(range);
        } else {
            std::cout << "UNRECOGNIZED CMD!\n";
            return;
        }
        count++;
    }

    std::atomic<int> range_complete, range_incomplete;
    range_complete.store(0);
    range_incomplete.store(0);

    fprintf(stderr, "ready\n");

    if (index_type == TYPE_MASSTREE) {
        masstree::masstree *tree = new masstree::masstree();

        {
            // Load
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, LOAD_SIZE),
                              [&](const tbb::blocked_range<uint64_t> &scope) {
                                  auto t = tree->getThreadInfo();
                                  for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                                      tree->put(init_keys[i], &init_keys[i], t);
                                  }
                              });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: load, %f ,ops/us\n", (LOAD_SIZE * 1.0) / duration.count());
        }

        {
            // Run
            auto starttime = std::chrono::system_clock::now();
            tbb::parallel_for(tbb::blocked_range<uint64_t>(0, RUN_SIZE),
                              [&](const tbb::blocked_range<uint64_t> &scope) {
                                  auto t = tree->getThreadInfo();
                                  for (uint64_t i = scope.begin(); i != scope.end(); i++) {
                                      if (ops[i] == OP_INSERT) {
                                          tree->put(keys[i], &keys[i], t);
                                      } else if (ops[i] == OP_READ) {
                                          uint64_t *ret = reinterpret_cast<uint64_t *> (tree->get(keys[i], t));
                                          if (ap == UNIFORM && *ret != keys[i]) {
                                              printf("[MASS] search key = %lu, search value = %lu\n", keys[i], *ret);
                                              exit(1);
                                          }
                                      } else if (ops[i] == OP_SCAN) {
                                          uint64_t buf[200];
                                          int ret = tree->scan(keys[i], ranges[i], buf, t);
                                      } else if (ops[i] == OP_DELETE) {
                                          tree->del(keys[i], t);
                                      } else if (ops[i] == OP_UPDATE) {
                                          tree->put(keys[i], &keys[i], t);
                                      }
                                  }
                              });
            auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                    std::chrono::system_clock::now() - starttime);
            printf("Throughput: run, %f ,ops/us\n", (RUN_SIZE * 1.0) / duration.count());
        }
    }

}

int main(int argc, char **argv) {
    if (argc != 6) {
        std::cout
                << "Usage: ./ycsb [index type] [ycsb workload type] [key distribution] [access pattern] [number of threads]\n";
        std::cout << "1. index type: masstree\n";
        std::cout << "2. ycsb workload type: a, b, c, e\n";
        std::cout << "3. key distribution: randint, string\n";
        std::cout << "4. access pattern: uniform, zipfian\n";
        std::cout << "5. number of threads (integer)\n";
        return 1;
    }

    printf("%s, workload%s, %s, %s, threads %s\n", argv[1], argv[2], argv[3], argv[4], argv[5]);

    which_memalign = posix_memalign;
    which_memfree = free;
    which_malloc = malloc;
    which_free = free;

    if (strcmp(argv[1], "masstree") != 0) {
        printf("only masstree is allowed!\n");
        return 1;
    }

    int index_type;
    if (strcmp(argv[1], "masstree") == 0)
        index_type = TYPE_MASSTREE;
    else {
        fprintf(stderr, "Unknown index type: %s\n", argv[1]);
        exit(1);
    }

    int wl;
    if (strcmp(argv[2], "a") == 0) {
        wl = WORKLOAD_A;
    } else if (strcmp(argv[2], "b") == 0) {
        wl = WORKLOAD_B;
    } else if (strcmp(argv[2], "c") == 0) {
        wl = WORKLOAD_C;
    } else if (strcmp(argv[2], "d") == 0) {
        wl = WORKLOAD_D;
    } else if (strcmp(argv[2], "e") == 0) {
        wl = WORKLOAD_E;
    } else {
        fprintf(stderr, "Unknown workload: %s\n", argv[2]);
        exit(1);
    }

    int kt;
    if (strcmp(argv[3], "randint") == 0) {
        kt = RANDINT_KEY;
    } else {
        fprintf(stderr, "Unknown key type: %s\n", argv[3]);
        exit(1);
    }

    int ap;
    if (strcmp(argv[4], "uniform") == 0) {
        ap = UNIFORM;
    } else if (strcmp(argv[4], "zipfian") == 0) {
        ap = ZIPFIAN;
    } else {
        fprintf(stderr, "Unknown access pattern: %s\n", argv[4]);
        exit(1);
    }

    int num_thread = atoi(argv[5]);
    tbb::task_scheduler_init init(num_thread);

    if (kt == RANDINT_KEY) {
        std::vector<uint64_t> init_keys;
        std::vector<uint64_t> keys;
        std::vector<int> ranges;
        std::vector<int> ops;

        init_keys.reserve(LOAD_SIZE);
        keys.reserve(RUN_SIZE);
        ranges.reserve(RUN_SIZE);
        ops.reserve(RUN_SIZE);

        memset(&init_keys[0], 0x00, LOAD_SIZE * sizeof(uint64_t));
        memset(&keys[0], 0x00, RUN_SIZE * sizeof(uint64_t));
        memset(&ranges[0], 0x00, RUN_SIZE * sizeof(int));
        memset(&ops[0], 0x00, RUN_SIZE * sizeof(int));

        ycsb_load_run_randint(index_type, wl, kt, ap, num_thread, init_keys, keys, ranges, ops);
    }

    return 0;
}
