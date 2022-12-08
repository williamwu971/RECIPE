#ifndef PLOG_H_
#define PLOG_H_


#include <cstdlib>
#include <fcntl.h>
#include <sys/mman.h>
#include <cassert>
#include <pthread.h>
#include <iostream>
#include <chrono>
#include <random>
//#include "tbb/tbb.h"
#include "masstree.h"
#include <cerrno>
#include <cstring>
#include <cstddef>
#include <cstdint>
//#include <stdatomic.h>
#include <atomic>
#include <libpmem.h>
#include <omp.h>
#include <unistd.h>
#include <libpmemobj.h>
#include <semaphore.h>

#define LOG_SIZE (4*1024*1024ULL)
#define LOG_MERGE_THRESHOLD (2*1024*1024ULL)
#define CACHE_LINE_SIZE (64)
#define GAR_QUEUE_LENGTH (2)
#define OMP_NUM_THREAD (23)
//#define PAGE_SIZE (4096)
#define NUM_LOG_PER_COLLECTION (10)
#define MASSTREE_FLUSH

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

#define die(msg, args...) \
   do {                         \
      fprintf(stderr,"(%s,%d,%s) " msg "\n", __FUNCTION__ , __LINE__,strerror(errno), ##args); \
      fflush(stderr);fflush(stdout); \
      exit(-1); \
   } while(0)


//asm volatile("sfence":::"memory");

#ifdef __x86_64__
#define rdtscll(val) { \
       unsigned int __a,__d;                                        \
       asm volatile("rdtsc" : "=a" (__a), "=d" (__d));              \
       (val) = ((unsigned long)__a) | (((unsigned long)__d)<<32);   \
 }
#define rdtscll_fence(val) { \
        asm volatile("sfence":::"memory");                \
       unsigned int __a,__d;                                        \
       asm volatile("rdtsc" : "=a" (__a), "=d" (__d));              \
       (val) = ((unsigned long)__a) | (((unsigned long)__d)<<32);   \
 asm volatile("sfence":::"memory");}
#else
#define rdtscll(val) __asm__ __volatile__("rdtsc" : "=A" (val))
#endif


#include <x86intrin.h>

inline
uint64_t readTSC(int front, int back) {
    if (front)_mm_lfence();  // optionally wait for earlier insns to retire before reading the clock
    uint64_t tsc = __rdtsc();
    if (back)_mm_lfence();  // optionally block later instructions until rdtsc retires
    return tsc;
}


#define declearTSC uint64_t TSCa;

#define startTSC TSCa=readTSC(1,1);

#define stopTSC(target) target+=(readTSC(1,1)-TSCa);

#define INODE_FN "/pmem0/masstree_log_inodes"
#define LOG_FN "/pmem0/masstree_log_logs"
#define META_FN "/pmem0/masstree_log_metas"

void *(*cpy_persist)(void *, const void *, size_t) =pmem_memcpy_persist;

//enum {
//    AVAILABLE,
//    OCCUPIED,
//};

enum {
    SUM_INVALID,
    SUM_WRITE,
    SUM_CHECK,
    SUM_LOG
};

static inline uint64_t *masstree_checksum(void *value,
                                          int check,
                                          uint64_t v,
                                          uint64_t iteration,
                                          uint64_t offset) {

    auto numbers = (uint64_t *) value;
    auto check_result = (uint64_t *) 1;

    if (check == SUM_INVALID) {
        numbers += iteration;

        numbers[0] = 0;
        return numbers;
    }

    uint64_t sum = 0;


    for (uint64_t i = 0; i < iteration; i++) {
        sum += numbers[0];
        if (check == SUM_CHECK && i == offset && numbers[0] != v) {
            check_result = nullptr;
            printf("value incorrect, offset %lu expecting %lu got %lu\n", offset, v, numbers[0]);
        }
        numbers++;
    }

    if (check == SUM_CHECK) {
        if (numbers[0] != sum || sum == 0) {
            check_result = nullptr;
            printf("sum incorrect, expecting %lu got %lu\n", sum, numbers[0]);
        }
    } else if (check == SUM_LOG) {
        if (numbers[0] != sum || sum == 0) {
            check_result = nullptr;
        }
    } else if (check == SUM_WRITE) {
        numbers[0] = sum;
    }


    return check_result;
}

// occupy the first 4m of log file
struct log_list_pack {

    int occupied;
    int num_log;
    int next;
    int *list;
    pthread_mutex_t list_lock;
};

// metadata for the current log, should be in DRAM
struct log {
    std::atomic<uint64_t> freed;
    uint64_t available;
    uint64_t index;
    std::atomic<uint64_t> full;
    char *base;
    char *curr;
    char padding[16];
};

struct garbage_queue_node {
    uint64_t index;
    struct garbage_queue_node *next;
};

struct garbage_queue {
    pthread_mutex_t lock;
    pthread_cond_t cond;
    struct garbage_queue_node *head;
    uint64_t num;

    int gc_stopped;
    pthread_t *gc_ids;
    int num_gcs;
};

// metadata for each cell in a log
struct log_cell {

    uint32_t value_size;
    uint32_t is_delete;
    uint64_t version;
    uint64_t key;
};

void log_recover(masstree::masstree *tree, int num_threads);

void log_init(uint64_t pool_size);

void *log_malloc(size_t size);

void log_free(void *ptr);

void log_start_gc(masstree::masstree *, int start_cpu, int end_cpu);

void log_wait_all_gc();

void log_join_all_gc();

void log_debug_print(FILE *f, int using_log);

int log_start_perf(const char *perf_fn);

int log_stop_perf();

void log_print_pmem_bandwidth(const char *perf_fn, double elapsed, FILE *f);

void *log_get_tombstone(uint64_t key);

void *log_garbage_collection(void *arg);

#endif