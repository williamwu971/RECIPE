#ifndef PLOG_H_
#define PLOG_H_


#include <stdlib.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <assert.h>
#include <pthread.h>
#include <iostream>
#include <chrono>
#include <random>
#include "tbb/tbb.h"
#include "masstree.h"
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <cstdint>
//#include <stdatomic.h>
#include <atomic>
#include <libpmem.h>
#include <omp.h>
#include <unistd.h>
#include <libpmemobj.h>

#define LOG_SIZE (4*1024*1024ULL)
#define LOG_MERGE_THRESHOLD (2*1024*1024ULL)
#define CACHE_LINE_SIZE (64)
#define GAR_QUEUE_LENGTH (2)
#define OMP_NUM_THREAD (23)
#define PAGE_SIZE (4096)
#define NUM_LOG_PER_COLLECTION (10)
#define MASSTREE_FLUSH
//#define NO_PERSIST

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

#define INODE_FN "/pmem0/masstree_log_inodes"
#define LOG_FN "/pmem0/masstree_log_logs"
#define META_FN "/pmem0/masstree_log_metas"

//enum {
//    AVAILABLE,
//    OCCUPIED,
//};

// occupy the first 4m of log file
struct log_map {
    uint64_t num_entries;

    int next_available;
    int used;

    // aligned to CACHELINE size
    int **entries;
};

// metadata for the current log, should be in DRAM
struct log {
//    std::atomic<size_t> free_space;
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
};

// metadata for each cell in a log
struct log_cell {
    uint64_t value_size;
    uint64_t is_delete;
    uint64_t version;
//    uint64_t reference;
    uint64_t key;
};

void log_recover(masstree::masstree *tree, int num_threads);

void log_init(uint64_t pool_size, int pre_fault_threads);

void *log_malloc(size_t size);

void log_free(void *ptr);

void log_start_gc(masstree::masstree *);

void log_join_all_gc();

void log_debug_print(int to_file, int show);

int log_start_perf(const char *perf_fn);

int log_stop_perf();

void log_print_pmem_bandwidth(const char *perf_fn, double elapsed, FILE *f);

void *log_get_tombstone(uint64_t key);

#endif