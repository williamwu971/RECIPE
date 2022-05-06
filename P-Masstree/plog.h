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
//#include <atomic>

#define LOG_SIZE (4*1024*1024)
#define LOG_MERGE_THRESHOLD (2*1024*1024)
#define CACHE_LINE_SIZE 64
#define GAR_QUEUE_LENGTH 2

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

#define die(msg, args...) \
   do {                         \
      fprintf(stderr,"(%s,%d,%s) " msg "\n", __FUNCTION__ , __LINE__,strerror(errno), ##args); \
      fflush(stderr);fflush(stdout); \
      exit(-1); \
   } while(0)

enum {
    AVAILABLE,
    OCCUPIED,
};

// occupy the first 4m of log file
struct log_map {
    uint64_t num_entries;

    // aligned to CACHELINE size
    int **entries;
};

// metadata for the current log, should be in DRAM
struct log {
//    std::atomic<size_t> free_space;
    uint64_t freed;
    uint64_t available;
    uint64_t index;
    uint64_t full;
    char *base;
    char *curr;
    char padding[24];
};

// metadata for each cell in a log
//struct log_cell {
//    uint64_t size;
//};

void log_init(const char *fn, uint64_t num_logs);

void *log_malloc(size_t size);

void log_free(void *ptr);

void log_start_gc(masstree::masstree *);

void log_debug_print();

#endif