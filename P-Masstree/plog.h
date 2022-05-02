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

#define LOG_SIZE 4*1024*1024
#define LOG_MERGE_THRESHOLD 2*1024*1024
#define CACHE_LINE_SIZE 64

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

#define die(msg, args...) \
   do {                         \
      fprintf(stderr,"(%s,%d) " msg "\n", __FUNCTION__ , __LINE__, ##args); \
      fflush(stdout); \
      exit(-1); \
   } while(0)

void log_init(const char *fn, uint64_t num_logs);

void *log_malloc(size_t size);

void log_free(void *ptr);

#endif