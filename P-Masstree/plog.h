#ifndef PLOG_H_
#define PLOG_H_


#include <stdlib.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <assert.h>
#include <pthread.h>

#define LOG_SIZE 4*1024*1024
#define LOG_MERGE_THRESHOLD 2*1024*1024
#define CACHE_LINE_SIZE 64

#define likely(x)       __builtin_expect((x),1)
#define unlikely(x)     __builtin_expect((x),0)

void log_init(char *fn, off_t size);

void *log_malloc(size_t size);

#endif