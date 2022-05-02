#include "plog.h"
#include <iostream>
#include <chrono>
#include <random>
#include "tbb/tbb.h"

enum {
    AVAILABLE,
    OCCUPIED,
};

// occupy the first 4m of log file
struct log_map {
    int num_entries;

    // aligned to CACHELINE size
    int **entries;
};

// metadata for the current log, should be in DRAM
struct log {
    size_t free_space;
    void *base;
    void *curr;
};

// metadata for each cell in a log
struct log_cell {
    size_t size;
};

int inited = 0;

// the absolute base address
void *big_map;

// log map and lock to protect the map
struct log_map lm;
pthread_mutex_t lm_lock = PTHREAD_MUTEX_INITIALIZER;

// every thread hold its own log
__thread struct log *thread_log = NULL;

void log_init(const char *fn, int num_logs) {

    assert(size >= 2 * LOG_SIZE);
    size_t file_size = (num_logs+1)*LOG_SIZE;

    int fd = open(fn, O_RDWR | O_CREAT | O_EXCL, 00777);
    if(fd < 0)die("fd error: %d",fd);
    if (posix_fallocate(fd, 0, file_size)) die("fallocate error");
    printf("file size %zu\n",file_size);

    big_map = mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if(big_map == MAP_FAILED)die("map error");

    lm.num_entries = num_logs;
    lm.entries = (int **) malloc(sizeof(int *) * lm.num_entries);
    for (int i = 0; i < lm.num_entries; i++) {
        lm.entries[i] = (int *) ((char *) big_map + CACHE_LINE_SIZE * i);
        lm.entries[i][0] = AVAILABLE;
    }

    inited = 1;
}

int log_new() {

    assert(inited);

    if (thread_log == NULL) {
        posix_memalign((void **) &thread_log, CACHE_LINE_SIZE, sizeof(struct log));
    }
    int success = 0;
    pthread_mutex_lock(&lm_lock);
    for (int i = 0; i < lm.num_entries; i++) {
        if (lm.entries[i][0] == AVAILABLE) {
            lm.entries[i][0] = OCCUPIED;
            success = 1;

            thread_log->free_space = LOG_SIZE;
            thread_log->base = (char *) big_map + i * LOG_SIZE;
            thread_log->curr = thread_log->base;

            goto end;
        }
    }

    end:
    pthread_mutex_unlock(&lm_lock);
    return success;
}

void *log_malloc(size_t size) {

    size_t required_size = size + sizeof(struct log_cell);

    if (unlikely(thread_log == NULL || thread_log->free_space < required_size)) {
        if (!log_new()) {
            return NULL;
        }
    }

    // write and decrease size
    thread_log->free_space -= required_size;
    *((size_t *) thread_log->curr) = size;
    void *to_return = (char *) thread_log->curr + sizeof(size_t);

    thread_log->curr = (char *) thread_log->curr + required_size;

    return to_return;
}


int log_memalign(void **memptr, size_t alignment, size_t size) {

    (void) alignment;

    // todo: how to make sure memory is aligned
    *memptr = log_malloc(size);

    return 0;
}

void log_free(void *ptr) {


    // todo: how to mark entry as freed
    (void) ptr;
    return;
}