#include "plog.h"

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
    size_t free_space;
    char *base;
    char *curr;
};

// metadata for each cell in a log
struct log_cell {
    size_t size;
};

int inited = 0;

// the absolute base address
char *inodes;
char *big_map;

// log map and lock to protect the map
struct log_map lm;
pthread_mutex_t lm_lock = PTHREAD_MUTEX_INITIALIZER;

// every thread hold its own log
char *log_meta;
__thread struct log *thread_log = NULL;

void log_init(const char *fn, uint64_t num_logs) {

    char buf[CACHE_LINE_SIZE];
    int fd;
    uint64_t file_size;


    if (sizeof(char) != 1) die("char size error: %lu", sizeof(char));

    //todo: what happens when recovering?

    sprintf(buf, "%s_inodes", fn);
    file_size = num_logs * CACHE_LINE_SIZE;
    fd = open(fn, O_RDWR | O_CREAT | O_EXCL, 00777);
    if (fd < 0)die("fd error: %d", fd);
    if (posix_fallocate(fd, 0, file_size)) die("fallocate error");
    inodes = (char *) mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (inodes == MAP_FAILED)die("map error");
    close(fd);


    sprintf(buf, "%s_logs", fn);
    file_size = num_logs * LOG_SIZE;
    fd = open(buf, O_RDWR | O_CREAT | O_EXCL, 00777);
    if (fd < 0)die("fd error: %d", fd);
    if (posix_fallocate(fd, 0, file_size)) die("fallocate error");
    big_map = (char *) mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (big_map == MAP_FAILED)die("map error");
    close(fd);

    // inodes
    lm.num_entries = num_logs;
    lm.entries = (int **) malloc(sizeof(int *) * lm.num_entries);
    for (uint64_t i = 0; i < lm.num_entries; i++) {
        lm.entries[i] = (int *) (inodes + CACHE_LINE_SIZE * i);
        lm.entries[i][0] = AVAILABLE;
    }

    // usage
    if (sizeof(struct log) > CACHE_LINE_SIZE) die("struct log size %lu too big", sizeof(struct log));
    log_meta = (char *) malloc(CACHE_LINE_SIZE * num_logs);

    inited = 1;
}

char *log_acquire(int write_thread_log) {

    char *log_address;

    if (!inited)die("not inited");
    log_address = NULL;

    //todo: this logic can be optimize to be similar to allocator
    pthread_mutex_lock(&lm_lock);
    for (uint64_t i = 0; i < lm.num_entries; i++) {
        if (lm.entries[i][0] == AVAILABLE) {
            lm.entries[i][0] = OCCUPIED;
            log_address = big_map + i * LOG_SIZE;

            if (write_thread_log) {
                thread_log = (struct log *) (log_meta + CACHE_LINE_SIZE * i);
                thread_log->free_space = LOG_SIZE;
                thread_log->base = log_address;
                thread_log->curr = thread_log->base;
            }
            goto end;
        }
    }
    end:
    pthread_mutex_unlock(&lm_lock);
    return log_address;

}

void *log_malloc(size_t size) {

    size_t required_size;

    required_size = size + sizeof(struct log_cell);

    if (unlikely(thread_log == NULL || thread_log->free_space < required_size)) {

        if (log_acquire(1) == NULL)die("cannot acquire new log");
    }

    // write and decrease size
    thread_log->free_space -= required_size;
    *((size_t *) thread_log->curr) = size;
    void *to_return = thread_log->curr + sizeof(size_t);

    thread_log->curr = thread_log->curr + required_size;

    return to_return;
}


int log_memalign(void **memptr, size_t alignment, size_t size) {

    size += ((size + sizeof(struct log_cell)) / alignment + 1) * alignment - size - sizeof(struct log_cell);

//    printf("size %lu\n",size+sizeof(struct log_cell));
    // todo: how to make sure memory is aligned
    *memptr = log_malloc(size);

    return 0;
}

void log_free(void *ptr) {


    // todo: how to mark entry as freed
    (void) ptr;
    return;
}

void *log_garbage_collection(void *arg) {

    masstree::masstree *tree;
    auto t = tree->getThreadInfo();
    char *new_log;

    // todo Question: do we need to lock the whole tree?

    tree = (masstree::masstree *) arg;
    new_log = log_acquire(0);

    // todo: how to properly store metadata
    for (int i = 0; i < 2; i++) {

        char *current_ptr;
        char *base_ptr;
        while (current_ptr < base_ptr + LOG_SIZE) {

            size_t size = *((size_t *) current_ptr);
            current_ptr += sizeof(size_t);

            // assume key is always 8 bytes and occupy the field following size
            uint64_t key = *((uint64_t *) current_ptr);
            void *value = current_ptr + sizeof(uint64_t);

            // this step might be buggy if went out of bound of the new log
            if (tree->get(key, t) == value) {
                *((size_t *) new_log) = size;
                new_log += sizeof(size_t);

                memcpy(new_log, current_ptr, size);
                new_log += size;
            }

            current_ptr += size;
        }
    }

    // todo: commit the new log


    return NULL;
}