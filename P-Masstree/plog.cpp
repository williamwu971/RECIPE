#include "plog.h"


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

struct garbage_queue {
    uint64_t indexes[7];
    uint64_t num = 0;
};

struct garbage_queue gq;
pthread_mutex_t gq_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t gq_cond = PTHREAD_COND_INITIALIZER;
int gc_stopped = 0;
pthread_t gc;


void log_structs_size_check() {

    // some structs are required to occupy a cache line
    if (sizeof(char) != 1) die("char size error: %lu", sizeof(char));
    if (sizeof(struct log) != CACHE_LINE_SIZE) die("struct log size %ld", sizeof(struct log));
    if (sizeof(struct garbage_queue) != CACHE_LINE_SIZE)
        die("struct garbage_queue size %ld", sizeof(struct garbage_queue));
//    if (sizeof(struct log_cell) == sizeof(uint64_t)) die("log cell size");

}

void log_init(const char *fn, uint64_t num_logs) {

    log_structs_size_check();

    char buf[CACHE_LINE_SIZE];
    int fd;
    uint64_t file_size;


    //todo: what happens when recovering?

    sprintf(buf, "%s_inodes", fn);
    file_size = num_logs * CACHE_LINE_SIZE;
    fd = open(buf, O_RDWR | O_CREAT | O_EXCL, 00777);
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

                // retire and mark the old log for collection
                if (thread_log != NULL) {
                    thread_log->full = 1;
                }

                thread_log = (struct log *) (log_meta + CACHE_LINE_SIZE * i);
                thread_log->freed = 0;
                thread_log->available = LOG_SIZE;
                thread_log->index = i;
                thread_log->full = 0;
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

void log_release(uint64_t idx) {
    pthread_mutex_lock(&lm_lock);

    lm.entries[idx][0] = AVAILABLE;

    pthread_mutex_unlock(&lm_lock);
}

uint64_t log_get_key_and_value_size_from_value_ptr(char *value_ptr) {
    uint64_t *size_ptr = (uint64_t *) (value_ptr - sizeof(uint64_t) * 2);
    return *size_ptr;
}

uint64_t log_get_key_from_value_ptr(char *value_ptr) {
    uint64_t *key = (uint64_t *) (value_ptr - sizeof(uint64_t));
    return *key;
}

void *log_malloc(size_t size) {

    uint64_t required_size = size + sizeof(uint64_t);

    // the "freed" space should be strictly increasing
    if (unlikely(thread_log == NULL || thread_log->available < required_size)) {
        if (log_acquire(1) == NULL)die("cannot acquire new log");
    }

    // write and decrease size
    thread_log->available -= required_size;


    *((uint64_t *) thread_log->curr) = size;
    void *to_return = thread_log->curr + sizeof(uint64_t);

    thread_log->curr = thread_log->curr + required_size;

    return to_return;
}


int log_memalign(void **memptr, size_t alignment, size_t size) {

    size += ((size + sizeof(uint64_t)) / alignment + 1) * alignment - size - sizeof(uint64_t);

//    printf("size %lu\n",size+sizeof(struct log_cell));
    // todo: how to make sure memory is aligned
    *memptr = log_malloc(size);

    return 0;
}

void log_free(void *ptr) {

    // todo: how to mark entry as freed
    char *char_ptr = (char *) ptr;
    uint64_t idx = (uint64_t) (char_ptr - big_map) / LOG_SIZE;

    struct log *target_log = (struct log *) (log_meta + CACHE_LINE_SIZE * idx);

    uint64_t new_space = *((uint64_t *) (char_ptr - sizeof(uint64_t))) + sizeof(uint64_t);
    target_log->freed += new_space;

    if (target_log->full && target_log->freed >= LOG_MERGE_THRESHOLD) {

        while (1) {
            pthread_mutex_lock(&gq_lock);

            //abort if the queue is already full
            if (gq.num == GAR_QUEUE_LENGTH) {
                pthread_mutex_unlock(&gq_lock);
                continue;
            }

            // if the log is already in the queue, don't add it
            for (uint64_t n = 0; n < gq.num; n++) {
                if (gq.indexes[n] == idx) {
                    goto end;
                }
            }

            gq.indexes[gq.num++] = idx;

            // wake up garbage collection thread
            if (gq.num == GAR_QUEUE_LENGTH) {
                pthread_cond_signal(&gq_cond);
            } else if (gq.num > GAR_QUEUE_LENGTH) {
                die("gq length:%lu error", gq.num);
            }
            end:
            pthread_mutex_unlock(&gq_lock);
            break;
        }
    }

}

void *log_garbage_collection(void *arg) {

    // todo Question: do we need to lock the whole tree?
    masstree::masstree *tree = (masstree::masstree *) arg;
    auto t = tree->getThreadInfo();


    while (!gc_stopped) {

        // wait for other threads to wait me up
        pthread_mutex_lock(&gq_lock);
        pthread_cond_wait(&gq_cond, &gq_lock);

        if (gq.num != GAR_QUEUE_LENGTH) die("gc detected gq length:%lu", gq.num);

        // acquire a new log
        if (log_acquire(1) == NULL)die("cannot acquire new log");

        printf("merge ");

        // todo: how to properly store metadata
        for (int i = 0; i < GAR_QUEUE_LENGTH; i++) {

            struct log *target_log = (struct log *) (log_meta + CACHE_LINE_SIZE * gq.indexes[i]);
            char *current_ptr = target_log->base;
            char *end_ptr = target_log->curr;

            printf("%lu ", gq.indexes[i]);


            while (current_ptr < end_ptr) {

                // read and advance the pointer
                uint64_t size = *((uint64_t *) current_ptr);
                current_ptr += sizeof(uint64_t);

                // todo: assume key is always 8 bytes and occupy the field following size
                uint64_t key = *((uint64_t *) current_ptr);
                void *value = current_ptr + sizeof(uint64_t);

                // this step might be buggy if went out of bound of the new log
                if (value != NULL && tree->get(key, t) == value) {
                    *((uint64_t *) thread_log->curr) = size;
                    thread_log->curr += sizeof(uint64_t);

                    // thread log curr now points to key
                    memcpy(thread_log->curr, current_ptr, size);


                    // try to commit this entry
                    int res = tree->put_if_match(key, value, thread_log->curr + sizeof(uint64_t), t);

                    // the log acquired by gc thread shouldn't need atomic ops
                    if (res) {
                        thread_log->available -= (sizeof(uint64_t) + size);
                        thread_log->curr += size;
                    } else {
                        thread_log->curr -= sizeof(uint64_t);
                    }
                }

                current_ptr += size;
            }


            log_release(gq.indexes[i]);

        }

        printf("to %lu\n", thread_log->index);
        if (thread_log->curr > thread_log->base + LOG_SIZE)
            die("log overflow detected used:%ld", thread_log->curr - thread_log->base);

        gq.num = 0;
        pthread_mutex_unlock(&gq_lock);

    }

    return NULL;
}

void log_start_gc(masstree::masstree *t) {
    pthread_create(&gc, NULL, log_garbage_collection, t);
    pthread_detach(gc);
}

void log_end_gc() {
    gc_stopped = 1;
    pthread_cancel(gc);
}

void log_debug_print(uint64_t bound) {
    for (uint64_t i = 0; i < bound; i++) {
//        printf("log %lu free %lu\n", i, ((struct log *) log_meta + CACHE_LINE_SIZE * i)->free_space);
    }
}