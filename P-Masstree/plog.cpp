#include "plog.h"


int inited = 0;

// the absolute base address
char *inodes;
char *big_map;

// log map and lock to protect the map
struct log_map lm;
int OCCUPIED;
pthread_mutex_t lm_lock = PTHREAD_MUTEX_INITIALIZER;

// every thread hold its own log
char *log_meta;
__thread struct log *thread_log = NULL;


struct garbage_queue gq;
int gc_stopped = 0;
pthread_t *gc_ids = NULL;
int num_gcs;

uint64_t perf_start_rtd;
uint64_t perf_stop_rtd;
int perf_stat = 1;

void log_structs_size_check() {

    // some structs are required to occupy a cache line
    if (sizeof(char) != 1) die("char size error: %lu", sizeof(char));
    if (sizeof(struct log) != CACHE_LINE_SIZE) die("struct log size %ld", sizeof(struct log));
//    if (sizeof(struct garbage_queue) != CACHE_LINE_SIZE)
//        die("struct garbage_queue size %ld", sizeof(struct garbage_queue));
//    if (sizeof(struct log_cell) == sizeof(uint64_t)) die("log cell size");

}


void log_tree_rebuild(masstree::masstree *tree, int num_threads) {

    int old_num_threads = omp_get_num_threads();
    omp_set_num_threads(num_threads);

    printf("... rebuilding tree using %d omp threads ...\n", num_threads);

    for (uint64_t i = 0; i < lm.num_entries; i++) {
        struct log *target_log = (struct log *) (log_meta + CACHE_LINE_SIZE * i);
        target_log->freed.store(0);
        target_log->available = LOG_SIZE;
    }

    // process inserts first todo: OCCUPIED ENUM IS WRONG NOW
#pragma omp parallel for schedule(dynamic, 1)
    for (uint64_t i = 0; i < lm.num_entries; i++) {

        // todo: not sure if this has overhead
        auto t = tree->getThreadInfo();

//        if (lm.entries[i][0] == OCCUPIED) {

        char *end = big_map + (i + 1) * LOG_SIZE;
        char *curr = big_map + i * LOG_SIZE;
        struct log *current_log = (struct log *) (log_meta + CACHE_LINE_SIZE * i);

        while (curr < end) {

            struct log_cell *lc = (struct log_cell *) curr;

            // if field of the struct is zero, then abort this one
            if (lc->version == 0) break;

            uint64_t total_size = sizeof(struct log_cell) + lc->value_size;
            current_log->available -= total_size;

            if (!lc->is_delete) {

                struct log_cell *res = (struct log_cell *)
                        tree->put_and_return(lc->key, lc, 1, t);

                // insert success and created a new key-value
                // replaced a value, should free some space in other log
                if (res != NULL && res != lc) {

                    uint64_t idx = (uint64_t) ((char *) res - big_map) / LOG_SIZE;
                    struct log *target_log = (struct log *) (log_meta + CACHE_LINE_SIZE * idx);
                    target_log->freed.fetch_add(sizeof(struct log_cell) + res->value_size);
                }

            } else {
                tree->del_and_return(lc->key, 1, lc->version, t);
            }

            // todo: should probably update the metadata here
            curr += sizeof(struct log_cell) + lc->value_size;
        }
//        }
    }

    // sequentially reconstruct metadata
    // from end to beginning
    puts("rebuilding metadata");

    lm.used = lm.num_entries;
    lm.next_available = -1;

    for (uint64_t i = lm.num_entries - 1;; i--) {
        struct log *target_log = (struct log *) (log_meta + CACHE_LINE_SIZE * i);

        if (target_log->available == 0 ||
            target_log->freed.load() == LOG_SIZE - target_log->available) {

            if (lm.used == i + 1) {
                lm.used--;
            } else {
                lm.entries[i][0] = lm.next_available;
                lm.next_available = i;
            }
        }
        if (i == 0) break;
    }

    omp_set_num_threads(old_num_threads);
}

uint64_t log_map(int use_pmem, const char *fn, uint64_t file_size, void **result) {

    void *map;

    if (use_pmem) {

        int is_pmem;
        size_t mapped_len;

        if (file_size == 0) {
            map = pmem_map_file(fn, 0, 0, 0, &mapped_len, &is_pmem);
        } else {
            map = pmem_map_file(fn, file_size,
                                PMEM_FILE_CREATE | PMEM_FILE_EXCL, 00666,
                                &mapped_len, &is_pmem);
            if (mapped_len != file_size) die("map error mapped_len:%zu", mapped_len);
        }
        is_pmem = is_pmem && pmem_is_pmem(map, mapped_len);

        if (map == NULL || !is_pmem)
            die("map error map:%p is_pmem:%d", map, is_pmem);
        *result = map;

        return mapped_len;

    } else {

        map = mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_ANONYMOUS, -1, 0);
        if (map == MAP_FAILED) die("map error map:%p", map);

        return file_size;
    }
}

int log_recover(masstree::masstree *tree, int num_threads) {
    log_structs_size_check();

    uint64_t mapped_len;

    mapped_len = log_map(1, INODE_FN, 0, (void **) &inodes);
    if (mapped_len % CACHE_LINE_SIZE != 0) die("inodes mapped_len:%zu", mapped_len);


    uint64_t num_logs = mapped_len / CACHE_LINE_SIZE;

    mapped_len = log_map(1, LOG_FN, 0, (void **) &big_map);
    if (mapped_len != num_logs * LOG_SIZE) die("big_map mapped_len:%zu", mapped_len);


    // inodes
    lm.num_entries = num_logs;
    lm.entries = (int **) malloc(sizeof(int *) * lm.num_entries);
    for (uint64_t i = 0; i < lm.num_entries; i++) {
        lm.entries[i] = (int *) (inodes + CACHE_LINE_SIZE * i);
    }
    OCCUPIED = num_logs + 1;

    // usage
    log_meta = (char *) malloc(CACHE_LINE_SIZE * num_logs);

    // reconstruct tree
    log_tree_rebuild(tree, num_threads);

    // gc
    pthread_mutex_init(&gq.lock, NULL);
    pthread_cond_init(&gq.cond, NULL);
    gq.head = NULL;
    gq.num = 0;

    inited = 1;
}

void log_init(uint64_t num_logs) {

    log_structs_size_check();

    uint64_t file_size;

    file_size = num_logs * CACHE_LINE_SIZE;

    log_map(1, INODE_FN, file_size, (void **) inodes);
    pmem_memset_persist(inodes, 0, file_size);

    log_map(1, META_FN, file_size, (void **) log_meta);
    pmem_memset_persist(log_meta, 0, file_size);

    file_size = num_logs * LOG_SIZE;
    log_map(1, LOG_FN, file_size, (void **) big_map);

    omp_set_num_threads(23);
#pragma omp parallel for schedule(dynamic, 10)
    for (uint64_t fs = 0; fs < file_size; fs += (4 * 1024ULL)) {
//        pmem_memset_persist(big_map + fs, 0, (4 * 1024ULL));
    }


    // inodes
    lm.num_entries = num_logs;
    lm.entries = (int **) malloc(sizeof(int *) * lm.num_entries);
    OCCUPIED = num_logs + 1;
    for (uint64_t i = 0; i < lm.num_entries; i++) {
        lm.entries[i] = (int *) (inodes + CACHE_LINE_SIZE * i);
    }
    lm.next_available = -1;
    lm.used = 0;

    // usage
//    log_meta = (char *) malloc(CACHE_LINE_SIZE * num_logs);


    // gc
    pthread_mutex_init(&gq.lock, NULL);
    pthread_cond_init(&gq.cond, NULL);
    gq.head = NULL;
    gq.num = 0;

    inited = 1;
}

void log_gq_add(uint64_t idx) {
    struct garbage_queue_node *node = (struct garbage_queue_node *) malloc(sizeof(struct garbage_queue_node));
    node->index = idx;
    node->next = NULL;

    pthread_mutex_lock(&gq.lock);

    // add the node to the queue
    if (gq.head != NULL) { node->next = gq.head; }
    gq.head = node;
    gq.num++;


    // wake up ONE garbage collector if queue is long enough
    if (gq.num >= GAR_QUEUE_LENGTH) {
        pthread_cond_signal(&gq.cond);
    }

    pthread_mutex_unlock(&gq.lock);
}

char *log_acquire(int write_thread_log) {

    char *log_address;

    if (!inited)die("not inited");
    log_address = NULL;
    uint64_t i;

    //todo: this logic can be optimize to be similar to allocator
    pthread_mutex_lock(&lm_lock);


    if (lm.next_available == -1) {
        i = lm.used;
        lm.used++;
    } else {
        i = lm.next_available;
        lm.next_available = lm.entries[i][0];
    }

    if (i >= lm.num_entries) die("all logs are occupied");

    lm.entries[i][0] = OCCUPIED;
//    pmem_persist(lm.entries[i], sizeof(int));

    log_address = big_map + i * LOG_SIZE;
    if (write_thread_log) {

        // retire and mark the old log for collection
        if (thread_log != NULL) {

            if (thread_log->freed.load() > LOG_MERGE_THRESHOLD) {
                log_gq_add(thread_log->index);
            } else {
                thread_log->full.store(1);
            }
        }

        thread_log = (struct log *) (log_meta + CACHE_LINE_SIZE * i);
        thread_log->freed.store(0);
        thread_log->available = LOG_SIZE;
        thread_log->index = i;
        thread_log->full.store(0);
        thread_log->base = log_address;
        thread_log->curr = thread_log->base;

//        pmem_persist(thread_log, CACHE_LINE_SIZE);
    }
    pthread_mutex_unlock(&lm_lock);
    return log_address;

}

void log_release(uint64_t idx) {
    pthread_mutex_lock(&lm_lock);

    // todo: this persist is possibly unreliable (for recovery purpose)
//    pmem_memset_persist(big_map + idx * LOG_SIZE, 0, LOG_SIZE);

//    lm.entries[idx][0] = AVAILABLE;


    lm.entries[idx][0] = lm.next_available;
    lm.next_available = idx;

//    pmem_persist(lm.entries[idx], sizeof(int));


    pthread_mutex_unlock(&lm_lock);
}

void *log_malloc(size_t size) {

//    uint64_t required_size = sizeof(struct log_cell) + size;
    if (unlikely(size < sizeof(struct log_cell))) die("size too small %zu", size);

    // the "freed" space should be strictly increasing
    if (unlikely(thread_log == NULL || thread_log->available < size)) {
        if (log_acquire(1) == NULL)die("cannot acquire new log");
    }

    // write and decrease size
    thread_log->available -= size;

//    struct log_cell *lc = (struct log_cell *) thread_log->curr;
//    lc->value_size = size - sizeof(struct log_cell);
//    rdtscll(lc->version);

    thread_log->curr += size;
//    pmem_persist(thread_log,sizeof(struct log));

    return thread_log->curr - size;
}


int log_memalign(void **memptr, size_t alignment, size_t size) {

    size += ((size + sizeof(uint64_t)) / alignment + 1) * alignment - size - sizeof(uint64_t);

//    printf("size %lu\n",size+sizeof(struct log_cell));
    // todo: how to make sure memory is aligned
    *memptr = log_malloc(size);

    return 0;
}

void log_free(void *ptr) {

    char *char_ptr = (char *) ptr;

    // commit a dummy log to represent that this entry has been freed
    struct log_cell *lc = (struct log_cell *) ptr;
//    struct log_cell *tombstone = (struct log_cell *) log_malloc(sizeof(struct log_cell));
//
//    tombstone->is_delete = 1;
//    tombstone->key = lc->key;
//    pmem_persist(tombstone, sizeof(struct log_cell)); // PERSIST


    // locate the log and its metadata
    uint64_t idx = (uint64_t) (char_ptr - big_map) / LOG_SIZE;
    struct log *target_log = (struct log *) (log_meta + CACHE_LINE_SIZE * idx);

    // update metadata and add the log to GC queue if suitable
    uint64_t freed = target_log->freed.fetch_add(sizeof(struct log_cell) + lc->value_size);
    uint64_t can_collect = target_log->full.load();

    if (can_collect && freed >= LOG_MERGE_THRESHOLD &&
        target_log->full.compare_exchange_strong(can_collect, 0)) {

        log_gq_add(idx);
    }

}

//#define GC_DEBUG_OUTPUT

void *log_garbage_collection(void *arg) {

    // todo Question: do we need to lock the whole tree?
    masstree::masstree *tree = (masstree::masstree *) arg;
    auto t = tree->getThreadInfo();


    while (!gc_stopped) {

        // wait for other threads to wait me up
        pthread_mutex_lock(&gq.lock);
        pthread_cond_wait(&gq.cond, &gq.lock);

        // gc takes the entire queue and release the lock instantly
        struct garbage_queue_node *queue = gq.head;
//        uint64_t queue_length = gq.num;

        gq.head = NULL;
        gq.num = 0;

        pthread_mutex_unlock(&gq.lock);

//        if (gq.num != GAR_QUEUE_LENGTH) die("gc detected gq length:%lu", gq.num);
//        if (queue_length < GAR_QUEUE_LENGTH) die("gc detected gq length:%lu", queue_length);

//        uint64_t counter = 0;

#ifdef GC_DEBUG_OUTPUT
        printf("merge ");
#endif

        // todo: how to properly store metadata
        while (queue != NULL) {

            // acquire a new log, it is poss
//            if (counter == 0) {
//                if (log_acquire(1) == NULL)die("cannot acquire new log");
//            }


            struct log *target_log = (struct log *) (log_meta + CACHE_LINE_SIZE * queue->index);
            char *current_ptr = target_log->base;
            char *end_ptr = target_log->curr;

#ifdef GC_DEBUG_OUTPUT
            printf("%lu->%lu ", queue->index, thread_log->index);
#endif

            while (current_ptr < end_ptr) {

                // read and advance the pointer
                struct log_cell *old_lc = (struct log_cell *) current_ptr;
                uint64_t total_size = sizeof(struct log_cell) + old_lc->value_size;
                if (thread_log == NULL || thread_log->available < total_size) {
                    if (log_acquire(1) == NULL)die("cannot acquire new log");
                }


                struct log_cell *new_lc = (struct log_cell *) thread_log->curr;


                // persist this entry to the new log first
                // todo: two flushes are required here
                new_lc->key = old_lc->key;
                new_lc->is_delete = old_lc->is_delete;
                new_lc->value_size = old_lc->value_size;
//                rdtscll(new_lc->version);
                new_lc->version = old_lc->version + 1; // todo: this is a heck
                pmem_persist(new_lc, sizeof(struct log_cell));

                pmem_memcpy_persist(thread_log->curr + sizeof(struct log_cell),
                                    current_ptr + sizeof(struct log_cell),
                                    new_lc->value_size);

//                pmem_memcpy_persist(thread_log->curr, current_ptr,
//                                    sizeof(struct log_cell) + new_lc->value_size);


                // this step might be buggy if went out of bound of the new log
                // ignore a cell if it is delete

                if (!new_lc->is_delete) {

                    // try to commit this entry
                    void *res = tree->put_and_return(new_lc->key, new_lc, 0, t);

                    // the log acquired by gc thread shouldn't need atomic ops
                    if (res != NULL) {
                        thread_log->available -= total_size;
                        thread_log->curr += total_size;
                    }
                }
                current_ptr += total_size;
            }


            log_release(queue->index);

            queue = queue->next;
//            counter++;
//            if (counter == GAR_QUEUE_LENGTH) counter = 0;

            if (thread_log->curr > thread_log->base + LOG_SIZE)
                die("log overflow detected used:%ld", thread_log->curr - thread_log->base);
        }

#ifdef GC_DEBUG_OUTPUT
        printf("\n");
#endif
    }

    return NULL;
}

void log_start_gc(masstree::masstree *t) {

    gc_ids = (pthread_t *) realloc(gc_ids, sizeof(pthread_t) * (++num_gcs));

    pthread_create(gc_ids + (num_gcs - 1), NULL, log_garbage_collection, t);
//    pthread_detach(gc_ids[num_gcs - 1]);

}

void log_end_gc() {

    pthread_cancel(gc_ids[num_gcs - 1]);
    gc_ids = (pthread_t *) realloc(gc_ids, sizeof(pthread_t) * (--num_gcs));
}

void log_join_all_gc() {

    gc_stopped = 1;
    puts("waiting gc");

    pthread_cond_broadcast(&gq.cond);

    for (int i = 0; i < num_gcs; i++) {
        pthread_join(gc_ids[i], NULL);
    }
}


void log_debug_print(int to_file, int show) {

    FILE *file = stdout;

    if (to_file) {
        char fn_buf[128];
        sprintf(fn_buf, "log_debug_print_%d.txt", to_file);
        file = fopen(fn_buf, "w");
    }

    pthread_mutex_lock(&lm_lock);

    uint64_t used = 0;

    for (uint64_t i = 0; i < lm.num_entries; i++) {

        if (lm.entries[i][0] == OCCUPIED) {
//            printf("%5lu ", i);
            used++;
            if (to_file) {
                struct log *target_log = (struct log *) (log_meta + CACHE_LINE_SIZE * i);
                fprintf(file, "log %lu available:%lu free:%lu\n",
                        i, target_log->available, target_log->freed.load());

            }
        }

//        if (used > 0 && used % 100 == 0) {
//            printf("\n");
//        }

    }

    pthread_mutex_unlock(&lm_lock);

    pthread_mutex_lock(&gq.lock);
    uint64_t len = gq.num;
    pthread_mutex_unlock(&gq.lock);

    if (show)printf("total logs used:%lu gq length:%lu\n", used, len);

    fflush(file);
}


int log_start_perf(const char *perf_fn) {

    char command[1024];
    sprintf(command,
            "sudo /home/blepers/linux/tools/perf/perf stat -e"

            "uncore_imc_1/event=0xe2,umask=0x0/,"
            "uncore_imc_1/event=0xe3,umask=0x0/,"
            "uncore_imc_1/event=0xe6,umask=0x0/,"
            "uncore_imc_1/event=0xe7,umask=0x0/,"

            "uncore_imc_4/event=0xe2,umask=0x0/,"
            "uncore_imc_4/event=0xe3,umask=0x0/,"
            "uncore_imc_4/event=0xe6,umask=0x0/,"
            "uncore_imc_4/event=0xe7,umask=0x0/,"

            "uncore_imc_7/event=0xe2,umask=0x0/,"
            "uncore_imc_7/event=0xe3,umask=0x0/,"
            "uncore_imc_7/event=0xe6,umask=0x0/,"
            "uncore_imc_7/event=0xe7,umask=0x0/,"

            "uncore_imc_10/event=0xe2,umask=0x0/,"
            "uncore_imc_10/event=0xe3,umask=0x0/,"
            "uncore_imc_10/event=0xe6,umask=0x0/,"
            "uncore_imc_10/event=0xe7,umask=0x0/ "


            "> %s 2>&1 &",
            perf_fn
    );

//    sprintf(command,
//            "/home/blepers/linux/tools/perf/perf record --call-graph dwarf -p %d -o %s -g >> perf.out 2>&1 &",
//            getpid(), perf_fn);
//    perf_stat = 0;

//    sprintf(command,
//            "sudo /home/blepers/linux/tools/perf/perf stat "
//            "-e SQ_MISC.SQ_FULL "
//            "-e CYCLE_ACTIVITY.CYCLES_MEM_ANY "
//            "-e OFFCORE_REQUESTS_OUTSTANDING.CYCLES_WITH_DATA_RD "
//            "-e OFFCORE_REQUESTS_OUTSTANDING.CYCLES_WITH_DEMAND_RFO "
//            "-e RESOURCE_STALLS.SB "
//            "-e UNC_M_RPQ_OCCUPANCY_PCH0 "
//            "-e UNC_M_RPQ_OCCUPANCY_PCH1 "
//            "-e UNC_M_WPQ_OCCUPANCY_PCH0 "
//            "-e UNC_M_WPQ_OCCUPANCY_PCH1 "
//            "-p %d > %s 2>&1 &",
//            getpid(), perf_fn
//    );
//
//    sprintf(command,
//            "sudo /home/blepers/linux/tools/perf/perf stat -d -d -d -p %d > %s 2>&1 &",
//            getpid(), perf_fn
//    );

//    sprintf(command,
//            "/home/blepers/linux/tools/perf/perf record -p %d -o %s -g >> perf.out 2>&1 &",
//            getpid(), perf_fn);

//    printf("perf: %s\n", command);
    int res = system(command);
    rdtscll(perf_start_rtd);

    return res;
}

int log_stop_perf() {

    rdtscll(perf_stop_rtd);

    char command[1024];
    sprintf(command, "sudo killall -s INT -w perf");
//    printf("perf: %s\n", command);


    int res = system(command);
    return res;
}


void log_print_pmem_bandwidth(const char *perf_fn, double elapsed) {

    if (!perf_stat) return;

    const char *pmem_sticks[] = {
            "uncore_imc_1/",
            "uncore_imc_4/",
            "uncore_imc_7/",
            "uncore_imc_10/"
    };
    int length = 4;

    FILE *file = fopen(perf_fn, "r");
    if (file == NULL) return;
    char buf[1024];

    uint64_t read = 0;
    uint64_t write = 0;
    uint64_t read_b_cycle = 0;
    uint64_t write_b_cycle = 0;

    while (fgets(buf, 1024, file)) {

        char no_use[1024];

        for (int i = 0; i < length; i++) {

            if (strstr(buf, pmem_sticks[i])) {

                uint64_t number;

                sscanf(buf, "%lu %s", &number, no_use);

                if (strstr(buf, "0xe2")) {
                    if (number > read_b_cycle)read_b_cycle = number;
                } else if (strstr(buf, "0xe3")) {
                    read += number;
                } else if (strstr(buf, "0xe6")) {
                    if (number > write_b_cycle)write_b_cycle = number;
                } else if (strstr(buf, "0xe7")) {
                    write += number;
                }
            }
        }
    }

    uint64_t elapsed_cycles = perf_stop_rtd - perf_start_rtd;

    double read_b_percent = (double) read_b_cycle / (double) elapsed_cycles * 100.0f;
    double read_bw = (double) read * 64.0f / 1024.0f / 1024.0f / 1024.0f / elapsed;
    double write_b_percent = (double) write_b_cycle / (double) elapsed_cycles * 100.0f;
    double write_bw = (double) write * 64.0f / 1024.0f / 1024.0f / 1024.0f / elapsed;

    printf("\nread: %.2f%% %.2fgb/s write: %.2f%% %.2fgb/s\n",
           read_b_percent, read_bw, write_b_percent, write_bw);
}