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
struct log *log_meta;
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

#ifdef NO_PERSIST
    puts("NO PERSIST");
#endif

}


void log_tree_rebuild(masstree::masstree *tree, int num_threads, int read_tree) {

    for (uint64_t i = 0; i < lm.num_entries; i++) {
        struct log *target_log = log_meta + i;
        target_log->freed.store(0);
        target_log->available = LOG_SIZE;
        target_log->curr = big_map + i * LOG_SIZE;
        target_log->index = i;
        target_log->base = big_map + i * LOG_SIZE;
        target_log->full.store(0);
    }

    if (read_tree) {

        printf("\n... rebuilding tree using %d omp threads ...\n", num_threads);

        int old_num_threads = omp_get_num_threads();
        omp_set_num_threads(num_threads);
        std::atomic<uint64_t> recovered;
        recovered.store(0);

        auto starttime = std::chrono::system_clock::now();

        // process inserts first todo: OCCUPIED ENUM IS WRONG NOW
#pragma omp parallel for schedule(static, 1)
        for (uint64_t i = 0; i < lm.num_entries; i++) {

            // todo: not sure if this has overhead
            auto t = tree->getThreadInfo();

            char *end = big_map + (i + 1) * LOG_SIZE;
            char *curr = big_map + i * LOG_SIZE;
            struct log *current_log = log_meta + i;

            while (curr < end) {

                struct log_cell *lc = (struct log_cell *) curr;

                // if field of the struct is zero, then abort the entire log
                if (lc->version == 0) break;

                uint64_t total_size = sizeof(struct log_cell) + lc->value_size;
                current_log->available -= total_size;

                if (!lc->is_delete) {

                    struct log_cell *res = (struct log_cell *)
                            tree->put_and_return(lc->key, lc, 1, 1, t);

                    // insert success and created a new key-value
                    // replaced a value, should free some space in other log
                    if (res != NULL) {

                        if (res != lc) {
                            uint64_t idx = (uint64_t) ((char *) res - big_map) / LOG_SIZE;
                            struct log *target_log = log_meta + idx;
                            target_log->freed.fetch_add(sizeof(struct log_cell) + res->value_size);
                        } else {
                            recovered.fetch_add(1);
                        }
                    }

                } else {

                    //todo: this is incorrect now
                    tree->del_and_return(lc->key, 1, lc->version,
                                         log_get_tombstone, t);
                }

                // todo: should probably update the metadata here
                curr += sizeof(struct log_cell) + lc->value_size;
            }
        }

        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);

        omp_set_num_threads(old_num_threads);
        printf("... rebuild complete, recovered %lu keys throughput %.2f ops/us...\n",
               recovered.load(), (recovered.load() * 1.0) / duration.count());
    }



    // sequentially reconstruct metadata
    // from end to beginning


    lm.used = lm.num_entries;
    lm.next_available = -1;

    for (int i = lm.num_entries - 1; i >= 0; i--) {

        struct log *target_log = log_meta + i;

        if (target_log->available == LOG_SIZE ||
            target_log->freed.load() == LOG_SIZE - target_log->available) {

            if (lm.used == i + 1) {
                lm.used--;
            } else {
                lm.entries[i][0] = lm.next_available;
                lm.next_available = i;
            }
        }
    }

}


uint64_t log_map(int use_pmem, const char *fn, uint64_t file_size,
                 void **result, int *pre_set, int alignment, int pre_fault_threads) {

    (void) pre_fault_threads;
    void *map = NULL;
    size_t mapped_len = 0;
    int is_pmem = 1;


    if (use_pmem) {


        if (file_size == 0) {
            map = pmem_map_file(fn, 0, 0, 0, &mapped_len, &is_pmem);
        } else {
            map = pmem_map_file(fn, file_size,
                                PMEM_FILE_CREATE | PMEM_FILE_EXCL, 00666,
                                &mapped_len, &is_pmem);
            if (mapped_len != file_size) die("map error mapped_len:%zu", mapped_len);
        }
        is_pmem = is_pmem && pmem_is_pmem(map, mapped_len);

    } else {


        mapped_len = file_size;

//        map = mmap(NULL, file_size, PROT_READ | PROT_WRITE, MAP_ANONYMOUS, -1, 0);
        map = malloc(file_size);

    }


    if (map == NULL || map == MAP_FAILED || !is_pmem)
        die("map error map:%p is_pmem:%d", map, is_pmem);

    if (mapped_len == 0 || mapped_len % alignment != 0)
        die("alignment check error size:%zu", mapped_len);

    if (pre_set != NULL) {

        int value = *pre_set;
        size_t PAGE_SIZE = sysconf(_SC_PAGESIZE);

        if (mapped_len % CACHE_LINE_SIZE != 0 && mapped_len % PAGE_SIZE != 0) {
            die("cannot memset size:%zu", mapped_len);
        }

        log_start_perf("fault.perf");


        auto starttime = std::chrono::system_clock::now();

//        memset(map, value, mapped_len);

        for (size_t i = 0; i < mapped_len; i += PAGE_SIZE) {
            ((char *) map)[i] = value;
        }

        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);

        log_stop_perf();
        log_print_pmem_bandwidth("fault.perf", duration.count() / 1000000.0, NULL);


        printf("\n\tpre-faulted %p %-30s %7.2fgb/s %7.2fs\n",
               map, fn, (mapped_len * 2.0 / 1024.0 / 1024.0 / 1024.0) / (duration.count() / 1000000.0),
               duration.count() / 1000000.0);

    }

    *result = map;
    return mapped_len;
}

void log_recover(masstree::masstree *tree, int num_threads) {
    log_structs_size_check();

    uint64_t mapped_len;

    mapped_len = log_map(1, INODE_FN, 0,
                         (void **) &inodes, NULL, CACHE_LINE_SIZE,
                         num_threads);


    if (log_map(1, META_FN, 0,
                (void **) &log_meta, NULL,
                CACHE_LINE_SIZE, num_threads) != mapped_len)
        die("META filesize inconsistent");

    uint64_t num_logs = mapped_len / CACHE_LINE_SIZE;

    mapped_len = log_map(1, LOG_FN, 0,
                         (void **) &big_map, NULL, LOG_SIZE, num_threads);
    if (mapped_len != num_logs * LOG_SIZE) die("big_map mapped_len:%zu", mapped_len);


    // inodes
    lm.num_entries = num_logs;
    lm.entries = (int **) malloc(sizeof(int *) * lm.num_entries);
    OCCUPIED = num_logs + 1;
    for (uint64_t i = 0; i < lm.num_entries; i++) {
        lm.entries[i] = (int *) (inodes + CACHE_LINE_SIZE * i);
    }

    // reconstruct tree
    log_tree_rebuild(tree, num_threads, 1);

    // gc
    pthread_mutex_init(&gq.lock, NULL);
    pthread_cond_init(&gq.cond, NULL);
    gq.head = NULL;
    gq.num = 0;

    inited = 1;
}


void log_init(uint64_t pool_size, int pre_fault_threads) {


    log_structs_size_check();

    uint64_t num_logs = pool_size / LOG_SIZE;
    uint64_t file_size = num_logs * CACHE_LINE_SIZE;
    int preset = 0;


    int *pptr = NULL;
    if (pre_fault_threads > 0)pptr = &preset;

    // this region controls pre fault?
    log_map(0, INODE_FN, file_size, (void **) &inodes, pptr, CACHE_LINE_SIZE,
            pre_fault_threads);
    log_map(0, META_FN, file_size, (void **) &log_meta, pptr, CACHE_LINE_SIZE,
            pre_fault_threads);

    file_size = num_logs * LOG_SIZE;
    log_map(1, LOG_FN, file_size, (void **) &big_map, pptr, LOG_SIZE,
            pre_fault_threads);

    // inodes
    lm.num_entries = num_logs;
    lm.entries = (int **) malloc(sizeof(int *) * lm.num_entries);
    OCCUPIED = num_logs + 1;
    for (uint64_t i = 0; i < lm.num_entries; i++) {
        lm.entries[i] = (int *) (inodes + CACHE_LINE_SIZE * i);
    }

    // usage
    log_tree_rebuild(NULL, 0, 0);


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

        thread_log = log_meta + i;

//        pmem_persist(thread_log, CACHE_LINE_SIZE);
    }
    pthread_mutex_unlock(&lm_lock);
    return log_address;

}

void log_release(uint64_t idx) {

    // todo: how to reduce the number of logs need to be scanned
    // BUG here, did not store FULL

    pthread_mutex_lock(&lm_lock);

    lm.entries[idx][0] = lm.next_available;
    lm.next_available = idx;

    struct log *target_log = log_meta + idx;
    target_log->curr = target_log->base;
    target_log->available = LOG_SIZE;
    target_log->freed.store(0);

    pthread_mutex_unlock(&lm_lock);
}

void *log_malloc(size_t size) {


    // the "freed" space should be strictly increasing
    if (unlikely(thread_log == NULL || thread_log->available < size)) {
        if (log_acquire(1) == NULL)die("cannot acquire new log");
    }

    // write and decrease size
    thread_log->available -= size;

    char *to_return = thread_log->curr;
    thread_log->curr += size;
    return to_return;
}


int log_memalign(void **memptr, size_t alignment, size_t size) {

    size += ((size + sizeof(uint64_t)) / alignment + 1) * alignment - size - sizeof(uint64_t);


    // todo: how to make sure memory is aligned
    *memptr = log_malloc(size);

    return 0;
}

void *log_get_tombstone(uint64_t key) {


    // todo: this here is potentially not 256-byte aligned
    struct log_cell *lc = (struct log_cell *) log_malloc(sizeof(struct log_cell));
//    struct log_cell *lc = (struct log_cell *) log_malloc(256);

    rdtscll(lc->version);
    lc->value_size = 0;
    lc->key = key;
    lc->is_delete = 1;


    pmem_persist(lc, sizeof(struct log_cell));


    return lc;
}

void log_free(void *ptr) {

    char *char_ptr = (char *) ptr;

    // commit a dummy log to represent that this entry has been freed
    struct log_cell *lc = (struct log_cell *) ptr;


    // locate the log and its metadata
    uint64_t idx = (uint64_t) (char_ptr - big_map) / LOG_SIZE;
    struct log *target_log = log_meta + idx;

    // update metadata and add the log to GC queue if suitable
    uint64_t freed = target_log->freed.fetch_add(sizeof(struct log_cell) + lc->value_size);


    if (freed >= LOG_MERGE_THRESHOLD) {

        uint64_t can_collect = target_log->full.load();

        if (can_collect && target_log->full.compare_exchange_strong(can_collect, 0)) {
            log_gq_add(idx);
        }
    }

}


void *log_garbage_collection(void *arg) {

    masstree::masstree *tree = (masstree::masstree *) arg;
    auto t = tree->getThreadInfo();

    while (1) {

        // wait for other threads to wait me up
        pthread_mutex_lock(&gq.lock);

        if (!gc_stopped) {
            if (gq.num == 0) {
                pthread_cond_wait(&gq.cond, &gq.lock);
            }
        } else if (gq.num == 0) {
            pthread_mutex_unlock(&gq.lock);
            break;
        }


        // gc takes the entire queue and release the lock instantly
        struct garbage_queue_node *queue = gq.head;
//        uint64_t queue_length = gq.num;

        if (gq.head != NULL) {
            gq.head = gq.head->next;
            gq.num--;
        }

        struct garbage_queue_node *tail = queue;

        for (int tmp = 1; tmp < NUM_LOG_PER_COLLECTION; tmp++) {

            if (gq.head == NULL) break;
            gq.head = gq.head->next;
            gq.num--;

            tail = tail->next;
        }

        if (tail != NULL)tail->next = NULL;

        if (gq.head != NULL) pthread_cond_signal(&gq.cond);

//        gq.head = NULL;
//        gq.num = 0;

        pthread_mutex_unlock(&gq.lock);


        while (queue != NULL) {

            // acquire a new log, it is poss
//            if (counter == 0) {
//                if (log_acquire(1) == NULL)die("cannot acquire new log");
//            }


            struct log *target_log = log_meta + queue->index;
            char *current_ptr = target_log->base;
            char *end_ptr = target_log->curr;


            while (current_ptr < end_ptr) {

                // read and advance the pointer
                struct log_cell *old_lc = (struct log_cell *) current_ptr;
                uint64_t total_size = sizeof(struct log_cell) + old_lc->value_size;
                if (thread_log == NULL || thread_log->available < total_size) {
                    if (log_acquire(1) == NULL)die("cannot acquire new log");
                }

                // persist this entry to the new log first


                // this step might be buggy if went out of bound of the new log
                // ignore a cell if it is deleted

                // lock the node
                struct masstree_put_to_pack pack = tree->put_to_lock(old_lc->key, t);
                if (pack.leafnode != NULL && pack.p != -1) {

                    // now we have the lock
                    masstree::leafnode *l = (masstree::leafnode *) pack.leafnode;
                    struct log_cell *current_value_in_tree = (struct log_cell *) l->value(pack.p);

                    if (!old_lc->is_delete) {

                        if (current_value_in_tree->version <= old_lc->version) {

                            pmem_memcpy_persist(thread_log->curr, current_ptr, total_size);


                            l->assign_value(pack.p, thread_log->curr);
                            thread_log->available -= total_size;
                            thread_log->curr += total_size;

                        } else {

                            // if this entry is ignored, then decrease the reference counter by 1
                            // no lock is needed, the leaf node is already locked

                            int ref = l->reference(pack.p);
                            if (ref > 0) {
                                l->modify_reference(pack.p, -1);
                            }

                            // free if it's a tombstone
                            if (ref == 1 && current_value_in_tree->is_delete) {

                                log_free(current_value_in_tree);
                            }
                        }

                        tree->put_to_unlock(pack.leafnode);

                    } else {

                        // if process a delete-type entry, check if the reference is at 0 first
                        // if ref=0 then the tombstone is not protecting anything
                        // do we need to check version?



                        if (l->reference(pack.p) == 0) {

                            // we have a tombstone and the reference is 0, attempt to delete again
                            tree->put_to_unlock(pack.leafnode);
//                            tombstones++;
                            // the version trick should solve the put-del-put situation
                            // a tombstone with old version will not be accepted

                            tree->del_and_return(old_lc->key, 1,
                                                 old_lc->version, NULL, t);

                        } else {

                            pmem_memcpy_persist(thread_log->curr, current_ptr, total_size);


                            l->assign_value(pack.p, thread_log->curr);
                            thread_log->available -= total_size;
                            thread_log->curr += total_size;

                            tree->put_to_unlock(pack.leafnode);
                        }
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


    }

//    printf("tombstone: %lu\n", tombstones);
//    return (void *) tombstones;
    return NULL;
}

void log_start_gc(masstree::masstree *t, int use_me) {


    pthread_attr_t attr;
    cpu_set_t cpu;
    CPU_ZERO(&cpu);
    CPU_SET(use_me, &cpu); // reserving CPU 0

    pthread_attr_init(&attr);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpu);


    gc_ids = (pthread_t *) realloc(gc_ids, sizeof(pthread_t) * (++num_gcs));

    pthread_create(gc_ids + (num_gcs - 1), &attr, log_garbage_collection, t);

//    pthread_detach(gc_ids[num_gcs - 1]);

}

void log_end_gc() {

    pthread_cancel(gc_ids[num_gcs - 1]);
    gc_ids = (pthread_t *) realloc(gc_ids, sizeof(pthread_t) * (--num_gcs));
}


void log_wait_all_gc() {

    while (1) {
        pthread_mutex_lock(&gq.lock);
        if (gq.num == 0) {
            pthread_mutex_unlock(&gq.lock);
            break;
        }
        pthread_mutex_unlock(&gq.lock);
        pthread_cond_broadcast(&gq.cond);
        usleep(4);
    }
}

void log_join_all_gc() {

    puts("waiting gc");
    gc_stopped = 1;

    // possible signal lost
    pthread_cond_broadcast(&gq.cond);


    for (int i = 0; i < num_gcs; i++) {
        pthread_join(gc_ids[i], NULL);
    }


}


void log_debug_print(FILE *f, int using_log) {

//    FILE *file = stdout;
//
//    if (to_file) {
//        char fn_buf[128];
//        sprintf(fn_buf, "log_debug_print_%d.txt", to_file);
//        file = fopen(fn_buf, "w");
//    }

    if (!using_log) {
        fprintf(f, "0,");
        return;
    }

    pthread_mutex_lock(&lm_lock);

    uint64_t used = 0;

    for (uint64_t i = 0; i < lm.num_entries; i++) {

        if (lm.entries[i][0] == OCCUPIED) {
//            printf("%5lu ", i);
            used++;

        }


    }

    pthread_mutex_unlock(&lm_lock);

    pthread_mutex_lock(&gq.lock);
    uint64_t len = gq.num;
    pthread_mutex_unlock(&gq.lock);


    printf("total logs used:%lu gq length:%lu\n", used, len);
    fprintf(f, "%.2f,", ((double) (used * LOG_SIZE)) / 1024. / 1024. / 1024.);


//    fflush(file);
}


int log_start_perf(const char *perf_fn) {

    (void) perf_fn;


    int cores = sysconf(_SC_NPROCESSORS_ONLN);
    int res = 1;

//    sprintf(command,
//            "sudo taskset -c %d-%d /home/blepers/linux-huge/tools/perf/perf record --call-graph dwarf -F 100 -p %d -o %s.record -g >> perf_record.out 2>&1 &",
//            cores * 3 / 4, cores - 1, getpid(), perf_fn);
//    res &= system(command);
//    perf_stat = 0;

    char real_command[4096];
//    sprintf(real_command, "sudo taskset -c %d-%d /home/blepers/linux-huge/tools/perf/perf %s", cores * 3 / 4, cores - 1,
//            command);

//    printf("perf: %s\n", command);

    remove("/mnt/sdb/xiaoxiang/pcm.txt");
    sprintf(real_command,
            "sudo taskset -c %d-%d /mnt/sdb/xiaoxiang/pcm/build/bin/pcm-memory -all >pcm-memory.log 2>&1 &",
            cores * 3 / 4, cores - 1);

    res &= system(real_command);


    sprintf(real_command, "sudo taskset -c %d-%d /home/blepers/linux-huge/tools/perf/perf stat "
                          "-e cycle_activity.stalls_l1d_miss "
                          "-e cycle_activity.stalls_l2_miss "
                          "-e cycle_activity.stalls_l3_miss "
                          "-e cycle_activity.stalls_mem_any "
                          "-e cycle_activity.stalls_total "
                          "-e resource_stalls.sb "
                          "-p %d -o %s.stat -g >> perf_stat.out 2>&1 &",
            cores * 3 / 4, cores - 1, getpid(), perf_fn);

    res &= system(real_command);

    sleep(1);
    rdtscll(perf_start_rtd)

    return res;
}

int log_stop_perf() {

    rdtscll(perf_stop_rtd)

    char command[1024];
    sprintf(command, "sudo killall -s INT -w perf");
    sprintf(command, "sudo killall -s INT perf");
//    printf("perf: %s\n", command);

    int res = system(command);

    sprintf(command, "sudo pkill --signal SIGHUP -f pcm-memory");

    res &= system(command);
    sleep(1);


    return res;
}


void log_print_pmem_bandwidth(const char *perf_fn, double elapsed, FILE *f) {

    (void) perf_fn;

    if (!perf_stat) {

        if (f != NULL)fprintf(f, ",,");
        return;
    }


    uint64_t pmem_read;
    uint64_t pmem_write;
    uint64_t dram_read;
    uint64_t dram_write;

    int scanned_channel;

    while (1) {

        scanned_channel = 0;

        FILE *file = fopen("/mnt/sdb/xiaoxiang/pcm.txt", "r");
        pmem_read = 0;
        pmem_write = 0;
        dram_read = 0;
        dram_write = 0;

        char buffer[256];
        int is_first_line = 1;
        while (fgets(buffer, 256, file) != NULL) {

            if (is_first_line) {
                is_first_line = 0;
                continue;
            }

            uint64_t skt, channel, pmmReads, pmmWrites, elapsedTime, dramReads, dramWrites;
            sscanf(buffer, "%lu %lu %lu %lu %lu %lu %lu",
                   &skt, &channel, &pmmReads, &pmmWrites, &elapsedTime, &dramReads, &dramWrites
            );

            scanned_channel++;
            pmem_read += pmmReads;
            pmem_write += pmmWrites;
            dram_read += dramReads;
            dram_write += dramWrites;
        }

        if (scanned_channel >= 16) {
            break;
        } else {
            puts("pcm.txt parse failed");
        }
    }


    double read_gb = (double) pmem_read / 1024.0f / 1024.0f / 1024.0f;
    double write_gb = (double) pmem_write / 1024.0f / 1024.0f / 1024.0f;

    double read_bw = read_gb / elapsed;
    double write_bw = write_gb / elapsed;

    double dram_read_gb = (double) dram_read / 1024.0f / 1024.0f / 1024.0f;
    double dram_write_gb = (double) dram_write / 1024.0f / 1024.0f / 1024.0f;

    double dram_read_bw = dram_read_gb / elapsed;
    double dram_write_bw = dram_write_gb / elapsed;


    printf("\n");


    printf("PR: ");
//    printf("%.2fgb ", read_gb);
    printf("%.2fgb/s ", read_bw);

    printf("PW: ");
//    printf("%.2fgb ", write_gb);
    printf("%.2fgb/s ", write_bw);

    printf("elapsed: %.2f ", elapsed);

    printf("DR: ");
//    printf("%.2fgb ", dram_read_gb);
    printf("%.2fgb/s ", dram_read_bw);

    printf("DW: ");
//    printf("%.2fgb ", dram_write_gb);
    printf("%.2fgb/s ", dram_write_bw);

    printf("\n");


    if (f != NULL) {
        fprintf(f, "%.2f,%.2f,%.2f,%.2f,", read_gb, read_bw, write_gb, write_bw);
        fprintf(f, "%.2f,%.2f,%.2f,%.2f,", dram_read_gb, dram_read_bw, dram_write_gb, dram_write_bw);
    }

}