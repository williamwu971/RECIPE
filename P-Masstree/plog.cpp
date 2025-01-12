#include "plog.h"

extern void *(*cpy_persist)(void *, const void *, size_t);

int inited = 0;

// the absolute base address
int *inodes;
char *big_map;

// log map and lock to protect the map
struct log_list_pack log_list;

// every thread hold its own log
struct log *log_meta;
__thread struct log *thread_log = nullptr;


struct garbage_queue gq;


void log_structs_size_check() {

    // some structs are required to occupy a cache line
    assert(sizeof(char) == 1);
    assert(sizeof(struct log) == CACHE_LINE_SIZE);
}

void log_list_init(int num_log, int *list_area) {

    memset(&log_list, 0, sizeof(struct log_list_pack));

    log_list.occupied = 0;
    log_list.num_log = num_log;
    log_list.next = 0;
    log_list.list = list_area;

    int i = 0;
    for (; i < num_log - 1; i++) {
        log_list.list[i] = i + 1;
    }
    log_list.list[i] = -1;

    pthread_mutex_init(&log_list.list_lock, nullptr);
}

void log_list_push(int log_index) {
    pthread_mutex_lock(&log_list.list_lock);

    log_list.occupied--;
    log_list.list[log_index] = log_list.next;
    log_list.next = log_index;

    pthread_mutex_unlock(&log_list.list_lock);
}

int log_list_get() {
    pthread_mutex_lock(&log_list.list_lock);

    log_list.occupied++;
    int res = log_list.next;

    if (res != -1)log_list.next = log_list.list[res];

    pthread_mutex_unlock(&log_list.list_lock);

    return res;
}

int log_list_occupied() {

    pthread_mutex_lock(&log_list.list_lock);
    int res = log_list.occupied;
    pthread_mutex_unlock(&log_list.list_lock);

    return res;
}

void log_gq_init() {

    pthread_mutex_init(&gq.lock, nullptr);
    pthread_cond_init(&gq.cond, nullptr);
    gq.head = nullptr;
    gq.num = 0;

    gq.gc_stopped = 0;
    gq.gc_ids = nullptr;
    gq.num_gcs = 0;
}

void log_gq_add(uint64_t idx) {
    auto node = (struct garbage_queue_node *) malloc(sizeof(struct garbage_queue_node));
    node->index = idx;

    pthread_mutex_lock(&gq.lock);

    // add the node to the queue
    node->next = gq.head;
    gq.head = node;
    gq.num++;


    // wake up ONE garbage collector if queue is long enough

    if (gq.num >= GAR_QUEUE_LENGTH) {
        pthread_cond_signal(&gq.cond);
    }

    pthread_mutex_unlock(&gq.lock);
}

int log_gq_get(struct garbage_queue_node **place) {

    // wait for other threads to wait me up
    pthread_mutex_lock(&gq.lock);

    if (gq.num == 0) {
        if (gq.gc_stopped) {
            pthread_mutex_unlock(&gq.lock);
            return 0;
        } else {
            pthread_cond_wait(&gq.cond, &gq.lock);
        }
    }


    // gc takes the entire queue and release the lock instantly
    *place = gq.head;
    struct garbage_queue_node *queue = gq.head;

    if (queue == nullptr) {
        pthread_mutex_unlock(&gq.lock);
        return 1;
    }

    for (int tmp = 1; tmp < NUM_LOG_PER_COLLECTION; tmp++) {

        if (queue->next == nullptr) {
            break;
        } else {
            queue = queue->next;
            gq.num--;
        }
    }

    struct garbage_queue_node *new_head = queue->next;
    queue->next = nullptr;
    gq.head = new_head;
    gq.num--;

    if (gq.head != nullptr) pthread_cond_signal(&gq.cond);

    pthread_mutex_unlock(&gq.lock);
    return 1;
}

void log_start_gc(masstree::masstree *t, int start_cpu, int end_cpu) {

    cpu_set_t cpu;
    CPU_ZERO(&cpu);

    // reserving CPU 0
    for (int i = start_cpu; i < end_cpu; i++) {
        CPU_SET(i, &cpu);
    }

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpu);


    gq.gc_ids = (pthread_t *) realloc(gq.gc_ids, sizeof(pthread_t) * (++gq.num_gcs));

    pthread_create(gq.gc_ids + (gq.num_gcs - 1), &attr, log_garbage_collection, t);

//    pthread_detach(gc_ids[num_gcs - 1]);

}

void log_end_gc() {

    pthread_cancel(gq.gc_ids[gq.num_gcs - 1]);
    gq.gc_ids = (pthread_t *) realloc(gq.gc_ids, sizeof(pthread_t) * (--gq.num_gcs));
}


void log_wait_all_gc() {

    while (true) {
        pthread_cond_broadcast(&gq.cond);
        pthread_mutex_lock(&gq.lock);
        if (gq.num == 0 || gq.num_gcs == 0) {
            pthread_mutex_unlock(&gq.lock);
            break;
        }
        pthread_mutex_unlock(&gq.lock);
        usleep(4);
    }
}

void log_join_all_gc() {

    gq.gc_stopped = 1;

    // possible signal lost
    pthread_cond_broadcast(&gq.cond);

    for (int i = 0; i < gq.num_gcs; i++) {
        pthread_join(gq.gc_ids[i], nullptr);
    }

}

void log_debug_print(FILE *f, int using_log) {

    if (!using_log) {
        fprintf(f, "0,");
        return;
    }

    int used = log_list_occupied();

    pthread_mutex_lock(&gq.lock);
    uint64_t len = gq.num;
    pthread_mutex_unlock(&gq.lock);


    printf("total logs used:%d gq length:%lu\n", used, len);
    fprintf(f, "%.2f,", ((double) (used * LOG_SIZE)) / 1024. / 1024. / 1024.);
}

pthread_mutex_t *log_set_locks;
std::set<char *> *marked_blks;

void log_rebuild_claim(void *ptr) {

    auto lc = (struct log_cell *) ptr;
    char *char_ptr = (char *) ptr;


    // locate the log and its metadata
    uint64_t idx = (uint64_t)(char_ptr - big_map) / LOG_SIZE;
//    struct log_blk *target = log_blks + idx;
    struct log *target_log = log_meta + idx;

    pthread_mutex_lock(log_set_locks + idx);

    auto result = marked_blks[idx].insert((char *) ptr);

    if (result.second) {
        target_log->available -= sizeof(struct log_cell) + lc->value_size;

        char *end_ptr = (char *) (lc + 1);
        if (end_ptr > target_log->curr) {
            target_log->curr = end_ptr;
        }
    }

    pthread_mutex_unlock(log_set_locks + idx);
}

void log_rebuild_compute_free() {

    for (int i = 0; i < log_list.num_log; i++) {

        if (marked_blks[i].empty()) {
            continue;
        }

        struct log *target_log = log_meta + i;
        char *prev_end = target_log->base;


        for (auto x: marked_blks[i]) {
            if (x != prev_end) {
                target_log->freed.fetch_add(x - prev_end);
            }

            auto lc = (struct log_cell *) x;
            prev_end = x + sizeof(struct log_cell) + lc->value_size;
        }

//        printf("Log %lu available %lu freed %lu\n",
//               target_log->index, target_log->available, target_log->freed.load());
    }

}


struct log_rebuild_args {
    masstree::masstree *tree;
    int id;
    int step;
};

pthread_mutex_t total_recover_lock = PTHREAD_MUTEX_INITIALIZER;
uint64_t log_total_recovered = 0;
uint64_t log_total_ops = 0;
uint64_t log_total_time_tree = 0;
uint64_t log_total_time_read = 0;
uint64_t log_total_time_meta = 0;


void *log_rebuild_thread(void *arg) {

    auto args = (struct log_rebuild_args *) arg;
    masstree::masstree *tree = args->tree;
    auto t = tree->getThreadInfo();

    uint64_t recovered = 0;
    uint64_t total_ops = 0;

    uint64_t time_tree = 0;
    uint64_t time_read = 0;
    uint64_t time_meta = 0;


    uint64_t a, b;

    for (int i = args->id; i < log_list.num_log; i += args->step) {

        struct log *current_log = log_meta + i;
        char *end = current_log->base + LOG_SIZE;

        while (current_log->curr < end) {

            auto lc = (struct log_cell *) current_log->curr;


            a = readTSC(1, 1);

            // if field of the struct is zero, then abort the entire log
            uint64_t total_size = sizeof(struct log_cell) + lc->value_size;
            if (current_log->curr + total_size > end || lc->value_size == 0) break; //  prevent overflow

            if (!masstree_checksum(lc, SUM_LOG, 0, total_size / sizeof(uint64_t) - 1, 0)) {
                break;
            }

            b = readTSC(1, 1);
            time_read += b - a;

            rdtscll(a)
            auto res = (struct log_cell *) tree->put_and_return(lc->key, lc, 1, 1, t);
            rdtscll(b)
            time_tree += b - a;

            rdtscll(a)

            current_log->available -= total_size;
            current_log->curr += total_size;
            // insert success and created a new key-value
            // replaced a value, should free some space in other log
            if (res != nullptr) {

                uint64_t idx = (uint64_t)((char *) res - big_map) / LOG_SIZE;
                struct log *target_log = log_meta + idx;
                target_log->freed.fetch_add(sizeof(struct log_cell) + res->value_size);

            } else if (!lc->is_delete) {
                recovered++;
            }
            total_ops++;

            rdtscll(b)
            time_meta += b - a;

        }
    }


    pthread_mutex_lock(&total_recover_lock);
    log_total_recovered += recovered;
    log_total_time_tree += time_tree;
    log_total_time_read += time_read;
    log_total_time_meta += time_meta;
    log_total_ops += total_ops;
    pthread_mutex_unlock(&total_recover_lock);

    return nullptr;
}

void log_tree_rebuild(masstree::masstree *tree, int num_threads) {

    for (int i = 0; i < log_list.num_log; i++) {
        struct log *target_log = log_meta + i;
        target_log->freed.store(0);
        target_log->available = LOG_SIZE;
        target_log->curr = big_map + i * LOG_SIZE;
        target_log->index = i;
        target_log->base = big_map + i * LOG_SIZE;
        target_log->full.store(0);
    }

    if (tree == nullptr) return;

    printf("\n... rebuilding tree using %d threads ...\n", num_threads);

    auto rebuild_threads = (pthread_t *) malloc(sizeof(pthread_t) * num_threads);
    auto args = (struct log_rebuild_args *) malloc(sizeof(struct log_rebuild_args) * num_threads);

    log_start_perf("rebuild.perf");

    auto starttime = std::chrono::system_clock::now();

    for (int i = 0; i < num_threads; i++) {
        args[i].tree = tree;
        args[i].id = i;
        args[i].step = num_threads;

        cpu_set_t cpu;
        CPU_ZERO(&cpu);

        // reserving CPU 0
        CPU_SET(i + 1, &cpu);


        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpu);

        pthread_create(rebuild_threads + i, &attr, log_rebuild_thread, args + i);
    }
    for (int i = 0; i < num_threads; i++) {
        pthread_join(rebuild_threads[i], nullptr);
    }

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);

    log_stop_perf();
    log_print_pmem_bandwidth("rebuild.perf", (double) duration.count() / 1000000.0, nullptr);

    printf("... rebuild complete, recovered %lu/%lu keys throughput %.2f ops/us %.2f sec...\n",
           log_total_recovered, log_total_ops, ((double) log_total_recovered * 1.0) / (double) duration.count(),
           (double) duration.count() / 1000000.0);

    printf("time slides:\n");
    printf("read: %.2fs\n", (double) log_total_time_read / (double) num_threads / 2000000000.0);
    printf("tree: %.2fs\n", (double) log_total_time_tree / (double) num_threads / 2000000000.0);
    printf("meta: %.2fs\n", (double) log_total_time_meta / (double) num_threads / 2000000000.0);


    // sequentially reconstruct metadata
    // from end to beginning

//    while (log_list_get() != -1) {}
//
//    for (int i = log_list.num_log - 1; i >= 0; i--) {
//
//        struct log *target_log = log_meta + i;
//
//        if (target_log->available == LOG_SIZE || target_log->freed.load() == LOG_SIZE - target_log->available) {
//
//            log_list_push(i);
//        }
//    }

}


uint64_t log_map(int use_pmem, const char *fn, uint64_t file_size,
                 void **result, const int *pre_set, int alignment) {

    void *map;
    size_t mapped_len = file_size;
    int is_pmem = 1;


    if (use_pmem) {


        if (file_size == 0) {
            map = pmem_map_file(fn, 0, 0, 0, &mapped_len, &is_pmem);
        } else {
            map = pmem_map_file(fn, file_size,
                                PMEM_FILE_CREATE | PMEM_FILE_EXCL, 00666,
                                &mapped_len, &is_pmem);

        }

    } else {

//        map = mmap(nullptr, file_size, PROT_READ | PROT_WRITE, MAP_ANONYMOUS, -1, 0);
        map = malloc(file_size);

    }


    if (map == nullptr || map == MAP_FAILED || !is_pmem) {
        die("map error map:%p is_pmem:%d", map, is_pmem);
    }

    if (mapped_len % alignment != 0) {
        die("map alignment check size:%zu alignment:%d", mapped_len, alignment);
    }


    if (pre_set != nullptr) {

        printf("\n");

        int value = *pre_set;
        size_t PAGE_SIZE = sysconf(_SC_PAGESIZE);

        log_start_perf("fault.perf");


        auto starttime = std::chrono::system_clock::now();

        for (size_t i = 0; i < mapped_len; i += PAGE_SIZE) {
            ((char *) map)[i] = (char) value;
//            memset((char *) map + i, value, PAGE_SIZE);
//            pmem_persist(((char *) map) + i, PAGE_SIZE);
        }

        auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
                std::chrono::system_clock::now() - starttime);

        log_stop_perf();
        log_print_pmem_bandwidth("fault.perf", (double) duration.count() / 1000000.0, nullptr);


        printf("pre-faulted %p %-30s %7.2fs\n", map, fn, (double) duration.count() / 1000000.0);

    }

    *result = map;
    return mapped_len;
}

void log_ralloc_recover() {

    log_structs_size_check();

    uint64_t mapped_len = log_map(1, LOG_FN, 0, (void **) &big_map, nullptr, LOG_SIZE);
    uint64_t num_logs = mapped_len / LOG_SIZE;

    int preset = 0;
    int *pptr = &preset;

    log_map(0, INODE_FN, sizeof(int) * num_logs, (void **) &inodes, pptr, sizeof(int));
    log_map(0, META_FN, sizeof(struct log) * num_logs, (void **) &log_meta, pptr, sizeof(struct log));

    // inodes
    log_list_init((int) num_logs, inodes);

    // reconstruct tree
    log_tree_rebuild(nullptr, 0);

//    log_blks = (struct log_blk *) calloc(num_logs, sizeof(struct log_blk));
//    for (uint64_t i = 0; i < num_logs; i++) {
//        pthread_mutex_init(&log_blks[i].log_set_locks, nullptr);
//        log_blks[i].marked_blk = std::set<char *>();
//    }

    log_set_locks = (pthread_mutex_t *) malloc(sizeof(pthread_mutex_t) * num_logs);
    for (uint64_t i = 0; i < num_logs; i++) {
        pthread_mutex_init(log_set_locks + i, nullptr);
    }
    marked_blks = new std::set<char *>[num_logs];


    // gc
    log_gq_init();

    inited = 1;
}

void log_recover(masstree::masstree *tree, int num_threads) {

    log_structs_size_check();

    uint64_t mapped_len = log_map(1, LOG_FN, 0, (void **) &big_map, nullptr, LOG_SIZE);
    uint64_t num_logs = mapped_len / LOG_SIZE;

    int preset = 0;
    int *pptr = &preset;

    log_map(0, INODE_FN, sizeof(int) * num_logs, (void **) &inodes, pptr, sizeof(int));
    log_map(0, META_FN, sizeof(struct log) * num_logs, (void **) &log_meta, pptr, sizeof(struct log));

    // inodes
    log_list_init((int) num_logs, inodes);

    // reconstruct tree
    log_tree_rebuild(tree, num_threads);

    // gc
    log_gq_init();

    inited = 1;
}


void log_init(uint64_t pool_size) {


    log_structs_size_check();

    uint64_t num_logs = pool_size / LOG_SIZE;
    uint64_t file_size = num_logs * LOG_SIZE;


    int preset = 0;
    int *pptr = &preset;

    log_map(1, LOG_FN, file_size, (void **) &big_map, pptr, LOG_SIZE);

    // this region controls pre fault?
    log_map(0, INODE_FN, num_logs * sizeof(int), (void **) &inodes, pptr, sizeof(int));
    log_map(0, META_FN, num_logs * sizeof(struct log), (void **) &log_meta, pptr, sizeof(struct log));


    // inodes
    log_list_init((int) num_logs, inodes);

    // usage
    log_tree_rebuild(nullptr, 0);


    // gc
    log_gq_init();

    inited = 1;
}

char *log_acquire(int write_thread_log) {

    if (!inited)die("not inited");

    int i = log_list_get();
    if (i == -1) die("all logs are occupied");

    char *log_address = big_map + i * LOG_SIZE;

    if (write_thread_log) {

        // retire and mark the old log for collection
        if (thread_log != nullptr) {

            if (thread_log->freed.load() > LOG_MERGE_THRESHOLD) {
                log_gq_add(thread_log->index);
            } else {
                thread_log->full.store(1);
            }
        }

        thread_log = log_meta + i;

    }

    return log_address;

}

void log_release(int idx) {

    // todo: how to reduce the number of logs need to be scanned

    struct log *target_log = log_meta + idx;
    target_log->freed.store(0);
    target_log->available = LOG_SIZE;
    target_log->full.store(0);
    target_log->curr = target_log->base;

    log_list_push(idx);
}

void *log_malloc(uint64_t size) {


    // the "freed" space should be strictly increasing
    while (unlikely(thread_log == nullptr || thread_log->available < size)) {
        if (log_acquire(1) == nullptr)die("cannot acquire new log");
    }

    // write and decrease size
    thread_log->available -= size;

    uint64_t to_return = (uint64_t) thread_log->curr;
    thread_log->curr += size;

    // hack
    size = size >> 48;

    // Set the unused 16 bits of the address
    to_return |= size;

    return (void *) to_return;
}


int log_memalign(void **memptr, size_t alignment, size_t size) {

    size += ((size + sizeof(uint64_t)) / alignment + 1) * alignment - size - sizeof(uint64_t);


    // todo: how to make sure memory is aligned
    *memptr = log_malloc(size);

    return 0;
}

void *log_get_tombstone(uint64_t key) {


    auto lc = (struct log_cell *) malloc(sizeof(struct log_cell) + sizeof(uint64_t));

    rdtscll(lc->version)
    lc->value_size = sizeof(uint64_t);
    lc->key = key;
    lc->is_delete = 1;

    masstree_checksum(lc, SUM_WRITE, 0, sizeof(struct log_cell) / sizeof(uint64_t), 0);


    // todo: this here is potentially not 256-byte aligned
    void *raw = log_malloc(sizeof(struct log_cell) + sizeof(uint64_t));
//    struct log_cell *lc = (struct log_cell *) log_malloc(256);

    cpy_persist(raw, lc, sizeof(struct log_cell) + sizeof(uint64_t));
    free(lc);

    return raw;
}

void log_free(void *ptr) {

    char *char_ptr = (char *) ptr;

    // commit a dummy log to represent that this entry has been freed
//    auto lc = (struct log_cell *) ptr;


    // locate the log and its metadata
    uint64_t idx = (uint64_t)(char_ptr - big_map) / LOG_SIZE;
    struct log *target_log = log_meta + idx;

    // update metadata and add the log to GC queue if suitable
//    uint64_t freed = target_log->freed.fetch_add(sizeof(struct log_cell) + lc->value_size);
    uint64_t freed = target_log->freed.fetch_add((uint64_t) ptr << 48);


    if (freed >= LOG_MERGE_THRESHOLD) {

        uint64_t can_collect = target_log->full.load();

        if (can_collect && target_log->full.compare_exchange_strong(can_collect, 0)) {
            log_gq_add(idx);
        }
    }

}


void *log_garbage_collection(void *arg) {

    auto tree = (masstree::masstree *) arg;
    auto t = tree->getThreadInfo();

    while (true) {

        struct garbage_queue_node *queue;

        if (!log_gq_get(&queue)) break;

        while (queue != nullptr) {

            struct log *target_log = log_meta + queue->index;
            char *current_ptr = target_log->base;
            char *end_ptr = target_log->curr;


            while (current_ptr < end_ptr) {

                // read and advance the pointer
                auto old_lc = (struct log_cell *) current_ptr;
                uint64_t total_size = sizeof(struct log_cell) + old_lc->value_size;

                // lock the node
                struct masstree_put_to_pack pack = tree->put_to_lock(old_lc->key, t);
                if (pack.leafnode != nullptr && pack.p != -1) {

                    // now we have the lock
                    auto l = (masstree::leafnode *) pack.leafnode;
                    auto current_value_in_tree = (struct log_cell *) l->value(pack.p);
                    int ref = l->reference(pack.p);

                    // always compare version fist, regardless of entry type
                    if (current_value_in_tree->version > old_lc->version) {
                        if (ref > 0) {
                            l->modify_reference(pack.p, -1);
                        }

                        // free if it's a tombstone
                        if (ref == 2 && current_value_in_tree->is_delete) {
                            log_free(current_value_in_tree);
                        }
                        tree->put_to_unlock(pack.leafnode);

                    } else {

                        if (old_lc->is_delete && ref == 1) {
                            tree->put_to_unlock(pack.leafnode);
                            tree->del_and_return(old_lc->key, 1, old_lc->version, nullptr, t);
                        } else {
                            void *new_slot = log_malloc(total_size);

                            // move entry to a new location
                            cpy_persist(new_slot, current_ptr, total_size);

                            l->assign_value(pack.p, new_slot);
                            tree->put_to_unlock(pack.leafnode);
                        }
                    }


                }
                current_ptr += total_size;
            }


            log_release((int) queue->index);


            struct garbage_queue_node *next = queue->next;
            free(queue);
            queue = next;

//            if (thread_log->curr > thread_log->base + LOG_SIZE) {
//                die("log overflow detected used:%ld", thread_log->curr - thread_log->base);
//            }
        }


    }
    return nullptr;
}


int log_start_perf(const char *perf_fn) {

    int cores = (int) sysconf(_SC_NPROCESSORS_ONLN);
    int res = 1;

    char command[4096];

//    sprintf(command,
//            "sudo taskset -c %d-%d /home/blepers/linux-huge/tools/perf/perf record "
//            "--call-graph dwarf -F 100 -C 1-19 -o %s.record -g >> perf_record.out 2>&1 &",
//            cores * 3 / 4, cores - 1, perf_fn);
//    puts("WARNING perf record is enabled");
//    res &= system(command);


//    sprintf(command, "sudo taskset -c %d-%d /home/blepers/linux-huge/tools/perf/perf %s", cores * 3 / 4, cores - 1,
//            command);

//    printf("perf: %s\n", command);

    remove("/mnt/sdb/xiaoxiang/pcm.txt");
    sprintf(command,
            "sudo taskset -c %d-%d /mnt/sdb/xiaoxiang/pcm/build/bin/pcm-memory -all >pcm-memory.log 2>&1 &",
            cores * 3 / 4, cores - 1);

    res &= system(command);


//    sprintf(command, "sudo taskset -c %d-%d /home/blepers/linux-huge/tools/perf/perf stat "
//                     "-e cycle_activity.stalls_l1d_miss "
//                     "-e cycle_activity.stalls_l2_miss "
//                     "-e cycle_activity.stalls_l3_miss "
//                     "-e cycle_activity.stalls_mem_any "
//                     "-e cycle_activity.stalls_total "
//                     "-e resource_stalls.sb "
//                     "-p %d -o %s.stat -g >> perf_stat.out 2>&1 &",
//            cores * 3 / 4, cores - 1, getpid(), perf_fn);
//
//    res &= system(command);

    sleep(1);

    return res;
}

int log_stop_perf() {

    char command[1024];
    int res;
//    sprintf(command, "sudo killall -s INT -w perf");
//    sprintf(command, "sudo killall -s INT perf");
//    printf("perf: %s\n", command);

//    int res = system(command);

    sprintf(command, "sudo pkill --signal SIGHUP -f pcm-memory");

    res &= system(command);
    sleep(1);


    return res;
}


void log_print_pmem_bandwidth(const char *perf_fn, double elapsed, FILE *f) {

    (void) perf_fn;

    uint64_t pmem_read;
    uint64_t pmem_write;
    uint64_t dram_read;
    uint64_t dram_write;

    int scanned_channel;

    while (true) {

        scanned_channel = 0;

        FILE *file = fopen("/mnt/sdb/xiaoxiang/pcm.txt", "r");
        pmem_read = 0;
        pmem_write = 0;
        dram_read = 0;
        dram_write = 0;

        char buffer[256];
        int is_first_line = 1;
        while (fgets(buffer, 256, file) != nullptr) {

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


    printf("PR: ");
    printf("%7.2fgb ", read_gb);
    printf("%7.2fgb/s ", read_bw);

    printf("PW: ");
    printf("%7.2fgb ", write_gb);
    printf("%7.2fgb/s ", write_bw);

    printf("\n");

//    printf("elapsed: %.2f ", elapsed);

    printf("DR: ");
    printf("%7.2fgb ", dram_read_gb);
    printf("%7.2fgb/s ", dram_read_bw);

    printf("DW: ");
    printf("%7.2fgb ", dram_write_gb);
    printf("%7.2fgb/s ", dram_write_bw);

    printf("\n");


    if (f != nullptr) {
        fprintf(f, "%.2f,%.2f,%.2f,%.2f,", read_gb, read_bw, write_gb, write_bw);
        fprintf(f, "%.2f,%.2f,%.2f,%.2f,", dram_read_gb, dram_read_bw, dram_write_gb, dram_write_bw);
    }

}