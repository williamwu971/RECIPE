#include <iostream>
#include <chrono>
#include <random>
#include <libpmemobj.h>
#include <fstream>
#include <set>
//#include "tbb/tbb.h"
#include "plog.cpp"

int (*which_memalign)(void **memptr, size_t alignment, size_t size) = posix_memalign;

void (*which_memfree)(void *ptr) =free;


int use_log = 0;
int total_size = 0;
int memset_size = 0;
int base_size = 0;
char *prefix = nullptr;
uint64_t iter;
uint64_t value_offset = 0;
uint64_t num_key;
int num_thread;
pthread_attr_t *ordered_attrs = nullptr;

struct rdtimes {
    uint64_t tree_time;
    uint64_t alloc_time;
    uint64_t free_time;
    uint64_t free_persist_time;
    uint64_t value_write_time;
    uint64_t value_read_time;
    uint64_t sum_time;
    uint64_t scan_memset_time;
};

struct functions {
    void
    (*update_func)(masstree::masstree *, MASS::ThreadInfo, uint64_t, uint64_t, int, void *, struct rdtimes *) = nullptr;

    void
    (*delete_func)(masstree::masstree *, MASS::ThreadInfo, uint64_t, struct rdtimes *) = nullptr;

    void
    (*lookup_func)(masstree::masstree *, MASS::ThreadInfo, uint64_t, uint64_t, int, struct rdtimes *) = nullptr;

    void
    (*scan_func)(masstree::masstree *, MASS::ThreadInfo, uint64_t, int, struct rdtimes *) = nullptr;
};

struct functions fs;

void *(*cpy_persist)(void *, const void *, size_t) = nullptr;

PMEMobjpool *pop = nullptr;

POBJ_LAYOUT_BEGIN(masstree);
POBJ_LAYOUT_TOID(masstree,
struct masstree_obj)

POBJ_LAYOUT_END(masstree)

struct masstree_obj {
//    TOID(struct masstree_obj) objToid;
    PMEMoid ht_oid;
    uint64_t data;
};

using namespace std;

#include "masstree.h"
#include "ralloc.hpp"
#include "pfence_util.h"

inline int RP_memalign(void **memptr, size_t alignment, size_t size) {

    *memptr = RP_malloc(size + (alignment - size % alignment));

    // todo: this can be removed
    // leaf is allocated one in a while so should be okay
    auto casted = (uint64_t)(*memptr);
    if (unlikely(casted % alignment != 0)) {
        throw;
    }

    return 0;
}

void *memcpy_then_persist(void *pmemdest, const void *src, size_t len) {
    memcpy(pmemdest, src, len);
    pmem_persist(pmemdest, len);
    return pmemdest;
}

void alignment_check(void *ptr) {
    auto number = (uint64_t) ptr;
    if (unlikely(number % 256 != 0)) {
        throw;
    }
}

__thread void *log_memcpy_prev_ptr = nullptr;
__thread uint64_t log_memcpy_prev_size = 0;

void *log_memcpy_then_persist(void *pmemdest, const void *src, size_t len) {

    memcpy(pmemdest, src, len);
    if (len % 256 == 0) {
        alignment_check(pmemdest);
        pmem_persist(pmemdest, len);
        return pmemdest;
    }

    /**
     * decide whether to persist or not
     * for small values only
     */

    if (log_memcpy_prev_ptr == nullptr) {
        log_memcpy_prev_ptr = pmemdest;
    }

    restart:
    if (
            ((char *) pmemdest)
            ==
            (((char *) log_memcpy_prev_ptr) + log_memcpy_prev_size)
            ) {

        uint64_t new_len = (((char *) pmemdest) + len) - (char *) log_memcpy_prev_ptr;
        if (new_len > 256) {

            uint64_t to_flush = new_len / 256 * 256;
//            printf("to_flush %lu\n", to_flush);
            alignment_check(log_memcpy_prev_ptr);
            pmem_persist(log_memcpy_prev_ptr, to_flush);

            log_memcpy_prev_ptr = (char *) log_memcpy_prev_ptr + to_flush;
            log_memcpy_prev_size = new_len - to_flush;
        } else {
            log_memcpy_prev_size = new_len;
        }

    } else {
//        printf("persist due to vast location change %lu\n", log_memcpy_prev_size);
        pmem_persist(log_memcpy_prev_ptr, log_memcpy_prev_size);
        log_memcpy_prev_ptr = pmemdest;
        log_memcpy_prev_size = 0;
        goto restart;
    }

    return pmemdest;

}

void dump_latencies(const char *fn, u_int64_t *numbers, uint64_t length) {

    // prevent over-recording
    if (length > 2000000)length = 2000000;

    FILE *latency_file = fopen(fn, "w");
    for (uint64_t idx = 0; idx < length; idx++) {
        fprintf(latency_file, "%lu\n", numbers[idx]);
    }
    fclose(latency_file);
}

void dump_rdtimes(const char *fn, struct rdtimes timing, int num_threads) {

    double tree_time;
    double alloc_time;
    double free_time;
    double free_persist_time;
    double value_write_time;
    double value_read_time;
    double sum_time;
    double scan_memset_time;

    // todo: freq is fixed here
    tree_time = (double) timing.tree_time / (double) num_threads / (double) 2000000000.0;
    alloc_time = (double) timing.alloc_time / (double) num_threads / (double) 2000000000.0;
    free_time = (double) timing.free_time / (double) num_threads / (double) 2000000000.0;
    free_persist_time = (double) timing.free_persist_time / (double) num_threads / (double) 2000000000.0;
    value_write_time = (double) timing.value_write_time / (double) num_threads / (double) 2000000000.0;
    value_read_time = (double) timing.value_read_time / (double) num_threads / (double) 2000000000.0;
    sum_time = (double) timing.sum_time / (double) num_threads / (double) 2000000000.0;
    scan_memset_time = (double) timing.scan_memset_time / (double) num_threads / (double) 2000000000.0;

    FILE *rdtimes_file = fopen(fn, "w");

    fprintf(rdtimes_file, "tree_time,%.2f,\n", tree_time);
    fprintf(rdtimes_file, "alloc_time,%.2f,\n", alloc_time);
    fprintf(rdtimes_file, "free_time,%.2f,\n", free_time);
//    fprintf(rdtimes_file, "free_persist_time,%.2f,\n", free_persist_time);
    fprintf(rdtimes_file, "value_write_time,%.2f,\n", value_write_time);
//    fprintf(rdtimes_file, "value_read_time,%.2f,\n", value_read_time);
    fprintf(rdtimes_file, "sum_time,%.2f,\n", sum_time);
//    fprintf(rdtimes_file, "scan_memset_time,%.2f,\n", scan_memset_time);

    fclose(rdtimes_file);
}

struct section_arg {
    uint64_t start;
    uint64_t end;
    masstree::masstree *tree;
    uint64_t *keys;
    uint64_t *rands;
    u_int64_t *latencies;
};

//std::vector<uint64_t> ycsb_init_keys;
std::vector <uint64_t> ycsb_keys;
std::vector<int> ycsb_ranges;
std::vector<int> ycsb_ops;

char *wl = nullptr;

enum {
    OP_INSERT,
    OP_UPDATE,
    OP_READ,
    OP_SCAN,
    OP_DELETE,
};


static long items; //initialized in init_zipf_generator function
static long base; //initialized in init_zipf_generator function
static double zipfianconstant; //initialized in init_zipf_generator function
static double alpha; //initialized in init_zipf_generator function
static double zetan; //initialized in init_zipf_generator function
static double eta; //initialized in init_zipf_generator function
static double theta; //initialized in init_zipf_generator function
static double zeta2theta; //initialized in init_zipf_generator function
static long countforzeta; //initialized in init_zipf_generator function
static unsigned int __thread seed;

double zetastatic(long st, long zn, double initialsum) {
    double sum = initialsum;
    for (long i = st; i < zn; i++) {
        sum += 1 / (pow(i + 1, theta));
    }
    return sum;
}

double zeta(long st, long zn, double initialsum) {
    countforzeta = zn;
    return zetastatic(st, zn, initialsum);
}

long next_long(long itemcount) {
    //from "Quickly Generating Billion-Record Synthetic Databases", Jim Gray et al, SIGMOD 1994
    if (itemcount != countforzeta) {
        if (itemcount > countforzeta) {
            printf("WARNING: Incrementally recomputing Zipfian distribtion. (itemcount= %ld; countforzeta= %ld)",
                   itemcount, countforzeta);
            //we have added more items. can compute zetan incrementally, which is cheaper
            zetan = zeta(countforzeta, itemcount, zetan);
            eta = (1 - pow(2.0 / (double) items, 1 - theta)) / (1 - zeta2theta / zetan);
        }
    }

    double u = (double) (rand_r(&seed) % RAND_MAX) / ((double) RAND_MAX);
    double uz = u * zetan;
    if (uz < 1.0) {
        return base;
    }

    if (uz < 1.0 + pow(0.5, theta)) {
        return base + 1;
    }
    long ret = base + (long) (((double) itemcount) * pow(eta * u - eta + 1, alpha));
    return ret;
}

long zipf_next() {
    return next_long(items);
}

/* Uniform */
long uniform_next() {
    return rand_r(&seed) % items;
}

void init_zipf_generator(long min, long max) {
    items = max - min + 1;
    base = min;
    zipfianconstant = 0.99;
    theta = zipfianconstant;
    zeta2theta = zeta(0, 2, 0);
    alpha = 1.0 / (1.0 - theta);
    zetan = zetastatic(0, max - min + 1, 0);
    countforzeta = items;
    eta = (1 - pow(2.0 / (double) items, 1 - theta)) / (1 - zeta2theta / zetan);

    zipf_next();
}

int random_get_put(int test) {
    long random = uniform_next() % 100;
    switch (test) {
        case 0: // A
            return random >= 50;
        case 1: // B
            return random >= 95;
        case 2: // C
            return 0;
        case 3: // E
            return random >= 95;
        default:
            die("Not a valid test\n");
    }

}

void bap_ycsb_load() {


    int test = 0;
    int zipfian = 0;

    if (wl[0] == 'a') {
        test = 0;
    } else if (wl[0] == 'b') {
        test = 1;
    } else if (wl[0] == 'c') {
        test = 2;
    } else if (wl[0] == 'e') {
        test = 3;
    }


    if (wl[1] == 'z') {
        zipfian = 1;
    }

    seed = time(nullptr);
    init_zipf_generator(0, (long) num_key - 1);
    long (*rand_next)() = zipfian ? zipf_next : uniform_next;


//    ycsb_init_keys.reserve(num_key);
    ycsb_keys.reserve(num_key);
    ycsb_ranges.reserve(num_key);
    ycsb_ops.reserve(num_key);

//    memset(&ycsb_init_keys[0], 0x00, num_key * sizeof(uint64_t));
    memset(&ycsb_keys[0], 0x00, num_key * sizeof(uint64_t));
    memset(&ycsb_ranges[0], 0x00, num_key * sizeof(int));
    memset(&ycsb_ops[0], 0x00, num_key * sizeof(int));

    uint64_t count = 0;
    while ((count < num_key)) {

//        uint64_t key = rand_next();
//        ycsb_init_keys.push_back(key);
        count++;
    }

    fprintf(stderr, "Loaded %lu keys\n", count);


    uint64_t count_OP_INSERT = 0;
    uint64_t count_OP_UPDATE = 0;
    uint64_t count_OP_READ = 0;
    uint64_t count_OP_SCAN = 0;

    count = 0;
    while ((count < num_key)) {

        long key = rand_next();
        int op_type = random_get_put(test);

        if (op_type) {
            ycsb_ops.push_back(OP_UPDATE);
            ycsb_keys.push_back(key);
            ycsb_ranges.push_back(1);
            count_OP_UPDATE++;
        } else {

            if (test != 3) {
                ycsb_ops.push_back(OP_READ);
                ycsb_keys.push_back(key);
                ycsb_ranges.push_back(1);
                count_OP_READ++;
            } else {
                int range = (int) uniform_next() % 99 + 1;
                ycsb_ops.push_back(OP_SCAN);
                ycsb_keys.push_back(key);
                ycsb_ranges.push_back(range);
                count_OP_SCAN++;
            }


        }
        count++;
    }

    printf("\nINSERT: %lu %5.2f \n", count_OP_INSERT, (double) count_OP_INSERT / (double) count * 100.0f);
    printf("UPDATE: %lu %5.2f \n", count_OP_UPDATE, (double) count_OP_UPDATE / (double) count * 100.0f);
    printf("READ  : %lu %5.2f \n", count_OP_READ, (double) count_OP_READ / (double) count * 100.0f);
    printf("SCAN  : %lu %5.2f \n", count_OP_SCAN, (double) count_OP_SCAN / (double) count * 100.0f);

}

uint64_t masstree_getsum(void *value) {

    auto numbers = (uint64_t *) value;
    uint64_t sum = 0;

    for (uint64_t i = 0; i < iter + 1; i++) {
        sum += numbers[i];
    }

    return sum;
}

pthread_mutex_t ralloc_recover_stats_lock = PTHREAD_MUTEX_INITIALIZER;
uint64_t ralloc_leafs = 0;
uint64_t ralloc_recovered = 0;
uint64_t ralloc_abandoned = 0;
uint64_t ralloc_total_time_tree = 0;
uint64_t ralloc_total_time_read = 0;
uint64_t ralloc_total_time_meta = 0;

uint64_t log_clam_time = 0;
uint64_t log_free_calc_time = 0;

struct ralloc_ptr_list {
    void *ptr;
    struct ralloc_ptr_list *next;
};

void ralloc_ptr_list_add(struct ralloc_ptr_list **head, void *ptr) {

    auto new_node = (struct ralloc_ptr_list *) malloc(sizeof(struct ralloc_ptr_list));

    new_node->ptr = ptr;
    new_node->next = *head;
    *head = new_node;
}

void *ralloc_ptr_list_pop(struct ralloc_ptr_list **head) {


    struct ralloc_ptr_list *old_head = *head;
    if (old_head == nullptr)return nullptr;

    *head = old_head->next;
    void *value = old_head->ptr;

    free(old_head);
    return value;
}

void *ralloc_recover_scan_thread(void *raw) {

    auto tree = (masstree::masstree *) raw;
    auto t = tree->getThreadInfo();

    uint64_t valid = 0;
    uint64_t invalid = 0;

    uint64_t time_tree = 0;
    uint64_t time_read = 0;
    uint64_t time_meta = 0;

    struct ralloc_ptr_list *ptrs = nullptr;

    while (true) {

        declearTSC

        struct RP_scan_pack pack = RP_scan_next();
        if (pack.curr == nullptr)break;
        if (pack.block_size < (uint32_t) total_size) throw;

        while (pack.curr < pack.end) {

            startTSC
            void *res = masstree_checksum(pack.curr, SUM_LOG, 0, iter, 0);
            stopTSC(time_read)

            if (res != nullptr) {
                valid++;

                // record pointer
                startTSC
                ralloc_ptr_list_add(&ptrs, pack.curr);
                stopTSC(time_meta)


                startTSC
                uint64_t key = ((uint64_t *) pack.curr)[1];
                void *returned = tree->put_and_return(key, pack.curr, 1, 0, t);
                stopTSC(time_tree)


                if (returned != nullptr) {
                    puts("detected replacing keys");
                    throw;
                }

            } else {
                invalid++;
            }
            pack.curr += pack.block_size;
        }

    }
//    printf("valid: %lu scanned: %fgb\n", valid, (double) scanned_bytes / 1024.0 / 1024.0 / 1024.0);
    pthread_mutex_lock(&ralloc_recover_stats_lock);
    ralloc_recovered += valid;
    ralloc_abandoned += invalid;
    ralloc_total_time_meta += time_meta;
    ralloc_total_time_read += time_read;
    ralloc_total_time_tree += time_tree;
    pthread_mutex_unlock(&ralloc_recover_stats_lock);

    return ptrs;
}

void ralloc_recover_scan(masstree::masstree *tree) {

    const char *section_name = "ralloc_recover_scan";
    char perf_fn[256];
    sprintf(perf_fn, "%s-%s.perf", prefix == nullptr ? "" : prefix, section_name);

    auto threads = (pthread_t *) calloc(num_thread, sizeof(pthread_t));
    auto ptr_lists = (struct ralloc_ptr_list **) calloc(num_thread, sizeof(struct ralloc_ptr_list *));
    RP_scan_init();

    log_start_perf(perf_fn);
    auto starttime = std::chrono::system_clock::now();

    for (int i = 0; i < num_thread; i++) {
        pthread_create(threads + i, ordered_attrs + i, ralloc_recover_scan_thread, tree);
    }

    for (int i = 0; i < num_thread; i++) {
        pthread_join(threads[i], (void **) (ptr_lists + i));
    }

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);


    log_stop_perf();
    log_print_pmem_bandwidth(perf_fn, (double) duration.count() / 1000000.0, nullptr);


    printf("Throughput: %s,%ld,%.2f ops/us %.2f sec\n",
           section_name, ralloc_recovered, ((double) ralloc_recovered * 1.0) / (double) duration.count(),
           (double) duration.count() / 1000000.0);



    // push pointers to ralloc's list single threaded
    starttime = std::chrono::system_clock::now();

    uint64_t meta_here = 0;

    declearTSC
    startTSC

    for (int i = 0; i < num_thread; i++) {
        struct ralloc_ptr_list *curr = ptr_lists[i];

        while (curr != nullptr) {

            RP_recover_xiaoxiang_insert(curr->ptr);

            struct ralloc_ptr_list *next = curr->next;
            free(curr);
            curr = next;
        }
    }

    stopTSC(meta_here)

    duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);


    printf("Throughput: %s,%ld,%.2f ops/us %.2f sec\n",
           "set-insert", ralloc_recovered, ((double) ralloc_recovered * 1.0) / (double) duration.count(),
           (double) duration.count() / 1000000.0);


    starttime = std::chrono::system_clock::now();
    startTSC
    RP_recover_xiaoxiang_go();
    stopTSC(meta_here)
    duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);


    printf("Throughput: %s,%ld,%.2f ops/us %.2f sec\n",
           "ralloc-build", ralloc_recovered, ((double) ralloc_recovered * 1.0) / (double) duration.count(),
           (double) duration.count() / 1000000.0);


    printf("recovered: %lu abandoned: %lu\n", ralloc_recovered, ralloc_abandoned);

    printf("time slides:\n");
    printf("read: %.2fs\n", (double) ralloc_total_time_read / (double) num_thread / 2000000000.0);
    printf("tree: %.2fs\n", (double) ralloc_total_time_tree / (double) num_thread / 2000000000.0);
    printf("meta: %.2fs\n", (double) ralloc_total_time_meta / (double) num_thread / 2000000000.0
                            + (double) meta_here / 2000000000.0);

    puts("");

}

#define REACH_T (LEAF_WIDTH+1)

void *ralloc_reachability_scan_thread(void *raw) {

    if (raw == nullptr) {
        printf("thread %lu returning\n", pthread_self());
        return nullptr;
    }

    struct ralloc_ptr_list *pointers = nullptr;
    uint64_t recovered_values = 0;
    uint64_t recovered_leafs = 0;
    uint64_t log_here = 0;
    declearTSC

    struct ralloc_ptr_list *to_visit = nullptr;
    ralloc_ptr_list_add(&to_visit, raw);

    while (true) {

        auto curr = (masstree::leafnode *) ralloc_ptr_list_pop(&to_visit);
        if (curr == nullptr) break;

        ralloc_ptr_list_add(&pointers, curr);
        recovered_leafs++;

        if (curr->level() != 0) {

            masstree::leafnode *target;

            for (int kv_idx = 0; kv_idx < REACH_T; kv_idx++) {

                if (kv_idx < LEAF_WIDTH) {
                    target = (masstree::leafnode *) curr->value(kv_idx);
                } else {
                    target = curr->leftmost();
                }

                if (target == nullptr)continue;

                bool visited = target->visited.load();
                if (!visited && target->visited.compare_exchange_strong(visited, true)) {
                    ralloc_ptr_list_add(&to_visit, target);
                }

            }


        } else {
            for (int kv_idx = 0; kv_idx < LEAF_WIDTH; kv_idx++) {

                void *v = curr->value(kv_idx);

                if (v != nullptr) {
                    if (use_log) {
                        startTSC
                        log_rebuild_claim(v);
                        stopTSC(log_here)
                    } else {
                        ralloc_ptr_list_add(&pointers, v);
                    }

                    recovered_values++;
                }

            }
        }
    }


    pthread_mutex_lock(&ralloc_recover_stats_lock);
    ralloc_recovered += recovered_values;
    ralloc_leafs += recovered_leafs;
    log_clam_time += log_here;
//    ralloc_abandoned += invalid;
    pthread_mutex_unlock(&ralloc_recover_stats_lock);

    return pointers;
}


void ralloc_reachability_scan(masstree::masstree *tree) {

    puts("\tbegin Ralloc reachability scan");

    printf("tree root: %p\n", tree->root());
    auto root = (masstree::leafnode *) tree->root();

    const char *section_name = "ralloc_reachability_scan";
    char perf_fn[256];
    sprintf(perf_fn, "%s-%s.perf", prefix == nullptr ? "" : prefix, section_name);

    auto threads = (pthread_t *) calloc(REACH_T, sizeof(pthread_t));
    auto ptr_lists = (struct ralloc_ptr_list **) calloc(REACH_T, sizeof(struct ralloc_ptr_list *));
    RP_scan_init();

    log_start_perf(perf_fn);
    auto starttime = std::chrono::system_clock::now();

    for (int i = 0; i < REACH_T; i++) {

        void *curr_arg = nullptr;

        if (i < LEAF_WIDTH) {
            curr_arg = root->value(i);
        } else {
            curr_arg = root->leftmost();
        }

        if (curr_arg == nullptr) {
            printf("index %d is nullptr\n", i);
            continue;
        }

        pthread_create(threads + i, ordered_attrs + i, ralloc_reachability_scan_thread, curr_arg);

    }

    for (int i = 0; i < REACH_T; i++) {
        if (threads[i] != 0) {
            pthread_join(threads[i], (void **) (ptr_lists + i));
        }
    }

    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);


    log_stop_perf();
    log_print_pmem_bandwidth(perf_fn, (double) duration.count() / 1000000.0, nullptr);

    double log_claim_casted = (double) log_clam_time / (double) REACH_T / 2000000000.0;

    printf("Throughput: %s,%ld,%.2f ops/us %.2f sec\n",
           section_name, ralloc_recovered, ((double) ralloc_recovered * 1.0) / (double) duration.count(),
           (double) duration.count() / 1000000.0 - log_claim_casted);

    declearTSC
    startTSC
    log_rebuild_compute_free();
    stopTSC(log_free_calc_time)

    double log_calc_casted = (double) log_free_calc_time / (double) REACH_T / 2000000000.0;

    printf("log times: %.2fs %.2fs\n", log_claim_casted, log_calc_casted);

    // push pointers to ralloc's list single threaded
    starttime = std::chrono::system_clock::now();

    RP_recover_xiaoxiang_insert(root);

    for (int i = 0; i < REACH_T; i++) {
        struct ralloc_ptr_list *curr = ptr_lists[i];

        while (curr != nullptr) {

            RP_recover_xiaoxiang_insert(curr->ptr);

            struct ralloc_ptr_list *next = curr->next;
            free(curr);
            curr = next;
        }
    }

    duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);


    printf("Throughput: %s,%ld,%.2f ops/us %.2f sec\n",
           "set-insert", ralloc_recovered, ((double) ralloc_recovered * 1.0) / (double) duration.count(),
           (double) duration.count() / 1000000.0);
    puts("");


    starttime = std::chrono::system_clock::now();
    RP_recover_xiaoxiang_go();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);

    puts("");
    printf("Throughput: %s,%ld,%.2f ops/us %.2f sec\n",
           "ralloc-build", ralloc_recovered, ((double) ralloc_recovered * 1.0) / (double) duration.count(),
           (double) duration.count() / 1000000.0);


    printf("recovered: %lu leafs: %lu abandoned: %lu\n", ralloc_recovered, ralloc_leafs, ralloc_abandoned);

}

int ralloc_extra = 0;
//__thread void *ralloc_free_list[4] = {NULL, NULL, NULL, NULL};
//__thread int ralloc_free_idx = 0;
//
//void masstree_ralloc_cross_update(masstree::masstree *tree,
//                                  MASS::ThreadInfo t,
//                                  uint64_t u_key,
//                                  uint64_t u_value,
//                                  int no_allow_prev_null,
//                                  void *tplate,
//                                  struct rdtimes *timing) {
//
//    declearTSC
//
//    startTSC
//    *((uint64_t *) tplate) = u_value;
//    if (ralloc_extra) {
//        ((uint64_t *) tplate)[1] = u_key;
//    }
//    if (!masstree_checksum(tplate, SUM_WRITE, u_value, iter, value_offset)) throw;
//    stopTSC(timing->sum_time)
//
////    startTSC
//    RP_malloc(total_size);
//    stopTSC(timing->alloc_time)
//
////    startTSC
//    if (ralloc_reuse == NULL) {
//        ralloc_reuse = RP_malloc(total_size);
//    }
//    cpy_persist(ralloc_reuse, tplate, total_size);
//    stopTSC(timing->value_write_time)
//
////    startTSC
//    ralloc_reuse = tree->put_and_return(u_key, ralloc_reuse, !no_allow_prev_null, 0, t);
//    stopTSC(timing->tree_time)
//
////    startTSC
//    int to_free = ralloc_free_idx - 3;
//    if (to_free < 0) to_free += 4;
//    void *free_me = ralloc_free_list[to_free];
//
//    if (free_me != NULL) {
//        RP_free(free_me);
//        if (ralloc_extra) {
//            pmem_persist(free_me, sizeof(void *));
//        }
//    }
//    stopTSC(timing->free_time)
//}

//__thread uint64_t *returned = nullptr;

void masstree_ralloc_update(masstree::masstree *tree,
                            MASS::ThreadInfo t,
                            uint64_t u_key,
                            uint64_t u_value,
                            int no_allow_prev_null,
                            void *tplate,
                            struct rdtimes *timing) {

    declearTSC

    startTSC
    *((uint64_t *) tplate) = u_value;
    if (ralloc_extra) {
        ((uint64_t *) tplate)[1] = u_key;
    }
    if (!masstree_checksum(tplate, SUM_WRITE, u_value, iter, value_offset)) throw;
    stopTSC(timing->sum_time)

//    startTSC
    void *value = RP_malloc(total_size);

//    void *value = returned;
//    if (value == nullptr)value = RP_malloc(total_size);
//    *(uint64_t*)value=*(uint64_t*)tplate;
    stopTSC(timing->alloc_time)

//    startTSC
//    cpy_persist((uint64_t *)value+1,(uint64_t*) tplate+1, total_size-sizeof(uint64_t));
    cpy_persist(value, tplate, total_size);
    stopTSC(timing->value_write_time)

//    startTSC
    auto returned = (uint64_t *) tree->put_and_return(u_key, value, !no_allow_prev_null, 0, t);
    stopTSC(timing->tree_time)

    //    startTSC
    if (no_allow_prev_null || returned != nullptr) {

//        if (returned[0] == 0) {
//            throw;
//        }
//        stopTSC(timing->value_write_time)

//        RP_free(returned);

//        returned= nullptr;
//        if (ralloc_extra) {
//            pmem_persist(returned, sizeof(void *));
//        }
    }
    stopTSC(timing->free_time)

}

void masstree_ralloc_delete(
        masstree::masstree *tree,
        MASS::ThreadInfo t,
        uint64_t d_key,
        struct rdtimes *timing
) {

    declearTSC

    startTSC
    auto returned = (uint64_t *) tree->del_and_return(d_key, 0, 0, nullptr, t);
    stopTSC(timing->tree_time)


    startTSC
    RP_free(returned);
    if (ralloc_extra) {
        pmem_persist(returned, sizeof(void *));
    }
    stopTSC(timing->free_time)
}

void masstree_log_update(masstree::masstree *tree,
                         MASS::ThreadInfo t,
                         uint64_t u_key,
                         uint64_t u_value,
                         int no_allow_prev_null,
                         void *tplate,
                         struct rdtimes *timing) {

    declearTSC

    startTSC
    auto lc = (struct log_cell *) tplate;
    lc->key = u_key;
    rdtscll(lc->version)
    *((uint64_t * )(lc + 1)) = u_value;
    if (!masstree_checksum(tplate, SUM_WRITE, u_value, iter, value_offset)) throw;
    stopTSC(timing->sum_time)

//    startTSC
    char *raw = (char *) log_malloc(total_size);
    stopTSC(timing->alloc_time)

//    startTSC

    cpy_persist(raw, tplate, total_size);
    stopTSC(timing->value_write_time)


//    startTSC
    void *returned = tree->put_and_return(u_key, raw, !no_allow_prev_null, 0, t);
    stopTSC(timing->tree_time)


//    startTSC
    if (no_allow_prev_null || returned != nullptr) {
        log_free(returned);
    }
    stopTSC(timing->free_time)

}

void masstree_log_delete(
        masstree::masstree *tree,
        MASS::ThreadInfo t,
        uint64_t d_key,
        struct rdtimes *timing
) {

    declearTSC

    startTSC
    void *target = tree->del_and_return(d_key, 0, 0, log_get_tombstone, t);
    stopTSC(timing->tree_time)

    startTSC
    log_free(target);
    stopTSC(timing->free_time)

}

void masstree_obj_update(
        masstree::masstree *tree,
        MASS::ThreadInfo t,
        uint64_t u_key,
        uint64_t u_value,
        int no_allow_prev_null,
        void *tplate,
        struct rdtimes *timing
) {


    TX_BEGIN(pop)
    {

        declearTSC

        startTSC
        PMEMoid
        ht_oid = pmemobj_tx_alloc(total_size, TOID_TYPE_NUM(
        struct masstree_obj));
        stopTSC(timing->alloc_time)

        startTSC
        auto o = (struct masstree_obj *) tplate;
        o->data = u_value;
        o->ht_oid = ht_oid;
        if (!masstree_checksum(tplate, SUM_WRITE, u_value, iter, value_offset)) throw;
        stopTSC(timing->sum_time)

        startTSC
        pmemobj_tx_add_range(ht_oid, 0, total_size);
        auto mo = (struct masstree_obj *) pmemobj_direct(ht_oid);
        memcpy(mo, tplate, total_size);
        stopTSC(timing->value_write_time)


        startTSC
        auto old_obj = (struct masstree_obj *) tree->put_and_return(u_key, mo, !no_allow_prev_null, 0, t);
        stopTSC(timing->tree_time)

        startTSC
        if (no_allow_prev_null || old_obj != nullptr) {

            //todo: possibly here can use same trick as Ralloc


            pmemobj_tx_add_range(old_obj->ht_oid, sizeof(struct masstree_obj) + memset_size,
                                 sizeof(uint64_t));
            if (!masstree_checksum(old_obj, SUM_INVALID, u_value, iter, value_offset)) throw;
            pmemobj_tx_free(old_obj->ht_oid);
        }
        stopTSC(timing->free_time)

    }
    TX_ONABORT{
            throw;
    }
    TX_END
}

void masstree_obj_delete(
        masstree::masstree *tree,
        MASS::ThreadInfo t,
        uint64_t d_key,
        struct rdtimes *timing
) {

    declearTSC

    startTSC
    auto old_obj = (struct masstree_obj *) tree->del_and_return(d_key, 0, 0, nullptr, t);
    stopTSC(timing->tree_time)


    TX_BEGIN(pop)
    {

        startTSC
        pmemobj_tx_add_range(old_obj->ht_oid, sizeof(struct masstree_obj) + memset_size,
                             sizeof(uint64_t));
        if (!masstree_checksum(old_obj, SUM_INVALID, d_key, iter, value_offset))throw;
        pmemobj_tx_free(old_obj->ht_oid);
        stopTSC(timing->free_time)
    }
    TX_ONABORT{
            throw;
    }
    TX_END


}


void masstree_universal_lookup(
        masstree::masstree *tree,
        MASS::ThreadInfo t,
        uint64_t g_key,
        uint64_t g_value,
        int check_value,
        struct rdtimes *timing
) {

    declearTSC

    startTSC
    void *raw = tree->get(g_key, t);
    stopTSC(timing->tree_time)

    if (raw != nullptr || check_value) {

        if (raw == nullptr) {
            printf("key %lu returned nullptr\n", g_key);
            throw;
        }

        startTSC
        if (!masstree_checksum(raw, SUM_CHECK, g_value, iter, value_offset)) {

            printf("error key %lu value %lu pointer %p\n", g_key, g_value, raw);
            throw;
        }
        stopTSC(timing->value_read_time)
    }
}

void masstree_ycsb_lookup(
        masstree::masstree *tree,
        MASS::ThreadInfo t,
        uint64_t g_key,
        uint64_t g_value,
        int check_value,
        struct rdtimes *timing
) {

    (void) g_value;
    (void) check_value;
    declearTSC

    startTSC
    void *raw = tree->get(g_key, t);
    stopTSC(timing->tree_time)

    if (raw != nullptr) {

        startTSC
        if (masstree_getsum(raw) == 0) {
            printf("ycsb lookup sum 0\n");
            throw;
        }
        stopTSC(timing->value_read_time)
    }
}

void masstree_ycsb_scan(
        masstree::masstree *tree,
        MASS::ThreadInfo t,
        uint64_t s_min,
        int s_num,
        struct rdtimes *timing
) {

    declearTSC

    startTSC
    uint64_t buf[200];
    memset(buf, 0, sizeof(uint64_t) * 200);
    stopTSC(timing->scan_memset_time)


    startTSC
    int ret = tree->scan(s_min, s_num, buf, t);
    stopTSC(timing->tree_time)

    startTSC
    for (int ret_idx = 0; ret_idx < ret; ret_idx++) {

        void *raw = (void *) buf[ret_idx];

        if (raw != nullptr) {
            if (masstree_getsum(raw) == 0) {
                printf("ycsb scan sum 0\n");
                throw;
            }
        }
    }
    stopTSC(timing->value_read_time)
}


void *section_ycsb_run(void *arg) {


    auto sa = (struct section_arg *) arg;

    masstree::masstree *tree = sa->tree;
    uint64_t start = sa->start;
    uint64_t end = sa->end;
    u_int64_t *latencies = sa->latencies;

    void *tplate = malloc(total_size);
    memset(tplate, 7, total_size);

    if (use_log) {

        auto lc = (struct log_cell *) tplate;
        lc->value_size = total_size - sizeof(struct log_cell);
        lc->is_delete = 0;

    }

    auto t = tree->getThreadInfo();

    auto timing = (struct rdtimes *) calloc(1, sizeof(struct rdtimes));

    for (uint64_t i = start; i < end; i++) {

        if (ycsb_ops[i] == OP_INSERT || ycsb_ops[i] == OP_UPDATE) {
            fs.update_func(tree, t, ycsb_keys[i], ycsb_keys[i], 0, tplate, timing);
        } else if (ycsb_ops[i] == OP_READ) {
            fs.lookup_func(tree, t, ycsb_keys[i], ycsb_keys[i], 0, timing);
        } else if (ycsb_ops[i] == OP_SCAN) {
            fs.scan_func(tree, t, ycsb_keys[i], ycsb_ranges[i], timing);
        } else if (ycsb_ops[i] == OP_DELETE) {
            fs.delete_func(tree, t, ycsb_keys[i], timing);
        }

        if (start == 0) {
            latencies[i] = readTSC(1, 1);
        }
    }


    return timing;
}

void *section_insert(void *arg) {

    auto sa = (struct section_arg *) arg;


    uint64_t start = sa->start;
    uint64_t end = sa->end;
    masstree::masstree *tree = sa->tree;
    uint64_t *keys = sa->keys;
    uint64_t *rands = sa->rands;
    u_int64_t *latencies = sa->latencies;


    void *tplate = malloc(total_size);
    memset(tplate, 7, total_size);


    if (use_log) {

        auto lc = (struct log_cell *) tplate;
        lc->value_size = total_size - sizeof(struct log_cell);
        lc->is_delete = 0;


    }

    auto t = tree->getThreadInfo();

    auto timing = (struct rdtimes *) calloc(1, sizeof(struct rdtimes));

    for (uint64_t i = start; i < end; i++) {

        fs.update_func(tree, t, keys[i], rands[i], 0, tplate, timing);

        if (start == 0) {
            latencies[i] = readTSC(1, 1);
        }
    }


    return timing;
}

void *section_update(void *arg) {

    auto sa = (struct section_arg *) arg;


    uint64_t start = sa->start;
    uint64_t end = sa->end;
    masstree::masstree *tree = sa->tree;
    uint64_t *keys = sa->keys;
    u_int64_t *latencies = sa->latencies;

    void *tplate = malloc(total_size);
    memset(tplate, 7, total_size);

    if (use_log) {

        auto lc = (struct log_cell *) tplate;
        lc->value_size = total_size - sizeof(struct log_cell);
        lc->is_delete = 0;

    }

    auto t = tree->getThreadInfo();

    auto timing = (struct rdtimes *) calloc(1, sizeof(struct rdtimes));

    for (uint64_t i = start; i < end; i++) {

        fs.update_func(tree, t, keys[i], keys[i], 1, tplate, timing);

//        if (start == 0) {
//            latencies[i] = readTSC(1, 1);
//        }
    }


    return timing;
}

void *section_lookup(void *arg) {

    auto sa = (struct section_arg *) arg;


    uint64_t start = sa->start;
    uint64_t end = sa->end;
    masstree::masstree *tree = sa->tree;
    uint64_t *keys = sa->keys;
    u_int64_t *latencies = sa->latencies;


    auto t = tree->getThreadInfo();

    auto timing = (struct rdtimes *) calloc(1, sizeof(struct rdtimes));

    for (uint64_t i = start; i < end; i++) {

        fs.lookup_func(tree, t, keys[i], keys[i], 1, timing);

        if (start == 0) {
            latencies[i] = readTSC(1, 1);
        }
    }


    return timing;
}

void *section_delete(void *arg) {
    auto sa = (struct section_arg *) arg;


    uint64_t start = sa->start;
    uint64_t end = sa->end;
    masstree::masstree *tree = sa->tree;
    uint64_t *keys = sa->keys;
    u_int64_t *latencies = sa->latencies;


    auto t = tree->getThreadInfo();

    auto timing = (struct rdtimes *) calloc(1, sizeof(struct rdtimes));

    for (uint64_t i = start; i < end; i++) {

        fs.delete_func(tree, t, keys[i], timing);

        if (start == 0) {
            latencies[i] = readTSC(1, 1);
        }
    }


    return timing;
}

void masstree_shuffle(uint64_t *array, uint64_t size) {

    printf("shuffling array of size %lu ... ", size);
    fflush(stdout);

    srand(time(nullptr));

    for (uint64_t i = 0; i < size - 1; i++) {
        uint64_t j = i + rand() / (RAND_MAX / (size - i) + 1);
        uint64_t t = array[j];
        array[j] = array[i];
        array[i] = t;
    }
    printf("done! \n");
    fflush(stdout);
}

void run(
        const char *section_name,
        FILE *throughput_file,
        struct section_arg *section_args,
        u_int64_t *latencies,
        void *(*routine)(void *),
        int interfere
) {

    char perf_fn[256];
    sprintf(perf_fn, "%s-%s.perf", prefix == nullptr ? "" : prefix, section_name);

    auto threads = (pthread_t *) calloc(num_thread, sizeof(pthread_t));

    log_start_perf(perf_fn);

    // Build tree
    auto starttime = std::chrono::system_clock::now();

    for (int i = 0; i < num_thread; i++) {
        pthread_create(threads + i, ordered_attrs + i, routine, section_args + i);
    }

    struct rdtimes total_timing = {};
    memset(&total_timing, 0, sizeof(struct rdtimes));

    for (int i = 0; i < num_thread; i++) {
        struct rdtimes *local_timing;
        pthread_join(threads[i], (void **) &local_timing);

        total_timing.tree_time += local_timing->tree_time;
        total_timing.alloc_time += local_timing->alloc_time;
        total_timing.free_time += local_timing->free_time;
        total_timing.free_persist_time += local_timing->free_persist_time;
        total_timing.value_write_time += local_timing->value_write_time;
        total_timing.value_read_time += local_timing->value_read_time;
        total_timing.sum_time += local_timing->sum_time;
        total_timing.scan_memset_time += local_timing->scan_memset_time;


        free(local_timing);
    }


    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);
//    if (throughput_file != nullptr)log_debug_print(throughput_file, use_log);

    if (!interfere) {
        log_stop_perf();
        log_print_pmem_bandwidth(perf_fn, (double) duration.count() / 1000000.0, throughput_file);
    }

    log_wait_all_gc();
    auto duration_with_gc = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);
//    if (throughput_file != nullptr)log_debug_print(throughput_file, use_log);

    if (interfere) {
        log_stop_perf();
        log_print_pmem_bandwidth(perf_fn, (double) duration_with_gc.count() / 1000000.0, throughput_file);
    }


    printf("Throughput: %s,%ld,%.2f ops/us %.2f sec\n",
           section_name, num_key, ((double) num_key * 1.0) / (double) duration.count(),
           (double) duration.count() / 1000000.0);

//    sprintf(perf_fn, "%s-%s.rdtsc", prefix, section_name);
//    dump_latencies(perf_fn, latencies, section_args[0].end);

    sprintf(perf_fn, "%s-%s.rdtimes", prefix, section_name);
    dump_rdtimes(perf_fn, total_timing, num_thread);


    if (throughput_file != nullptr) {
        fprintf(throughput_file, "%.2f,%.2f,",
                ((double) num_key * 1.0) / (double) duration.count(),
                ((double) num_key * 1.0) / (double) duration_with_gc.count());
    }

    puts("");
}

int main(int argc, char **argv) {

    // init thread attributes
    cpu_set_t fcpu;
    CPU_ZERO(&fcpu);
    CPU_SET(0, &fcpu);
    pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &fcpu);

    int numberOfProcessors = (int) sysconf(_SC_NPROCESSORS_ONLN);
    ordered_attrs = (pthread_attr_t *) calloc(numberOfProcessors, sizeof(pthread_attr_t));
    for (int i = 0; i < numberOfProcessors; i++) {

        cpu_set_t cpu;
        CPU_ZERO(&cpu);

        // reserving CPU 0
        CPU_SET(i + 1, &cpu);

        pthread_attr_init(ordered_attrs + i);
        pthread_attr_setaffinity_np(ordered_attrs + i, sizeof(cpu_set_t), &cpu);
    }


    // control variables
    int require_RP_init = 0;
    int require_obj_init = 0;
    int num_of_gc = 0;
    uint64_t PMEM_POOL_SIZE = 0;
    int goto_lookup = 0; //
    int interfere = 1; //

    if (argc < 3) {
        printf("usage: %s [n] [nthreads]\nn: number of keys (integer)\nnthreads: number of threads (integer)\n",
               argv[0]);
        return 1;
    }
    for (int ac = 0; ac < argc; ac++) printf("%s ", argv[ac]);
    puts("");

    for (int ac = 0; ac < argc; ac++) {

        if (ac == 1) {
            num_key = strtol(argv[1], nullptr, 10);
            printf("n:%lu ", num_key);

        } else if (ac == 2) {
            num_thread = (int) strtol(argv[2], nullptr, 10);
            printf("num_thread:%d ", num_thread);

        } else if (strcasestr(argv[ac], "iv=")) {

            char *index_loc = strcasestr(argv[ac], "=") + 1;
            char *value_loc = strcasestr(argv[ac], "-");
            value_loc[0] = '\0';
            value_loc++;

            if (strcasestr(index_loc, "ralloc")) {
                which_memalign = RP_memalign;
                which_memfree = RP_free;
                require_RP_init = 1;
                printf("index=ralloc ");

            } else if (strcasestr(index_loc, "obj")) {
                which_memalign = masstree::obj_memalign;
                which_memfree = masstree::obj_free;
                require_obj_init = 1;
                printf("index=obj ");

            } else {
                printf("index=dram ");

            }


            fs.lookup_func = masstree_universal_lookup;

            if (strcasestr(value_loc, "ralloc")) {
                require_RP_init = 1;
                base_size = sizeof(uint64_t) * 2;

                if (which_memalign == posix_memalign) {
                    base_size += sizeof(uint64_t);
                    ralloc_extra = 1;
                }

                printf("value=ralloc ");

                fs.update_func = masstree_ralloc_update;
//                fs.update_func = masstree_ralloc_cross_update;
                fs.delete_func = masstree_ralloc_delete;

            } else if (strcasestr(value_loc, "log")) {
                use_log = 1;
                base_size = sizeof(struct log_cell) + sizeof(uint64_t) * 2;
                printf("value=log ");


                value_offset = sizeof(struct log_cell);
                assert(value_offset % sizeof(uint64_t) == 0);
                value_offset = value_offset / sizeof(uint64_t);

                fs.update_func = masstree_log_update;
                fs.delete_func = masstree_log_delete;

                if (strcasestr(value_loc, "best")) {
                    interfere = 0;
                }

                if (strcasestr(value_loc, "256")) {
                    cpy_persist = log_memcpy_then_persist;
                    printf("persist=log_256_flush ");
                }

                printf("interfere=%d ", interfere);

            } else if (strcasestr(value_loc, "obj")) {
                require_obj_init = 1;
                base_size = sizeof(struct masstree_obj) + sizeof(uint64_t);
                printf("value=obj ");


                struct masstree_obj obj_tmp = {};
                value_offset = ((char *) (&obj_tmp.data) - (char *) (&obj_tmp));
                assert(value_offset % sizeof(uint64_t) == 0);
                value_offset = value_offset / sizeof(uint64_t);

                fs.update_func = masstree_obj_update;
                fs.delete_func = masstree_obj_delete;

            } else {
                die("what?");
            }
        } else if (strcasestr(argv[ac], "gc=")) {
            if (use_log)num_of_gc = (int) strtol(strcasestr(argv[ac], "=") + 1, nullptr, 10);
            printf("gc=%d ", num_of_gc);

        } else if (strcasestr(argv[ac], "extra_size=")) {
            memset_size = (int) strtol(strcasestr(argv[ac], "=") + 1, nullptr, 10);
            printf("memset_size=%d ", memset_size);

        } else if (strcasestr(argv[ac], "total_size=")) {
            total_size = (int) strtol(strcasestr(argv[ac], "=") + 1, nullptr, 10);
            printf("total_size=%d ", total_size);

        } else if (strcasestr(argv[ac], "ycsb=")) {
            wl = strcasestr(argv[ac], "=") + 1;

            if (wl[0] != 'a' && wl[0] != 'b' && wl[0] != 'c' && wl[0] != 'd' && wl[0] != 'e') {
                wl = nullptr;
                continue;
            }

            fs.lookup_func = masstree_ycsb_lookup;
            fs.scan_func = masstree_ycsb_scan;

            printf("ycsb=%s ", wl);

//            ycsb_load();
            bap_ycsb_load();

        } else if (strcasestr(argv[ac], "prefix=")) {
            char *prefix_ptr = strcasestr(argv[ac], "=") + 1;
            prefix = (char *) malloc(sizeof(char) * (strlen(prefix_ptr) + 1));
            strcpy(prefix, prefix_ptr);
        } else if (strcasestr(argv[ac], "persist=") && cpy_persist == nullptr) {
            if (strcasestr(argv[ac], "flush")) {

                cpy_persist = memcpy_then_persist;
                printf("persist=flush ");

            } else {
                cpy_persist = pmem_memcpy_persist;
                printf("persist=non-temporal ");
            }
        }
    }

    if (total_size == 0) {
        total_size = memset_size + base_size;
    } else {

        if (total_size < base_size) total_size = base_size;

        memset_size = total_size - base_size;
    }


    iter = total_size / sizeof(uint64_t) - 1;
    printf("total_size=%d %lu uint64_t", total_size, iter);

    puts("");

    puts("\tbegin generating keys");
    auto keys = new uint64_t[num_key];
    auto rands = new uint64_t[num_key];


    // Generate keys
    srand(time(nullptr));
    for (uint64_t i = 0; i < num_key; i++) {
        rands[i] = rand();
    }

    // Generate keys
    for (uint64_t i = 0; i < num_key; i++) {
        keys[i] = i + 1;
    }


    PMEM_POOL_SIZE = total_size * num_key * 3;
    uint64_t size_round = 4ULL * 1024ULL * 1024ULL * 1024ULL;
    while (size_round < PMEM_POOL_SIZE) {
        size_round += 4ULL * 1024ULL * 1024ULL * 1024ULL;
    }
    PMEM_POOL_SIZE = size_round;


    masstree::masstree *tree = nullptr;

    if (use_log) {

        puts("\tbegin preparing Log");

        if (
//                access(INODE_FN, F_OK) == 0 &&
//                access(META_FN, F_OK) == 0 &&
                access(LOG_FN, F_OK) == 0

                ) {

            if (which_memalign == RP_memalign) {
                log_ralloc_recover();
            } else {
                tree = new masstree::masstree();
                log_recover(tree, num_thread);
            }

            goto_lookup = 1;
        } else {
            log_init(PMEM_POOL_SIZE);
        }
    }

    if (require_RP_init) {

        int preset = 0;

        puts("\tbegin preparing Ralloc");
        int should_recover = RP_init("masstree", PMEM_POOL_SIZE, &preset);

//        int should_recover=(access("/pmem0/masstree_sb", F_OK) != -1);
//        RP_init("masstree", PMEM_POOL_SIZE, &preset);

        if (should_recover && which_memalign == RP_memalign) {

            tree = new masstree::masstree(RP_get_root<masstree::leafnode>(0));
            ralloc_reachability_scan(tree);

//            throw;
            goto_lookup = 1;

        } else if (should_recover) {
            tree = new masstree::masstree();
            ralloc_recover_scan(tree);
            goto_lookup = 1;
        }
    }

    if (require_obj_init) {

        puts("\tbegin preparing Obj");

        // Enable prefault
        int arg_open = 1, arg_create = 1;
        if ((pmemobj_ctl_set(pop, "prefault.at_open", &arg_open)) != 0)
            perror("failed to configure prefaults at open\n");
        if ((pmemobj_ctl_set(pop, "prefault.at_create", &arg_create)) != 0)
            perror("failed to configure prefaults at create\n");


        if (access("/pmem0/masstree_obj", F_OK) != -1) {
            pop = pmemobj_open("/pmem0/masstree_obj", POBJ_LAYOUT_NAME(masstree));
        } else {
            pop = pmemobj_create("/pmem0/masstree_obj", POBJ_LAYOUT_NAME(masstree),
                                 PMEM_POOL_SIZE, 0666);
        }

        masstree::obj_init(pop);
    }

    if (tree == nullptr) {
        puts("\t creating new tree");
        tree = new masstree::masstree();
    }

    if (num_of_gc > 0) {
        puts("\tbegin creating Gc");

        int start_cpu;
        int end_cpu;

        if (interfere) {
            start_cpu = 1;
            end_cpu = 1 + num_thread;
            end_cpu = 1 + num_of_gc; // todo: delete to make update faster
        } else {
            start_cpu = 1 + num_thread;
            end_cpu = start_cpu + num_of_gc;
        }

        for (int gcc = 0; gcc < num_of_gc; gcc++) {
            log_start_gc(tree, start_cpu, end_cpu);
        }
    }

//    tbb::task_scheduler_init init(num_thread);

    FILE *throughput_file = fopen("perf.csv", "a");
    u_int64_t *latencies = nullptr;

    auto section_args = (struct section_arg *) calloc(num_thread, sizeof(struct section_arg));

    uint64_t n_per_thread = num_key / num_thread;
    uint64_t n_remainder = num_key % num_thread;


    for (int i = 0; i < num_thread; i++) {

        if (i == 0) {
            section_args[i].start = 0;
        } else {
            section_args[i].start = section_args[i - 1].end;
        }

//        section_args[i].start = i;
        section_args[i].end = section_args[i].start + n_per_thread;
        if (n_remainder > 0) {
            n_remainder--;
            section_args[i].end++;
        }

//        printf("thread %d from %lu to %lu\n", i, section_args[i].start, section_args[i].end - 1);

        section_args[i].tree = tree;

//        if (wl != nullptr) {
//            section_args[i].keys = ycsb_init_keys.data();
//            section_args[i].rands = ycsb_init_keys.data();
//        } else {
        section_args[i].keys = keys;
        section_args[i].rands = rands;
//        }

        if (i == 0) {
            latencies = (u_int64_t *) malloc(sizeof(u_int64_t) * section_args[i].end);
            section_args[i].latencies = latencies;
        } else {
            section_args[i].latencies = nullptr;
        }

    }


    std::cout << "Simple Example of P-Masstree-New" << std::endl;
    printf("operation,n,ops/s\n");

    if (goto_lookup) goto lookup;

    {
        /**
         * section YCSB
         */
        if (wl != nullptr) {
            puts("\t\t\t *** YCSB workload ***");
//            run("ycsb_load", throughput_file, section_args, latencies, section_ycsb_load,interfere);
            masstree_shuffle(keys, num_key);
            run("ycsb_load", throughput_file, section_args, latencies, section_insert, interfere);
            run("ycsb_run", throughput_file, section_args, latencies, section_ycsb_run, interfere);
            goto end;
        }
    }

    {
        /**
         * section INSERT
         */
        masstree_shuffle(keys, num_key);
        run("insert", throughput_file, section_args, latencies, section_insert, interfere);
    }

//    printf("count RP_MALLOC %lu\n", RP_lock_count);

    {
        /**
         * section UPDATE
         */
        masstree_shuffle(keys, num_key);
        run("update", throughput_file, section_args, latencies, section_update, interfere);
    }


    lookup:
    {
        /**
         * section LOOKUP
         */
//        masstree_shuffle(keys, num_key);
//        run("lookup", throughput_file, section_args, latencies, section_lookup, interfere);
    }


    if (which_memalign == RP_memalign) {
        RP_set_root(tree->root(), 0);
        printf("RP set root: %p\n", tree->root());
    }

    {
        /**
         * section DELETE
         */
//        throw;
//        masstree_shuffle(keys, num_key);
//        run("delete", throughput_file, section_args, latencies, section_delete, interfere);
    }


    end:

    // logging throughput to files
    fclose(throughput_file);


    if (use_log) {
        log_join_all_gc();
    }


    delete[] keys;

    return 0;
}

