#include <iostream>
#include <chrono>
#include <random>
#include <libpmemobj.h>
#include <fstream>
//#include "tbb/tbb.h"
#include "plog.cpp"

//#define PMEM_POOL_SIZE (4*1024*1024*1024ULL)
//#define FOOTER 0xdeadbeef

// todo: make templates/cpp (modular) <- important
int (*which_memalign)(void **memptr, size_t alignment, size_t size) = posix_memalign;

void (*which_memfree)(void *ptr) =free;

int require_RP_init = 0;
int require_log_init = 0;
int require_obj_init = 0;
int shuffle_keys = 0;
int use_perf = 0;
int num_of_gc = 0;
int show_log_usage = 1;
int record_latency = 0;
int display_throughput = 1;
int use_obj = 0;
int use_ralloc = 0;
int use_log = 0;
int total_size = 0;
int memset_size = 0;
int base_size = 0;
char *prefix = NULL;
uint64_t iter;
uint64_t PMEM_POOL_SIZE = 0;
uint64_t value_offset = 0;

uint64_t n;
int num_thread;
PMEMobjpool *pop = NULL;

POBJ_LAYOUT_BEGIN(masstree);
POBJ_LAYOUT_TOID(masstree, struct masstree_obj)

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
    return 0;
}

//static constexpr uint64_t CACHE_LINE_SIZE = 64;

static inline void clflush(char *data, int len, bool front, bool back) {
    volatile char *ptr = (char *) ((unsigned long) data & ~(CACHE_LINE_SIZE - 1));
    if (front)
        asm volatile("sfence":: :"memory");
    for (; ptr < data + len; ptr += CACHE_LINE_SIZE) {
#ifdef CLFLUSH
        asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
#elif CLFLUSH_OPT
        asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *) (ptr)));
#elif CLWB
        //        asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *) (ptr)));
                FLUSH(ptr);
#endif
    }
    if (back)
        asm volatile("sfence":: :"memory");
}


void dump_latencies(const char *fn, u_int64_t *numbers, uint64_t length) {
    FILE *latency_file = fopen(fn, "w");
    for (uint64_t idx = 0; idx < length; idx++) {
        fprintf(latency_file, "%lu\n", numbers[idx]);
    }
    fclose(latency_file);
}

struct section_arg {
    uint64_t start;
    uint64_t end;
    masstree::masstree *tree;
    uint64_t *keys;
    uint64_t *rands;
    u_int64_t *latencies;
};


static uint64_t YCSB_SIZE = 64000000;
//static uint64_t YCSB_SIZE = 64000000;

std::vector<uint64_t> ycsb_init_keys;
std::vector<uint64_t> ycsb_keys;
std::vector<int> ycsb_ranges;
std::vector<int> ycsb_ops;

char *wl = NULL;

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
            eta = (1 - pow(2.0 / items, 1 - theta)) / (1 - zeta2theta / zetan);
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
    long ret = base + (long) ((itemcount) * pow(eta * u - eta + 1, alpha));
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
    eta = (1 - pow(2.0 / items, 1 - theta)) / (1 - zeta2theta / zetan);

    zipf_next();
}

static int random_get_put(int test) {
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
    }
    die("Not a valid test\n");
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

    init_zipf_generator(0, YCSB_SIZE - 1);
    long (*rand_next)(void) = zipfian ? zipf_next : uniform_next;


    std::string insert("INSERT");
    std::string update("UPDATE");
    std::string read("READ");
    std::string scan("SCAN");

    ycsb_init_keys.reserve(YCSB_SIZE);
    ycsb_keys.reserve(YCSB_SIZE);
    ycsb_ranges.reserve(YCSB_SIZE);
    ycsb_ops.reserve(YCSB_SIZE);

    memset(&ycsb_init_keys[0], 0x00, YCSB_SIZE * sizeof(uint64_t));
    memset(&ycsb_keys[0], 0x00, YCSB_SIZE * sizeof(uint64_t));
    memset(&ycsb_ranges[0], 0x00, YCSB_SIZE * sizeof(int));
    memset(&ycsb_ops[0], 0x00, YCSB_SIZE * sizeof(int));

    uint64_t count = 0;
    while ((count < YCSB_SIZE)) {

        uint64_t key = rand_next();
        ycsb_init_keys.push_back(key);
        count++;
    }

    fprintf(stderr, "Loaded %lu keys\n", count);


    uint64_t count_OP_INSERT = 0;
    uint64_t count_OP_UPDATE = 0;
    uint64_t count_OP_READ = 0;
    uint64_t count_OP_SCAN = 0;

    count = 0;
    while ((count < YCSB_SIZE)) {

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
                int range = uniform_next() % 99 + 1;
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

void ycsb_load() {


    std::string init_file;
    std::string txn_file;


    if (strcmp(wl, "a") == 0) {
        init_file = "../../index-microbench/workloads/loada_unif_int.dat";
        txn_file = "../../index-microbench/workloads/txnsa_unif_int.dat";
    } else if (strcmp(wl, "b") == 0) {
        init_file = "../../index-microbench/workloads/loadb_unif_int.dat";
        txn_file = "../../index-microbench/workloads/txnsb_unif_int.dat";
    } else if (strcmp(wl, "c") == 0) {
        init_file = "../../index-microbench/workloads/loadc_unif_int.dat";
        txn_file = "../../index-microbench/workloads/txnsc_unif_int.dat";
    } else if (strcmp(wl, "d") == 0) {
        init_file = "../../index-microbench/workloads/loadd_unif_int.dat";
        txn_file = "../../index-microbench/workloads/txnsd_unif_int.dat";
    } else if (strcmp(wl, "e") == 0) {
        init_file = "../../index-microbench/workloads/loade_unif_int.dat";
        txn_file = "../../index-microbench/workloads/txnse_unif_int.dat";
    }


    std::ifstream infile_load(init_file);

    std::string op;
    uint64_t key;
    int range;

    std::string insert("INSERT");
    std::string update("UPDATE");
    std::string read("READ");
    std::string scan("SCAN");

    ycsb_init_keys.reserve(YCSB_SIZE);
    ycsb_keys.reserve(YCSB_SIZE);
    ycsb_ranges.reserve(YCSB_SIZE);
    ycsb_ops.reserve(YCSB_SIZE);

    memset(&ycsb_init_keys[0], 0x00, YCSB_SIZE * sizeof(uint64_t));
    memset(&ycsb_keys[0], 0x00, YCSB_SIZE * sizeof(uint64_t));
    memset(&ycsb_ranges[0], 0x00, YCSB_SIZE * sizeof(int));
    memset(&ycsb_ops[0], 0x00, YCSB_SIZE * sizeof(int));

    uint64_t count = 0;
    while ((count < YCSB_SIZE) && infile_load.good()) {
        infile_load >> op >> key;
        if (op.compare(insert) != 0) {
            std::cout << "READING LOAD FILE FAIL!\n";
            return;
        }
        ycsb_init_keys.push_back(key);
        count++;
    }

    fprintf(stderr, "Loaded %lu keys\n", count);

    std::ifstream infile_txn(txn_file);


    uint64_t count_OP_INSERT = 0;
    uint64_t count_OP_UPDATE = 0;
    uint64_t count_OP_READ = 0;
    uint64_t count_OP_SCAN = 0;

    count = 0;
    while ((count < YCSB_SIZE) && infile_txn.good()) {
        infile_txn >> op >> key;
        if (op.compare(insert) == 0) {
            ycsb_ops.push_back(OP_INSERT);
            ycsb_keys.push_back(key);
            ycsb_ranges.push_back(1);
            count_OP_INSERT++;
        } else if (op.compare(update) == 0) {
            ycsb_ops.push_back(OP_UPDATE);
            ycsb_keys.push_back(key);
            ycsb_ranges.push_back(1);
            count_OP_UPDATE++;
        } else if (op.compare(read) == 0) {
            ycsb_ops.push_back(OP_READ);
            ycsb_keys.push_back(key);
            ycsb_ranges.push_back(1);
            count_OP_READ++;
        } else if (op.compare(scan) == 0) {
            infile_txn >> range;
            ycsb_ops.push_back(OP_SCAN);
            ycsb_keys.push_back(key);
            ycsb_ranges.push_back(range);
            count_OP_SCAN++;
        } else {
            std::cout << "UNRECOGNIZED CMD!\n";
            return;
        }
        count++;
    }

    printf("\nINSERT: %lu %5.2f \n", count_OP_INSERT, (double) count_OP_INSERT / (double) count * 100.0f);
    printf("UPDATE: %lu %5.2f \n", count_OP_UPDATE, (double) count_OP_UPDATE / (double) count * 100.0f);
    printf("READ  : %lu %5.2f \n", count_OP_READ, (double) count_OP_READ / (double) count * 100.0f);
    printf("SCAN  : %lu %5.2f \n", count_OP_SCAN, (double) count_OP_SCAN / (double) count * 100.0f);

    std::atomic<int> range_complete, range_incomplete;
    range_complete.store(0);
    range_incomplete.store(0);
}

static inline int masstree_checksum(void *value, int check, uint64_t v) {

    uint64_t *numbers = (uint64_t *) value;
    uint64_t sum = 0;
    int check_result = 1;

    for (uint64_t i = 0; i < iter; i++) {
        sum += numbers[0];
        if (i == value_offset && numbers[0] != v) {
            check_result = 0;
            printf("value incorrect, offset %lu expecting %lu got %lu\n", value_offset, v, numbers[0]);
        }

        numbers++;
    }

    if (check && numbers[0] != sum) {
        check_result = 0;
        printf("sum incorrect, expecting %lu got %lu\n", sum, numbers[0]);
    } else {
        numbers[0] = sum;
    }

    return check_result;
}

static inline void masstree_branched_insert(
        masstree::masstree *tree,
        MASS::ThreadInfo t,
        uint64_t p_key,
        uint64_t p_value,
        void *tplate
) {


    if (use_obj) {

        void *v = NULL;
        TX_BEGIN(pop) {


                        PMEMoid ht_oid = pmemobj_tx_alloc(total_size, TOID_TYPE_NUM(struct masstree_obj));

                        struct masstree_obj *mo = (struct masstree_obj *) tplate;
                        mo->data = p_value;
                        mo->ht_oid = ht_oid;
                        if (!masstree_checksum(tplate, 0, p_value))throw;

                        pmemobj_tx_add_range(ht_oid, 0, total_size);
                        v = (struct masstree_obj *) pmemobj_direct(ht_oid);
                        memcpy(v, tplate, total_size);

                    }
                        TX_ONABORT {
                        throw;
                    }
        TX_END

        tree->put_and_return(p_key, v, 1, 0, t);

    } else if (use_log) {


        struct log_cell *lc = (struct log_cell *) tplate;
        lc->key = p_key;
        rdtscll(lc->version)
        *((uint64_t *) (lc + 1)) = p_value;
        if (!masstree_checksum(tplate, 0, p_value)) throw;

        void *v = log_malloc(total_size);
        pmem_memcpy_persist(v, tplate, total_size);
        tree->put_and_return(p_key, v, 1, 0, t);

    } else if (use_ralloc) {

        *((uint64_t *) tplate) = p_value;
        if (!masstree_checksum(tplate, 0, p_value)) throw;

        void *v = RP_malloc(total_size);
        pmem_memcpy_persist(v, tplate, total_size);
        tree->put_and_return(p_key, v, 1, 0, t);

        // todo: remove
        if (p_key == 8671920) {
            uint64_t * sbsbsb = (uint64_t*)v;
            printf("\t\t pointer %p value %lu\n", v,sbsbsb[0]);
        }

    } else {

        *((uint64_t *) tplate) = p_value;
        if (!masstree_checksum(tplate, 0, p_value)) throw;

        void *v = malloc(total_size);
        memcpy(v, tplate, total_size);
        tree->put_and_return(p_key, v, 1, 0, t);
    }
}

static inline void masstree_branched_update(
        masstree::masstree *tree,
        MASS::ThreadInfo t,
        uint64_t u_key,
        uint64_t u_value,
        int no_allow_prev_null,
        void *tplate

) {

    if (use_obj) {

        struct masstree_obj *mo = NULL;

        TX_BEGIN(pop) {

                        PMEMoid ht_oid = pmemobj_tx_alloc(total_size, TOID_TYPE_NUM(struct masstree_obj));

                        struct masstree_obj *o = (struct masstree_obj *) tplate;
                        o->data = u_value;
                        o->ht_oid = ht_oid;
                        if (!masstree_checksum(tplate, 0, u_value)) throw;

                        pmemobj_tx_add_range(ht_oid, 0, total_size);
                        mo = (struct masstree_obj *) pmemobj_direct(ht_oid);
                        memcpy(mo, tplate, total_size);

                    }
                        TX_ONABORT {
                        throw;
                    }
        TX_END

        struct masstree_obj *old_obj = (struct masstree_obj *)
                tree->put_and_return(u_key, mo, !no_allow_prev_null, 0, t);

        if (no_allow_prev_null || old_obj != NULL) {
            TX_BEGIN(pop) {

                            pmemobj_tx_add_range(old_obj->ht_oid, sizeof(struct masstree_obj) + memset_size,
                                                 sizeof(uint64_t));
                            ((uint64_t *) (((char *) (old_obj + 1)) + memset_size))[0] = 0;
                            pmemobj_tx_free(old_obj->ht_oid);
                        }
                            TX_ONABORT {
                            throw;
                        }
            TX_END
        }

    } else if (use_log) {


        struct log_cell *lc = (struct log_cell *) tplate;
        lc->key = u_key;
        rdtscll(lc->version)
        *((uint64_t *) (lc + 1)) = u_value;
        if (!masstree_checksum(tplate, 0, u_value)) throw;

        char *raw = (char *) log_malloc(total_size);
        pmem_memcpy_persist(raw, tplate, total_size);

        void *returned = tree->put_and_return(u_key, raw, !no_allow_prev_null, 0, t);

        if (no_allow_prev_null || returned != NULL) {
            log_free(returned);
        }

    } else if (use_ralloc) {

        *((uint64_t *) tplate) = u_value;
        if (!masstree_checksum(tplate, 0, u_value)) throw;


        void *value = RP_malloc(total_size);
        pmem_memcpy_persist(value, tplate, total_size);

        uint64_t *returned = (uint64_t *) tree->put_and_return(u_key, value, !no_allow_prev_null, 0, t);

        if (no_allow_prev_null || returned != NULL) {
            uint64_t *footer_loc = (uint64_t *) (((char *) (returned + 1)) + memset_size);
            footer_loc[0] = 0;
            pmem_persist(footer_loc, sizeof(uint64_t));

            RP_free(returned);
        }


    } else {

        *((uint64_t *) tplate) = u_value;
        if (!masstree_checksum(tplate, 0, u_value)) throw;

        void *value = malloc(total_size);
        memcpy(value, tplate, total_size);

        uint64_t *returned = (uint64_t *) tree->put_and_return(u_key, value, !no_allow_prev_null, 0, t);

        if (no_allow_prev_null || returned != NULL) {
            uint64_t *footer_loc = (uint64_t *) (((char *) (returned + 1)) + memset_size);
            footer_loc[0] = 0;
            free(returned);
        }

    }
}

static inline void masstree_branched_lookup(
        masstree::masstree *tree,
        MASS::ThreadInfo t,
        uint64_t g_key,
        uint64_t g_value,
        int check_value
) {

    void *raw = tree->get(g_key, t);
    if (raw != NULL || check_value) {
        if (!masstree_checksum(raw, 1, g_value)) {

            // todo: remove this
            for (uint64_t i = 0; i < YCSB_SIZE; i++) {
                if (ycsb_init_keys[i] == g_key) {
                    printf("=== key is inside init\n");
                }
            }


            printf("error key %lu value %lu pointer %p\n", g_key, g_value, raw);
            throw;
        }
    }
}

static inline void masstree_branched_delete(
        masstree::masstree *tree,
        MASS::ThreadInfo t,
        uint64_t d_key
) {
    if (use_obj) {

        struct masstree_obj *old_obj = (struct masstree_obj *)
                tree->del_and_return(d_key, 0, 0,
                                     NULL, t);


        TX_BEGIN(pop) {

                        pmemobj_tx_add_range(old_obj->ht_oid, sizeof(struct masstree_obj) + memset_size,
                                             sizeof(uint64_t));
                        ((uint64_t *) (((char *) (old_obj + 1)) + memset_size))[0] = 0;
                        pmemobj_tx_free(old_obj->ht_oid);
                    }
                        TX_ONABORT {
                        throw;
                    }
        TX_END


    } else if (use_log) {
        log_free(tree->del_and_return(d_key, 0, 0,
                                      log_get_tombstone, t));
    } else if (use_ralloc) {

        uint64_t *returned = (uint64_t *) tree->del_and_return(d_key, 0, 0,
                                                               NULL, t);

        uint64_t *footer_loc = (uint64_t *) (((char *) (returned + 1)) + memset_size);
        footer_loc[0] = 0;
        pmem_persist(footer_loc, sizeof(uint64_t));

        RP_free(returned);

    } else {

        uint64_t *returned = (uint64_t *) tree->del_and_return(d_key, 0, 0,
                                                               NULL, t);

        uint64_t *footer_loc = (uint64_t *) (((char *) (returned + 1)) + memset_size);
        footer_loc[0] = 0;

        free(returned);
    }
}

void *section_ycsb_load(void *arg) {

    struct section_arg *sa = (struct section_arg *) arg;

    masstree::masstree *tree = sa->tree;
    uint64_t start = sa->start;
    uint64_t end = sa->end;
    u_int64_t *latencies = sa->latencies;


    void *tplate = malloc(total_size);
    memset(tplate, 7, total_size);

    if (use_log) {

        struct log_cell *lc = (struct log_cell *) tplate;
        lc->value_size = total_size - sizeof(struct log_cell);
        lc->is_delete = 0;

    }

    auto t = tree->getThreadInfo();


//        u_int64_t a;
    u_int64_t b;

    //todo: remove
    int check_cc=0;

    for (uint64_t i = start; i < end; i++) {
//            rdtscll(a)

        masstree_branched_insert(tree, t, ycsb_init_keys[i], ycsb_init_keys[i], tplate);

        if (check_cc){
            uint64_t * vvv = (uint64_t*)tree->get(8671920,t);
            printf("CCC check point VVV is %lu key %lu\n",vvv[0],ycsb_init_keys[i]);
            check_cc=0;
        }

        //todo: remove
        if (ycsb_init_keys[i]==8671920){
            uint64_t * vvv = (uint64_t*)tree->get(8671920,t);
            printf("CCC check point VVV is %lu\n",vvv[0]);
            check_cc=1;
        }

        if (start == 0) {
            rdtscll(b)

//            latencies[i] = b - a;
            latencies[i] = b;
        }
    }

    //todo: remove
    if (start==0){
        uint64_t * vvv = (uint64_t*)tree->get(8671920,t);
        printf("CCCC value is %lu\n",vvv[0]);
    }

    return NULL;
}

void *section_ycsb_run(void *arg) {


    struct section_arg *sa = (struct section_arg *) arg;

    masstree::masstree *tree = sa->tree;
    uint64_t start = sa->start;
    uint64_t end = sa->end;
    u_int64_t *latencies = sa->latencies;
    int check_value = (YCSB_SIZE == 64000000);
    check_value = 0;

    void *tplate = malloc(total_size);
    memset(tplate, 7, total_size);

    if (use_log) {

        struct log_cell *lc = (struct log_cell *) tplate;
        lc->value_size = total_size - sizeof(struct log_cell);
        lc->is_delete = 0;

    }

    auto t = tree->getThreadInfo();


//        u_int64_t a;
    u_int64_t b;

    //todo: remove
    if (start==0){
        uint64_t * vvv = (uint64_t*)tree->get(8671920,t);
        printf("CCCC value is %lu\n",vvv[0]);
    }

    for (uint64_t i = start; i < end; i++) {

//            rdtscll(a)

        if (ycsb_ops[i] == OP_INSERT) {
            masstree_branched_insert(tree, t, ycsb_keys[i], ycsb_keys[i], tplate);
        } else if (ycsb_ops[i] == OP_UPDATE) {
            masstree_branched_update(tree, t, ycsb_keys[i], ycsb_keys[i], 0, tplate);
        } else if (ycsb_ops[i] == OP_READ) {
            masstree_branched_lookup(tree, t, ycsb_keys[i], ycsb_keys[i], check_value);
        } else if (ycsb_ops[i] == OP_SCAN) {
            uint64_t buf[200];
            int ret = tree->scan(ycsb_keys[i], ycsb_ranges[i], buf, t);
            (void) ret;
        } else if (ycsb_ops[i] == OP_DELETE) {
            masstree_branched_delete(tree, t, ycsb_keys[i]);
        }

        if (start == 0) {
            rdtscll(b)
//            latencies[i] = b - a;
            latencies[i] = b;
        }
    }


    return NULL;
}

void *section_insert(void *arg) {

    struct section_arg *sa = (struct section_arg *) arg;


    uint64_t start = sa->start;
    uint64_t end = sa->end;
    masstree::masstree *tree = sa->tree;
    uint64_t *keys = sa->keys;
    uint64_t *rands = sa->rands;
    u_int64_t *latencies = sa->latencies;


    void *tplate = malloc(total_size);
    memset(tplate, 7, total_size);


    if (use_log) {

        struct log_cell *lc = (struct log_cell *) tplate;
        lc->value_size = total_size - sizeof(struct log_cell);
        lc->is_delete = 0;


    }

    auto t = tree->getThreadInfo();


//        u_int64_t a;
    u_int64_t b;

    for (uint64_t i = start; i < end; i++) {

//            rdtscll(a)

        masstree_branched_insert(tree, t, keys[i], rands[i], tplate);

        if (start == 0) {
            rdtscll(b)
//            latencies[i] = b - a;
            latencies[i] = b;
        }
    }


    return NULL;
}

void *section_update(void *arg) {

    struct section_arg *sa = (struct section_arg *) arg;


    uint64_t start = sa->start;
    uint64_t end = sa->end;
    masstree::masstree *tree = sa->tree;
    uint64_t *keys = sa->keys;
    u_int64_t *latencies = sa->latencies;

    void *tplate = malloc(total_size);
    memset(tplate, 7, total_size);

    if (use_log) {

        struct log_cell *lc = (struct log_cell *) tplate;
        lc->value_size = total_size - sizeof(struct log_cell);
        lc->is_delete = 0;

    }

    auto t = tree->getThreadInfo();



//        u_int64_t a;
    u_int64_t b;

    for (uint64_t i = start; i < end; i++) {

//            rdtscll(a)

        masstree_branched_update(tree, t, keys[i], keys[i], 1, tplate);


        if (start == 0) {
            rdtscll(b)
//            latencies[i] = b - a;
            latencies[i] = b;
        }
    }


    return NULL;
}

void *section_lookup(void *arg) {

    struct section_arg *sa = (struct section_arg *) arg;


    uint64_t start = sa->start;
    uint64_t end = sa->end;
    masstree::masstree *tree = sa->tree;
    uint64_t *keys = sa->keys;
    u_int64_t *latencies = sa->latencies;


    auto t = tree->getThreadInfo();


//        u_int64_t a;
    u_int64_t b;

    for (uint64_t i = start; i < end; i++) {

//            rdtscll(a)

        masstree_branched_lookup(tree, t, keys[i], keys[i], 1);

        if (start == 0) {
            rdtscll(b)
//            latencies[i] = b - a;
            latencies[i] = b;

        }
    }


    return NULL;
}

void *section_delete(void *arg) {
    struct section_arg *sa = (struct section_arg *) arg;


    uint64_t start = sa->start;
    uint64_t end = sa->end;
    masstree::masstree *tree = sa->tree;
    uint64_t *keys = sa->keys;
    u_int64_t *latencies = sa->latencies;


    auto t = tree->getThreadInfo();


//        u_int64_t a;
    u_int64_t b;

    for (uint64_t i = start; i < end; i++) {

//            rdtscll(a)

        masstree_branched_delete(tree, t, keys[i]);

        if (start == 0) {
            rdtscll(b)
//            latencies[i] = b - a;
            latencies[i] = b;

        }
    }


    return NULL;
}

void run(
        const char *section_name,
        FILE *throughput_file,
        pthread_attr_t *attrs,
        struct section_arg *section_args,
        u_int64_t *latencies,
        void *(*routine)(void *)) {

    char perf_fn[64];
    sprintf(perf_fn, "%s-%s.perf", prefix == NULL ? "" : prefix, section_name);
    pthread_t *threads = (pthread_t *) calloc(num_thread, sizeof(pthread_t));

    if (use_perf)log_start_perf(perf_fn);

    // Build tree
    auto starttime = std::chrono::system_clock::now();

    for (int i = 0; i < num_thread; i++) {
        pthread_create(threads + i, attrs + i, routine, section_args + i);
    }
    for (int i = 0; i < num_thread; i++) {
        pthread_join(threads[i], NULL);
    }


    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);

    log_wait_all_gc();
    auto duration_with_gc = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);

    if (use_perf) {
        log_stop_perf();
        log_print_pmem_bandwidth(perf_fn, duration.count() / 1000000.0, throughput_file);
    }
    if (display_throughput)
        printf("Throughput: %s,%ld,%.2f ops/us %.2f sec\n",
               section_name, n, (n * 1.0) / duration.count(), duration.count() / 1000000.0);

    sprintf(perf_fn, "%s-%s.rdtsc", prefix, section_name);
    if (record_latency) dump_latencies(perf_fn, latencies, section_args[0].end);

    if (throughput_file != NULL) {
        fprintf(throughput_file, "%.2f,%.2f,",
                (n * 1.0) / duration.count(),
                (n * 1.0) / duration_with_gc.count());
    }
}

int main(int argc, char **argv) {

    if (argc < 3) {
        printf("usage: %s [n] [nthreads]\nn: number of keys (integer)\nnthreads: number of threads (integer)\n",
               argv[0]);
        return 1;
    }
    for (int ac = 0; ac < argc; ac++) printf("%s ", argv[ac]);
    puts("");

    for (int ac = 0; ac < argc; ac++) {

        if (ac == 1) {
            n = std::atoll(argv[1]);
            printf("n:%lu ", n);
            YCSB_SIZE = n;

        } else if (ac == 2) {
            num_thread = atoi(argv[2]);
            printf("num_thread:%d ", num_thread);

        } else if (strcasestr(argv[ac], "index=")) {
            if (strcasestr(argv[ac], "ralloc")) {
                which_memalign = RP_memalign;
                which_memfree = RP_free;
                require_RP_init = 1;
                printf("index=ralloc ");

            } else if (strcasestr(argv[ac], "log")) {
                which_memalign = log_memalign;
                which_memfree = log_free;
                require_log_init = 1;
                printf("index=log ");

            } else if (strcasestr(argv[ac], "obj")) {
                which_memalign = masstree::obj_memalign;
                which_memfree = masstree::obj_free;
                require_obj_init = 1;
                printf("index=obj ");

            } else {
                printf("index=dram ");

            }
        } else if (strcasestr(argv[ac], "value=")) {
            if (strcasestr(argv[ac], "ralloc")) {
                require_RP_init = 1;
                use_ralloc = 1;
                base_size = sizeof(uint64_t) * 2;
                printf("value=ralloc ");

            } else if (strcasestr(argv[ac], "log")) {
                require_log_init = 1;
                use_log = 1;
                base_size = sizeof(struct log_cell) + sizeof(uint64_t) * 2;
                printf("value=log ");


                value_offset = sizeof(struct log_cell);
                assert(value_offset % sizeof(uint64_t) == 0);
                value_offset = value_offset / sizeof(uint64_t);

            } else if (strcasestr(argv[ac], "obj")) {
                use_obj = 1;
                require_obj_init = 1;
                base_size = sizeof(struct masstree_obj) + sizeof(uint64_t);
                printf("value=obj ");


                struct masstree_obj obj_tmp;
                value_offset = ((char *) (&obj_tmp.data) - (char *) (&obj_tmp));
                assert(value_offset % sizeof(uint64_t) == 0);
                value_offset = value_offset / sizeof(uint64_t);

            } else {
                printf("value=dram ");
                base_size = sizeof(uint64_t);
            }
        } else if (strcasestr(argv[ac], "key=")) {
            if (strcasestr(argv[ac], "rand")) {
                shuffle_keys = 1;
                printf("key=rand ");

            } else {
                printf("key=seq ");

            }
        } else if (strcasestr(argv[ac], "perf=")) {
            if (strcasestr(argv[ac], "y")) {
                use_perf = 1;
                printf("perf=y ");

            } else {
                printf("perf=n ");

            }
        } else if (strcasestr(argv[ac], "gc=")) {
            if (require_log_init)num_of_gc = atoi(strcasestr(argv[ac], "=") + 1);
            printf("gc=%d ", num_of_gc);

        } else if (strcasestr(argv[ac], "latency=")) {
            if (strcasestr(argv[ac], "yes")) {
                record_latency = 1;
                printf("latency=yes ");

            } else {
                printf("latency=no ");

            }
        } else if (strcasestr(argv[ac], "extra_size=")) {
            memset_size = atoi(strcasestr(argv[ac], "=") + 1);
            printf("memset_size=%d ", memset_size);

        } else if (strcasestr(argv[ac], "total_size=")) {
            total_size = atoi(strcasestr(argv[ac], "=") + 1);
            printf("total_size=%d ", total_size);

        } else if (strcasestr(argv[ac], "ycsb=")) {
            wl = strcasestr(argv[ac], "=") + 1;

            if (wl[0] != 'a' && wl[0] != 'b' && wl[0] != 'c' && wl[0] != 'd' && wl[0] != 'e') {
                wl = NULL;
                continue;
            }

            printf("ycsb=%s ", wl);

            if (YCSB_SIZE > 64000000) YCSB_SIZE = 64000000;
            if (n > 64000000) n = 64000000;

//            ycsb_load();
            bap_ycsb_load();

        } else if (strcasestr(argv[ac], "prefix=")) {
            char *prefix_ptr = strcasestr(argv[ac], "=") + 1;
            prefix = (char *) malloc(sizeof(char) * (strlen(prefix_ptr) + 1));
            strcpy(prefix, prefix_ptr);
        }
    }

    if (total_size == 0) {
        total_size = memset_size + base_size;
    } else {

        if (total_size < base_size) total_size = base_size;

        memset_size = total_size - base_size;
    }


    iter = total_size / sizeof(uint64_t) - 1;
    printf("total_size=%d ", total_size);

    puts("");

    puts("\tbegin generating keys");
    uint64_t *keys = new uint64_t[n];
    uint64_t *rands = new uint64_t[n];


    // Generate keys
    for (uint64_t i = 0; i < n; i++) {
        rands[i] = rand();
    }

    // Generate keys
    for (uint64_t i = 0; i < n; i++) {
        keys[i] = i + 1;
    }

    // (TP dropped) shuffle the array
    if (shuffle_keys) {

        for (uint64_t i = 0; i < n - 1; i++) {
            uint64_t j = i + rand() / (RAND_MAX / (n - i) + 1);
            uint64_t t = keys[j];
            keys[j] = keys[i];
            keys[i] = t;
        }
    }

#ifdef CLFLUSH
    puts("\tdetected CLFLUSH");
#elif CLFLUSH_OPT
    puts("\tdetected CLFLUSH_OPT");
#elif CLWB
    puts("\tdetected CLWB");
#else
    puts("\tno available cache line write back found")
#endif

#ifdef MASSTREE_FLUSH
    puts("\tMASSTREE_FLUSH");
#else
    puts("\ttesting eADR");
#endif


    PMEM_POOL_SIZE = total_size * n * 3;
    uint64_t size_round = 4ULL * 1024ULL * 1024ULL * 1024ULL;
    while (size_round < PMEM_POOL_SIZE) {
        size_round += 4ULL * 1024ULL * 1024ULL * 1024ULL;
    }
    PMEM_POOL_SIZE = size_round;

    if (require_RP_init) {
        puts("\tbegin preparing Ralloc");
        int preset = 0;
        RP_init("masstree", PMEM_POOL_SIZE, &preset);
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

//    tbb::task_scheduler_init init(num_thread);
    srand(time(NULL));
    masstree::masstree *tree = new masstree::masstree();

    FILE *throughput_file = fopen("perf.csv", "a");
    u_int64_t *latencies = NULL;

    pthread_attr_t *attrs = (pthread_attr_t *) calloc(num_thread, sizeof(pthread_attr_t));
    cpu_set_t *cpus = (cpu_set_t *) calloc(num_thread, sizeof(cpu_set_t));
    struct section_arg *section_args = (struct section_arg *) calloc(num_thread, sizeof(struct section_arg));

    uint64_t n_per_thread = n / num_thread;
    uint64_t n_remainder = n % num_thread;
    int numberOfProcessors = sysconf(_SC_NPROCESSORS_ONLN);
    printf("\tNumber of processors: %d\n", numberOfProcessors);


    for (int i = 0; i < num_thread; i++) {

        CPU_ZERO(cpus + i);
        CPU_SET(i + 1, cpus + i); // reserving CPU 0

        pthread_attr_init(attrs + i);
        pthread_attr_setaffinity_np(attrs + i, sizeof(cpu_set_t), cpus + i);

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
        section_args[i].keys = keys;
        section_args[i].rands = rands;

        if (i == 0) {
            latencies = (u_int64_t *) malloc(sizeof(u_int64_t) * section_args[i].end);
            section_args[i].latencies = latencies;
        } else {
            section_args[i].latencies = NULL;
        }

    }

    if (require_log_init) {

        puts("\tbegin preparing Log");

        if (
                access(INODE_FN, F_OK) == 0 &&
                access(LOG_FN, F_OK) == 0 &&
                access(META_FN, F_OK) == 0
                ) {
            log_recover(tree, 20);
            goto lookup;
        } else {
            log_init(PMEM_POOL_SIZE, num_thread);
        }

        if (num_of_gc > 0) {
            puts("\tbegin creating Gc");
            for (int gcc = 0; gcc < num_of_gc; gcc++) {
                log_start_gc(tree);
            }
        }
    }


    std::cout << "Simple Example of P-Masstree-New" << std::endl;
    printf("operation,n,ops/s\n");


    {
        /**
         * section YCSB
         */
        if (wl != NULL) {
            run("ycsb_load", throughput_file, attrs, section_args, latencies, section_ycsb_load);
            run("ycsb_run", throughput_file, attrs, section_args, latencies, section_ycsb_run);
            goto end;
        }
    }

    {
        /**
         * section INSERT
         */
        run("insert", throughput_file, attrs, section_args, latencies, section_insert);
        if (use_log) log_debug_print(1, show_log_usage);
    }

    {
        /**
         * section UPDATE
         */
        run("update", throughput_file, attrs, section_args, latencies, section_update);
        if (use_log) log_debug_print(0, show_log_usage);
    }

    lookup:
    {
        /**
         * section LOOKUP
         */
        run("lookup", throughput_file, attrs, section_args, latencies, section_lookup);
        if (use_log) log_debug_print(0, show_log_usage);
    }

    {
        /**
         * section DELETE
         */
        run("delete", throughput_file, attrs, section_args, latencies, section_delete);
        if (use_log) log_debug_print(0, show_log_usage);
    }


    end:

    // logging throughput to files
    fclose(throughput_file);


    if (use_log) {
        log_join_all_gc();
        log_debug_print(2, show_log_usage);
    }


    delete[] keys;

    return 0;
}

