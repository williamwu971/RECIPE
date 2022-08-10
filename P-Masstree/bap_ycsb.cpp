
static long items; //initialized in init_zipf_generator function
static long base; //initialized in init_zipf_generator function
static double zipfianconstant; //initialized in init_zipf_generator function
static double alpha; //initialized in init_zipf_generator function
static double zetan; //initialized in init_zipf_generator function
static double eta; //initialized in init_zipf_generator function
static double theta; //initialized in init_zipf_generator function
static double zeta2theta; //initialized in init_zipf_generator function
static long countforzeta; //initialized in init_zipf_generator function


double zetastatic(long st, long n, double initialsum) {
    double sum = initialsum;
    for (long i = st; i < n; i++) {
        sum += 1 / (pow(i + 1, theta));
    }
    return sum;
}

double zeta(long st, long n, double initialsum) {
    countforzeta = n;
    return zetastatic(st, n, initialsum);
}

static unsigned int __thread seed;

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

typedef enum available_bench {
    ycsb_a_uniform,
    ycsb_b_uniform,
    ycsb_c_uniform,
    ycsb_e_uniform,
    ycsb_a_zipfian,
    ycsb_b_zipfian,
    ycsb_c_zipfian,
    ycsb_e_zipfian,
    prod1,
    prod2,
} bench_t;

/* Is the current request a get or a put? */
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

struct item_metadata {
    size_t rdt;
    size_t key_size;
    size_t value_size;
    // key
    // value
};

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

char *create_unique_item(size_t item_size, uint64_t uid) {
    char *item = (char *) which_malloc(item_size);
    struct item_metadata *meta = (struct item_metadata *) item;
    meta->key_size = 8;
    meta->value_size = item_size - 8 - sizeof(*meta);

    char *item_key = &item[sizeof(*meta)];
    char *item_value = &item[sizeof(*meta) + meta->key_size];
    *(uint64_t *) item_key = uid;
    *(uint64_t *) item_value = uid;
    return item;
}

/* YCSB A (or D), B, C */
static void _launch_ycsb(int test, int nb_requests, int zipfian) {

    init_zipf_generator(0, nb_requests - 1);
    long (*rand_next)(void) = zipfian ? zipf_next : uniform_next;
    masstree::masstree *tree = new masstree::masstree();

    auto starttime = std::chrono::system_clock::now();
    tbb::parallel_for(tbb::blocked_range<uint64_t>(0, nb_requests), [&](const tbb::blocked_range<uint64_t> &scope) {
        auto t = tree->getThreadInfo();
        for (uint64_t i = scope.begin(); i != scope.end(); i++) {

            long fkey = rand_next();
            char *item = create_unique_item(1024, fkey);

            if (random_get_put(test)) { // In these tests we update with a given probability
                tree->put(fkey, item, t);
            } else { // or we read
                tree->get(fkey, t);
//                uint64_t *ret = reinterpret_cast<uint64_t *> (tree->get((char *)keys[i]->fkey, t));
//                if (ap == UNIFORM && (uint64_t) ret != keys[i]->value) {
//                    printf("[MASS] search key = %lu, search value = %lu\n", keys[i]->value, ret);
//                    exit(1);
//                }
            }
        }
    });
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);
    printf("Throughput: run, %f ,ops/us\n", (nb_requests * 1.0) / duration.count());
}

/* YCSB E */
static void _launch_ycsb_e(int test, int nb_requests, int zipfian) {

    init_zipf_generator(0, nb_requests - 1);
    long (*rand_next)(void) = zipfian ? zipf_next : uniform_next;
    masstree::masstree *tree = new masstree::masstree();

    auto starttime = std::chrono::system_clock::now();
    tbb::parallel_for(tbb::blocked_range<uint64_t>(0, nb_requests), [&](const tbb::blocked_range<uint64_t> &scope) {
        auto t = tree->getThreadInfo();
        for (uint64_t i = scope.begin(); i != scope.end(); i++) {

            long key = rand_next();
            char *item = create_unique_item(1024, key);

            if (random_get_put(test)) {
                tree->put(key, item, t);
            } else {
                uint64_t buf[200];
                int ret = tree->scan(key, uniform_next() % 99 + 1, buf, t);
                (void) ret;
            }

        }
    });
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::system_clock::now() - starttime);
    printf("Throughput: run, %f ,ops/us\n", (nb_requests * 1.0) / duration.count());
}

/* Generic interface */
static void launch_ycsb(bench_t b, uint64_t nb_requests) {
    switch (b) {
        case ycsb_a_uniform:
            return _launch_ycsb(0, nb_requests, 0);
        case ycsb_b_uniform:
            return _launch_ycsb(1, nb_requests, 0);
        case ycsb_c_uniform:
            return _launch_ycsb(2, nb_requests, 0);
        case ycsb_e_uniform:
            return _launch_ycsb_e(3, nb_requests, 0);
        case ycsb_a_zipfian:
            return _launch_ycsb(0, nb_requests, 1);
        case ycsb_b_zipfian:
            return _launch_ycsb(1, nb_requests, 1);
        case ycsb_c_zipfian:
            return _launch_ycsb(2, nb_requests, 1);
        case ycsb_e_zipfian:
            return _launch_ycsb_e(3, nb_requests, 1);
        default:
            die("Unsupported workload\n");
    }
}