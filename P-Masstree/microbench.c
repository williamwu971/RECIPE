#include "microbench.h"

static __uint128_t g_lehmer64_state;

static void init_seed(void) {
    srand(time(NULL));
    g_lehmer64_state = rand();
}

static uint64_t lehmer64() {
    g_lehmer64_state *= 0xda942042e4dd58b5;
    return g_lehmer64_state >> 64;
}

/* data_size in Bytes, time in us, returns bandwith in MB/s */
static double bandwith(long data_size, long time) {
    return (((double) data_size) / 1024. / 1024.) / (((double) time) / 1000000.);
}

int main(int argc, char **argv) {
    init_seed();

    long granularity = 64;      // granularity of accesses
    long nb_accesses = 10000;   // nb ops
    char *path = "/pmem0/masstree_sb";   // benched file

    /* Open file */
    int fd = open(path, O_RDWR | O_CREAT | O_DIRECT, 0777);
    if (fd == -1)
        die("Cannot open %s\n", path);

    /* Find size */
    struct stat sb;
    fstat(fd, &sb);
    printf("# Size of file being benched: %luMB\n", sb.st_size / 1024 / 1024);

    /* Mmap file */
    char *map = pmem_map_file(path, 0, 0, 0777, NULL, NULL);
    if (!pmem_is_pmem(map, sb.st_size))
        die("File is not in pmem?!");
    memset(map, 0, sb.st_size);

    /* Allocate data to copy to the file */
    char *page_data = aligned_alloc(PAGE_SIZE, granularity);
    memset(page_data, 52, granularity);

    /*for(int i = 0; i < nb_accesses; i++) {
       memcpy(map[location], xxx, size);
    }*/

    /* Benchmark N memcpy */
    declare_timer;
    start_timer
    {
        for (size_t i = 0; i < nb_accesses; i++) {
            uint64_t loc = lehmer64() % (sb.st_size - granularity);
            pmem_memcpy_persist(&map[loc], page_data, granularity);
        }
    }stop_timer("Doing %ld memcpy of %ld bytes (%f MB/s)", nb_accesses, granularity,
                bandwith(nb_accesses * granularity, elapsed));

    return 0;
}
