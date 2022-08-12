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

static inline void clflush(char *data, int len, int front, int back) {
    volatile char *ptr = (char *) ((unsigned long) data & ~(CACHE_LINE_SIZE - 1));
    if (front)
        asm volatile("sfence":: :"memory");
    for (; ptr < data + len; ptr += CACHE_LINE_SIZE) {
#ifdef CLFLUSH
        asm volatile("clflush %0" : "+m" (*(volatile char *)ptr));
#elif CLFLUSH_OPT
        asm volatile(".byte 0x66; clflush %0" : "+m" (*(volatile char *)(ptr)));
#elif CLWB
        asm volatile(".byte 0x66; xsaveopt %0" : "+m" (*(volatile char *) (ptr)));
#endif
    }
    if (back)
        asm volatile("sfence":: :"memory");
}

int main(int argc, char **argv) {
    init_seed();

    long granularity = 256;      // granularity of accesses
    long nb_accesses = 30000000;   // nb ops
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
//    char *map = pmem_map_file(path, 0, 0, 0777, NULL, NULL);
//    if (!pmem_is_pmem(map, sb.st_size))
//        die("File is not in pmem?!");
    char *map = mmap(NULL, sb.st_size, PROT_READ | PROT_WRITE, MAP_SYNC | MAP_SHARED_VALIDATE, fd, 0);
    memset(map, 0, sb.st_size);

    /* Allocate data to copy to the file */
    char *page_data = aligned_alloc(PAGE_SIZE, granularity);
    memset(page_data, lehmer64(), granularity);

    /*for(int i = 0; i < nb_accesses; i++) {
       memcpy(map[location], xxx, size);
    }*/
    puts("begin");

    /* Benchmark N memcpy */
    declare_timer;
    start_timer
    {
        uint64_t start = lehmer64() % (sb.st_size - (nb_accesses + 1) * granularity);
        for (size_t i = 0; i < nb_accesses; i++) {
            uint64_t loc = lehmer64() % (sb.st_size - granularity);
//            uint64_t loc = start + i * granularity;
//            pmem_memcpy_persist(&map[loc], page_data, granularity);
//            clflush(&map[loc], granularity, 1, 1);
            msync(&map[loc], granularity, MS_SYNC);
        }
    }stop_timer("Doing %ld memcpy of %ld bytes (%f MB/s)", nb_accesses, granularity,
                bandwith(nb_accesses * granularity, elapsed));

    return 0;
}
