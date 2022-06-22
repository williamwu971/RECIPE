//
// Created by Xiaoxiang Wu on 22/6/2022.
//

#include <stdio.h>
#include <omp.h>
#include <stdlib.h>
#include <pthread.h>
#include <libpmem.h>
#include <sys/time.h>

#define declare_timer u_int64_t elapsed; \
   struct timeval st, et;

#define start_timer do { \
    gettimeofday(&st,NULL); \
} while(0);


#define stop_timer do { \
   gettimeofday(&et,NULL); \
   elapsed = ((et.tv_sec - st.tv_sec) * 1000000) + (et.tv_usec - st.tv_usec) + 1; \
} while(0);

struct minibench_arg {
    void *file_map;
    size_t file_size;
    int i;
    int num_of_thread;

    int step_size;

    size_t *offset;
    pthread_mutex_t *mutex;
    pthread_barrier_t *barrier;


//    u_int64_t fault_elapsed;
//    u_int64_t write_elapsed;
};

void *minibench_thread(void *ptr) {
    struct minibench_arg arg = *(struct minibench_arg *) ptr;
    declare_timer

    start_timer

    while (1) {

        size_t start;

        pthread_mutex_lock(arg.mutex);

        if (arg.offset[0] >= arg.file_size) {
            pthread_mutex_unlock(arg.mutex);
            break;
        }

        start = arg.offset[0];
        arg.offset[0] += arg.step_size;

        pthread_mutex_unlock(arg.mutex);


        pmem_memset_persist(arg.file_map + start, 7, arg.step_size);
    }

    pthread_barrier_wait(arg.barrier);

    stop_timer
    start_timer

    if (arg.i == 0) {
        printf("elapsed: %.2fs", (double) elapsed / 1000000.0);
    }


    char buf[64] = "hello_world_from_william_wu";

    size_t per_thread = arg.file_size / arg.num_of_thread;
    size_t start = per_thread * arg.i;
    size_t end = per_thread * (arg.i + 1);

    for (size_t loc = start; loc < end; loc += 64) {
        pmem_memcpy_persist(arg.file_map + loc, buf, 64);
    }

    pthread_barrier_wait(arg.barrier);

    stop_timer

    if (arg.i == 0) {
        printf("elapsed: %.2fs", (double) elapsed / 1000000.0);
    }

    return NULL;
}

int main(int argc, char **argv) {

    if (argc != 4) {
        printf("argc: %d\n", argc);
        return 1;
    }


    int num_of_gb = atoi(argv[1]);
    int num_of_thread = atoi(argv[2]);
    int step_size = atoi(argv[3]);


    u_int64_t file_size = (u_int64_t)num_of_gb * 1024 * 1024 * 1024;
    size_t mapped_len;
    int is_pmem;

    void *file_map = pmem_map_file("/pmem0/minibench", file_size,
                                   PMEM_FILE_CREATE | PMEM_FILE_EXCL, 00666,
                                   &mapped_len, &is_pmem);

    if (file_map == NULL || mapped_len != file_size || !is_pmem) {
        printf("mapped_len:%zu is_pmem:%d\n", mapped_len, is_pmem);
        return 1;
    }

    pthread_t *threads = malloc(sizeof(pthread_t) * num_of_thread);
    struct minibench_arg *args = malloc(sizeof(struct minibench_arg) * num_of_thread);

    size_t offset = 0;
    pthread_mutex_t mutex;
    pthread_barrier_t barrier;
    pthread_mutex_init(&mutex, NULL);
    pthread_barrier_init(&barrier, NULL, num_of_thread);

    for (int i = 0; i < num_of_thread; i++) {
        args[i].file_map = file_map;
        args[i].barrier = &barrier;
        args[i].mutex = &mutex;
        args[i].i = i;
        args[i].num_of_thread = num_of_thread;
        args[i].offset = &offset;
        args[i].file_size = file_size;
        args[i].step_size = step_size;
    }

    for (int i = 0; i < num_of_thread; i++) {
        pthread_create(threads + i, NULL, minibench_thread, args + i);
    }

    for (int i = 0; i < num_of_thread; i++) {
        pthread_join(threads[i], NULL);
    }
}