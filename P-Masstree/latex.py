import sys

for line in sys.stdin:
    tokens = line.split()
    tokens[0] = tokens[0].replace("_", " ")
    print("&".join(tokens), end="\\\\\n")
    print("\\hline")
"""

tx.c pmemobj_tx_alloc()
    tx.c tx_alloc_common()
        palloc.c palloc_reserve()
            palloc.c palloc_reservation_create()
                heap.c heap_get_bestfit_block()
                    heap.c heap_ensure_run_bucket_filled()
                        heap.c heap_detach_and_try_discard_run()
                            heap.c heap_discard_run()
                                heap.c heap_reclaim_run()
                                    recycler.c recycler_put()
                                        sys_util.h util_mutex_lock()
                                            os_thread_posix.c os_mutex_lock()
                                                pthread.h pthread_mutex_lock()

"""