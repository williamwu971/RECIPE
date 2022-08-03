//
// Created by Xiaoxiang Wu on 21/6/2022.
//

#include "pobj.h"
#include "plog.h"


void pobj_init(const char *fn, size_t pool_size) {

    if (pop != NULL) {
        die("pop is %p", pop);
    }

    // Enable prefault
    int arg_open = 1, arg_create = 1;
    if ((pmemobj_ctl_set(pop, "prefault.at_open", &arg_open)) != 0)
        die("failed to configure prefaults at open\n");
    if ((pmemobj_ctl_set(pop, "prefault.at_create", &arg_create)) != 0)
        die("failed to configure prefaults at create\n");

    if (access(fn, F_OK) != -1) {
        pop = pmemobj_open(fn, POBJ_LAYOUT_NAME(masstree));
    } else {
        pop = pmemobj_create(fn, POBJ_LAYOUT_NAME(masstree), pool_size, 0666);
    }

    if (pop == NULL) {
        die("pop is %p", pop);
    }

    // Create the root pointer
    PMEMoid my_root = pmemobj_root(pop, sizeof(struct pobj_masstree));
    struct pobj_masstree *pm = (struct pobj_masstree *) pmemobj_direct(my_root);
    if (pm == NULL) {
        die("root pointer is null\n");
    }

    pool_uuid = my_root.pool_uuid_lo;
}

void *pobj_malloc(size_t size) {
//    TX_

    PMEMoid ht_oid;
    if (pmemobj_alloc(pop, &ht_oid, size, TOID_TYPE_NUM(clht_hashtable_t), 0, 0)) {
        die("pmemobj_alloc error");
    }


    return NULL;
}