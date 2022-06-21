//
// Created by Xiaoxiang Wu on 21/6/2022.
//

#ifndef RECIPE_POBJ_H
#define RECIPE_POBJ_H

#include <libpmemobj.h>

struct pobj_masstree{
    int dummy;
};

// Global pool uuid
uint64_t pool_uuid;

// Global pool pointer
PMEMobjpool *pop = NULL;

#endif //RECIPE_POBJ_H
