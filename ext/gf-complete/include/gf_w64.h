/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_w64.h
 *
 * Defines and data structures for 64-bit Galois fields
 */

#ifndef GF_COMPLETE_GF_W64_H
#define GF_COMPLETE_GF_W64_H

#include <stdint.h>

#define GF_FIELD_WIDTH (64)
#define GF_FIRST_BIT (1ULL << 63)

#define GF_BASE_FIELD_WIDTH (32)
#define GF_BASE_FIELD_SIZE       (1ULL << GF_BASE_FIELD_WIDTH)
#define GF_BASE_FIELD_GROUP_SIZE  GF_BASE_FIELD_SIZE-1

struct gf_w64_group_data {
    uint64_t *reduce;
    uint64_t *shift;
    uint64_t *memory;
};

struct gf_split_4_64_lazy_data {
    uint64_t      tables[16][16];
    uint64_t      last_value;
};

struct gf_split_8_64_lazy_data {
    uint64_t      tables[8][(1<<8)];
    uint64_t      last_value;
};

struct gf_split_16_64_lazy_data {
    uint64_t      tables[4][(1<<16)];
    uint64_t      last_value;
};

struct gf_split_8_8_data {
    uint64_t      tables[15][256][256];
};

void gf_w64_neon_split_init(gf_t *gf);

#endif /* GF_COMPLETE_GF_W64_H */
