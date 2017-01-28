/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_w16.h
 *
 * Defines and data structures for 16-bit Galois fields
 */

#ifndef GF_COMPLETE_GF_W16_H
#define GF_COMPLETE_GF_W16_H

#include <stdint.h>

#define GF_FIELD_WIDTH (16)
#define GF_FIELD_SIZE (1 << GF_FIELD_WIDTH)
#define GF_MULT_GROUP_SIZE GF_FIELD_SIZE-1

#define GF_BASE_FIELD_WIDTH (8)
#define GF_BASE_FIELD_SIZE       (1 << GF_BASE_FIELD_WIDTH)

struct gf_w16_logtable_data {
    uint16_t      log_tbl[GF_FIELD_SIZE];
    uint16_t      antilog_tbl[GF_FIELD_SIZE * 2];
    uint16_t      inv_tbl[GF_FIELD_SIZE];
    uint16_t      *d_antilog;
};

struct gf_w16_zero_logtable_data {
    int           log_tbl[GF_FIELD_SIZE];
    uint16_t      _antilog_tbl[GF_FIELD_SIZE * 4];
    uint16_t      *antilog_tbl;
    uint16_t      inv_tbl[GF_FIELD_SIZE];
};

struct gf_w16_lazytable_data {
    uint16_t      log_tbl[GF_FIELD_SIZE];
    uint16_t      antilog_tbl[GF_FIELD_SIZE * 2];
    uint16_t      inv_tbl[GF_FIELD_SIZE];
    uint16_t      *d_antilog;
    uint16_t      lazytable[GF_FIELD_SIZE];
};

struct gf_w16_bytwo_data {
    uint64_t prim_poly;
    uint64_t mask1;
    uint64_t mask2;
};

struct gf_w16_split_8_8_data {
    uint16_t      tables[3][256][256];
};

struct gf_w16_group_4_4_data {
    uint16_t reduce[16];
    uint16_t shift[16];
};

struct gf_w16_composite_data {
  uint8_t *mult_table;
};

void gf_w16_neon_split_init(gf_t *gf);

#endif /* GF_COMPLETE_GF_W16_H */
