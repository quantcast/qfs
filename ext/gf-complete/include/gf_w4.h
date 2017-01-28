/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_w4.h
 *
 * Defines and data structures for 4-bit Galois fields
 */

#ifndef GF_COMPLETE_GF_W4_H
#define GF_COMPLETE_GF_W4_H

#include <stdint.h>

#define GF_FIELD_WIDTH      4
#define GF_DOUBLE_WIDTH     (GF_FIELD_WIDTH*2)
#define GF_FIELD_SIZE       (1 << GF_FIELD_WIDTH)
#define GF_MULT_GROUP_SIZE       (GF_FIELD_SIZE-1)

/* ------------------------------------------------------------
   JSP: Each implementation has its own data, which is allocated
   at one time as part of the handle. For that reason, it
   shouldn't be hierarchical -- i.e. one should be able to
   allocate it with one call to malloc. */

struct gf_logtable_data {
    uint8_t      log_tbl[GF_FIELD_SIZE];
    uint8_t      antilog_tbl[GF_FIELD_SIZE * 2];
    uint8_t      *antilog_tbl_div;
};

struct gf_single_table_data {
    uint8_t      mult[GF_FIELD_SIZE][GF_FIELD_SIZE];
    uint8_t      div[GF_FIELD_SIZE][GF_FIELD_SIZE];
};

struct gf_double_table_data {
    uint8_t      div[GF_FIELD_SIZE][GF_FIELD_SIZE];
    uint8_t      mult[GF_FIELD_SIZE][GF_FIELD_SIZE*GF_FIELD_SIZE];
};
struct gf_quad_table_data {
    uint8_t      div[GF_FIELD_SIZE][GF_FIELD_SIZE];
    uint16_t     mult[GF_FIELD_SIZE][(1<<16)];
};

struct gf_quad_table_lazy_data {
    uint8_t      div[GF_FIELD_SIZE][GF_FIELD_SIZE];
    uint8_t      smult[GF_FIELD_SIZE][GF_FIELD_SIZE];
    uint16_t     mult[(1 << 16)];
};

struct gf_bytwo_data {
    uint64_t prim_poly;
    uint64_t mask1;
    uint64_t mask2;
};

// ARM NEON init functions
int gf_w4_neon_cfm_init(gf_t *gf);
void gf_w4_neon_single_table_init(gf_t *gf);

#endif /* GF_COMPLETE_GF_W4_H */
