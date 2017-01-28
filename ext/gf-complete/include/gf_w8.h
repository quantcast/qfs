/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_w8.c
 *
 * Defines and data stuctures for 8-bit Galois fields
 */

#ifndef GF_COMPLETE_GF_W8_H
#define GF_COMPLETE_GF_W8_H

#include "gf_int.h"
#include <stdint.h>

#define GF_FIELD_WIDTH (8)
#define GF_FIELD_SIZE       (1 << GF_FIELD_WIDTH)
#define GF_HALF_SIZE       (1 << (GF_FIELD_WIDTH/2))
#define GF_MULT_GROUP_SIZE       GF_FIELD_SIZE-1

#define GF_BASE_FIELD_WIDTH (4)
#define GF_BASE_FIELD_SIZE       (1 << GF_BASE_FIELD_WIDTH)

struct gf_w8_logtable_data {
    uint8_t         log_tbl[GF_FIELD_SIZE];
    uint8_t         antilog_tbl[GF_FIELD_SIZE * 2];
    uint8_t         inv_tbl[GF_FIELD_SIZE];
};

struct gf_w8_logzero_table_data {
    short           log_tbl[GF_FIELD_SIZE];  /* Make this signed, so that we can divide easily */
    uint8_t         antilog_tbl[512+512+1];
    uint8_t         *div_tbl;
    uint8_t         *inv_tbl;
};

struct gf_w8_logzero_small_table_data {
    short           log_tbl[GF_FIELD_SIZE];  /* Make this signed, so that we can divide easily */
    uint8_t         antilog_tbl[255*3];
    uint8_t         inv_tbl[GF_FIELD_SIZE];
    uint8_t         *div_tbl;
};

struct gf_w8_composite_data {
  uint8_t *mult_table;
};

/* Don't change the order of these relative to gf_w8_half_table_data */

struct gf_w8_default_data {
  uint8_t     high[GF_FIELD_SIZE][GF_HALF_SIZE];
  uint8_t     low[GF_FIELD_SIZE][GF_HALF_SIZE];
  uint8_t     divtable[GF_FIELD_SIZE][GF_FIELD_SIZE];
  uint8_t     multtable[GF_FIELD_SIZE][GF_FIELD_SIZE];
};

struct gf_w8_half_table_data {
  uint8_t     high[GF_FIELD_SIZE][GF_HALF_SIZE];
  uint8_t     low[GF_FIELD_SIZE][GF_HALF_SIZE];
};

struct gf_w8_single_table_data {
  uint8_t     divtable[GF_FIELD_SIZE][GF_FIELD_SIZE];
  uint8_t     multtable[GF_FIELD_SIZE][GF_FIELD_SIZE];
};

struct gf_w8_double_table_data {
    uint8_t         div[GF_FIELD_SIZE][GF_FIELD_SIZE];
    uint16_t        mult[GF_FIELD_SIZE][GF_FIELD_SIZE*GF_FIELD_SIZE];
};

struct gf_w8_double_table_lazy_data {
    uint8_t         div[GF_FIELD_SIZE][GF_FIELD_SIZE];
    uint8_t         smult[GF_FIELD_SIZE][GF_FIELD_SIZE];
    uint16_t        mult[GF_FIELD_SIZE*GF_FIELD_SIZE];
};

struct gf_w4_logtable_data {
    uint8_t         log_tbl[GF_BASE_FIELD_SIZE];
    uint8_t         antilog_tbl[GF_BASE_FIELD_SIZE * 2];
    uint8_t         *antilog_tbl_div;
};

struct gf_w4_single_table_data {
    uint8_t         div[GF_BASE_FIELD_SIZE][GF_BASE_FIELD_SIZE];
    uint8_t         mult[GF_BASE_FIELD_SIZE][GF_BASE_FIELD_SIZE];
};

struct gf_w8_bytwo_data {
    uint64_t prim_poly;
    uint64_t mask1;
    uint64_t mask2;
};

int gf_w8_neon_cfm_init(gf_t *gf);
void gf_w8_neon_split_init(gf_t *gf);

#endif /* GF_COMPLETE_GF_W8_H */
