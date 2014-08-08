/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_example_2.c
 *
 * Demonstrates using the procedures for examples in GF(2^w) for w <= 32.
 */

#include <stdio.h>
#include <getopt.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#include "gf_complete.h"
#include "gf_rand.h"

void usage(char *s)
{
  fprintf(stderr, "usage: gf_example_2 w - w must be between 1 and 32\n");
  exit(1);
}

int main(int argc, char **argv)
{
  uint32_t a, b, c;
  uint8_t *r1, *r2;
  uint16_t *r16 = NULL;
  uint32_t *r32 = NULL;
  int w, i;
  gf_t gf;

  if (argc != 2) usage(NULL);
  w = atoi(argv[1]);
  if (w <= 0 || w > 32) usage("Bad w");

  /* Get two random numbers in a and b */

  MOA_Seed(time(0));
  a = MOA_Random_W(w, 0);
  b = MOA_Random_W(w, 0);

  /* Create the proper instance of the gf_t object using defaults: */

  gf_init_easy(&gf, w);

  /* And multiply a and b using the galois field: */

  c = gf.multiply.w32(&gf, a, b);
  printf("%u * %u = %u\n", a, b, c);

  /* Divide the product by a and b */

  printf("%u / %u = %u\n", c, a, gf.divide.w32(&gf, c, a));
  printf("%u / %u = %u\n", c, b, gf.divide.w32(&gf, c, b));

  /* If w is 4, 8, 16 or 32, do a very small region operation */

  if (w == 4 || w == 8 || w == 16 || w == 32) {
    r1 = (uint8_t *) malloc(16);
    r2 = (uint8_t *) malloc(16);

    if (w == 4 || w == 8) {
      r1[0] = b;
      for (i = 1; i < 16; i++) r1[i] = MOA_Random_W(8, 1);
    } else if (w == 16) {
      r16 = (uint16_t *) r1;
      r16[0] = b;
      for (i = 1; i < 8; i++) r16[i] = MOA_Random_W(16, 1);
    } else {
      r32 = (uint32_t *) r1;
      r32[0] = b;
      for (i = 1; i < 4; i++) r32[i] = MOA_Random_W(32, 1);
    }

    gf.multiply_region.w32(&gf, r1, r2, a, 16, 0);
  
    printf("\nmultiply_region by 0x%x (%u)\n\n", a, a);
    printf("R1 (the source):  ");
    if (w == 4) {
      for (i = 0; i < 16; i++) printf(" %x %x", r1[i] >> 4, r1[i] & 0xf);
    } else if (w == 8) {
      for (i = 0; i < 16; i++) printf(" %02x", r1[i]);
    } else if (w == 16) {
      for (i = 0; i < 8; i++) printf(" %04x", r16[i]);
    } else if (w == 32) {
      for (i = 0; i < 4; i++) printf(" %08x", r32[i]);
    }
    printf("\nR2 (the product): ");
    if (w == 4) {
      for (i = 0; i < 16; i++) printf(" %x %x", r2[i] >> 4, r2[i] & 0xf);
    } else if (w == 8) {
      for (i = 0; i < 16; i++) printf(" %02x", r2[i]);
    } else if (w == 16) {
      r16 = (uint16_t *) r2;
      for (i = 0; i < 8; i++) printf(" %04x", r16[i]);
    } else if (w == 32) {
      r32 = (uint32_t *) r2;
      for (i = 0; i < 4; i++) printf(" %08x", r32[i]);
    }
    printf("\n");
  }
  exit(0);
}
