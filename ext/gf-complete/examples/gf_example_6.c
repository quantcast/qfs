/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_example_6.c
 *
 * Demonstrating altmap and extract_word
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
  fprintf(stderr, "usage: gf_example_6\n");
  exit(1);
}

int main(int argc, char **argv)
{
  uint32_t *a, *b;
  int i, j;
  gf_t gf, gf_16;

  if (gf_init_hard(&gf_16, 16, GF_MULT_LOG_TABLE, GF_REGION_DEFAULT, GF_DIVIDE_DEFAULT,
                   0, 0, 0, NULL, NULL) == 0) {
    fprintf(stderr, "gf_init_hard (6) failed\n");
    exit(1);
  }

  if (gf_init_hard(&gf, 32, GF_MULT_COMPOSITE, GF_REGION_ALTMAP, GF_DIVIDE_DEFAULT, 
                   0, 2, 0, &gf_16, NULL) == 0) {
    fprintf(stderr, "gf_init_hard (32) failed\n");
    exit(1);
  }

  a = (uint32_t *) malloc(200);
  b = (uint32_t *) malloc(200);

  a += 3;
  b += 3;

  MOA_Seed(0);

  for (i = 0; i < 30; i++) a[i] = MOA_Random_W(32, 1);

  gf.multiply_region.w32(&gf, a, b, 0x12345678, 30*4, 0);

  printf("a: 0x%lx    b: 0x%lx\n", (unsigned long) a, (unsigned long) b);

  for (i = 0; i < 30; i += 10) {
    printf("\n");
    printf("  ");
    for (j = 0; j < 10; j++) printf(" %8d", i+j);
    printf("\n");

    printf("a:");
    for (j = 0; j < 10; j++) printf(" %08x", a[i+j]);
    printf("\n");

    printf("b:");
    for (j = 0; j < 10; j++) printf(" %08x", b[i+j]);
    printf("\n");
    printf("\n");
  }

  for (i = 0; i < 15; i ++) {
    printf("Word %2d: 0x%08x * 0x12345678 = 0x%08x    ", i,
           gf.extract_word.w32(&gf, a, 30*4, i),
           gf.extract_word.w32(&gf, b, 30*4, i));
    printf("Word %2d: 0x%08x * 0x12345678 = 0x%08x\n", i+15,
           gf.extract_word.w32(&gf, a, 30*4, i+15),
           gf.extract_word.w32(&gf, b, 30*4, i+15));
  }
  return 0;
}
