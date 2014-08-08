/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_example_5.c
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
  fprintf(stderr, "usage: gf_example_5\n");
  exit(1);
}

int main(int argc, char **argv)
{
  uint16_t *a, *b;
  int i, j;
  gf_t gf;

  if (gf_init_hard(&gf, 16, GF_MULT_SPLIT_TABLE, GF_REGION_ALTMAP, GF_DIVIDE_DEFAULT, 
                   0, 16, 4, NULL, NULL) == 0) {
    fprintf(stderr, "gf_init_hard failed\n");
    exit(1);
  }

  a = (uint16_t *) malloc(200);
  b = (uint16_t *) malloc(200);

  a += 6;
  b += 6;

  MOA_Seed(0);

  for (i = 0; i < 30; i++) a[i] = MOA_Random_W(16, 1);

  gf.multiply_region.w32(&gf, a, b, 0x1234, 30*2, 0);

  printf("a: 0x%lx    b: 0x%lx\n", (unsigned long) a, (unsigned long) b);

  for (i = 0; i < 30; i += 10) {
    printf("\n");
    printf("  ");
    for (j = 0; j < 10; j++) printf(" %4d", i+j);
    printf("\n");

    printf("a:");
    for (j = 0; j < 10; j++) printf(" %04x", a[i+j]);
    printf("\n");

    printf("b:");
    for (j = 0; j < 10; j++) printf(" %04x", b[i+j]);
    printf("\n");
    printf("\n");
  }

  for (i = 0; i < 15; i ++) {
    printf("Word %2d: 0x%04x * 0x1234 = 0x%04x    ", i,
           gf.extract_word.w32(&gf, a, 30*2, i),
           gf.extract_word.w32(&gf, b, 30*2, i));
    printf("Word %2d: 0x%04x * 0x1234 = 0x%04x\n", i+15,
           gf.extract_word.w32(&gf, a, 30*2, i+15),
           gf.extract_word.w32(&gf, b, 30*2, i+15));
  }
  return 0;
}
