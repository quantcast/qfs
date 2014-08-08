/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_example_7.c
 *
 * Demonstrating extract_word and Cauchy
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
  fprintf(stderr, "usage: gf_example_7\n");
  exit(1);
}

int main(int argc, char **argv)
{
  uint8_t *a, *b;
  int i, j;
  gf_t gf;

  if (gf_init_hard(&gf, 3, GF_MULT_TABLE, GF_REGION_CAUCHY, GF_DIVIDE_DEFAULT, 0, 0, 0, NULL, NULL) == 0) {
    fprintf(stderr, "gf_init_hard failed\n");
    exit(1);
  }

  a = (uint8_t *) malloc(3);
  b = (uint8_t *) malloc(3);

  MOA_Seed(0);

  for (i = 0; i < 3; i++) a[i] = MOA_Random_W(8, 1);

  gf.multiply_region.w32(&gf, a, b, 5, 3, 0);

  printf("a: 0x%lx    b: 0x%lx\n", (unsigned long) a, (unsigned long) b);

  printf("\n");
  printf("a: 0x%02x 0x%02x 0x%02x\n", a[0], a[1], a[2]);
  printf("b: 0x%02x 0x%02x 0x%02x\n", b[0], b[1], b[2]);
  printf("\n");

  printf("a bits:");
  for (i = 0; i < 3; i++) {
    printf(" ");
    for (j = 7; j >= 0; j--) printf("%c", (a[i] & (1 << j)) ? '1' : '0');
  }
  printf("\n");

  printf("b bits:");
  for (i = 0; i < 3; i++) {
    printf(" ");
    for (j = 7; j >= 0; j--) printf("%c", (b[i] & (1 << j)) ? '1' : '0');
  }
  printf("\n");

  printf("\n");
  for (i = 0; i < 8; i++) {
    printf("Word %2d: %d * 5 = %d\n", i,
           gf.extract_word.w32(&gf, a, 3, i),
           gf.extract_word.w32(&gf, b, 3, i));
  }
  return 0;
}
