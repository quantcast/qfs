/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_example_4.c
 *
 * Identical to example_3 except it works in GF(2^128)
 */

#include <stdio.h>
#include <getopt.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

#include "gf_complete.h"
#include "gf_rand.h"

#define LLUI (long long unsigned int) 

void usage(char *s)
{
  fprintf(stderr, "usage: gf_example_3\n");
  exit(1);
}

int main(int argc, char **argv)
{
  uint64_t a[2], b[2], c[2];
  uint64_t *r1, *r2;
  int i;
  gf_t gf;

  if (argc != 1) usage(NULL);

  /* Get two random numbers in a and b */

  MOA_Seed(time(0));
  MOA_Random_128(a);
  MOA_Random_128(b);

  /* Create the proper instance of the gf_t object using defaults: */

  gf_init_easy(&gf, 128);

  /* And multiply a and b using the galois field: */

  gf.multiply.w128(&gf, a, b, c);
  printf("%016llx%016llx * %016llx%016llx =\n%016llx%016llx\n", 
      LLUI a[0], LLUI a[1], LLUI b[0], LLUI b[1], LLUI c[0], LLUI c[1]);

  r1 = (uint64_t *) malloc(32);
  r2 = (uint64_t *) malloc(32);

  for (i = 0; i < 4; i++) r1[i] = MOA_Random_64();

  gf.multiply_region.w128(&gf, r1, r2, a, 32, 0);

  printf("\nmultiply_region by %016llx%016llx\n\n", LLUI a[0], LLUI a[1]);
  printf("R1 (the source):  ");
  for (i = 0; i < 4; i += 2) printf(" %016llx%016llx", LLUI r1[i], LLUI r1[i+1]);

  printf("\nR2 (the product): ");
  for (i = 0; i < 4; i += 2) printf(" %016llx%016llx", LLUI r2[i], LLUI r2[i+1]);
  printf("\n");
  exit(0);
}
