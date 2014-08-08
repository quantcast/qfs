/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_example_3.c
 *
 * Identical to example_2 except it works in GF(2^64)
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
  fprintf(stderr, "usage: gf_example_3\n");
  exit(1);
}

int main(int argc, char **argv)
{
  uint64_t a, b, c;
  uint64_t *r1, *r2;
  int i;
  gf_t gf;

  if (argc != 1) usage(NULL);

  /* Get two random numbers in a and b */

  MOA_Seed(time(0));
  a = MOA_Random_64();
  b = MOA_Random_64();

  /* Create the proper instance of the gf_t object using defaults: */

  gf_init_easy(&gf, 64);

  /* And multiply a and b using the galois field: */

  c = gf.multiply.w64(&gf, a, b);
  printf("%llx * %llx = %llx\n", (long long unsigned int) a, (long long unsigned int) b, (long long unsigned int) c);

  /* Divide the product by a and b */

  printf("%llx / %llx = %llx\n", (long long unsigned int) c, (long long unsigned int) a, (long long unsigned int) gf.divide.w64(&gf, c, a));
  printf("%llx / %llx = %llx\n", (long long unsigned int) c, (long long unsigned int) b, (long long unsigned int) gf.divide.w64(&gf, c, b));

  r1 = (uint64_t *) malloc(32);
  r2 = (uint64_t *) malloc(32);

  r1[0] = b;

  for (i = 1; i < 4; i++) r1[i] = MOA_Random_64();

  gf.multiply_region.w64(&gf, r1, r2, a, 32, 0);

  printf("\nmultiply_region by %llx\n\n", (long long unsigned int) a);
  printf("R1 (the source):  ");
  for (i = 0; i < 4; i++) printf(" %016llx", (long long unsigned int) r1[i]);

  printf("\nR2 (the product): ");
  for (i = 0; i < 4; i++) printf(" %016llx", (long long unsigned int) r2[i]);
  printf("\n");

  exit(0);
}
