/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_example_1.c
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
  fprintf(stderr, "usage: gf_example_1 w - w must be between 1 and 32\n");
  exit(1);
}

int main(int argc, char **argv)
{
  uint32_t a, b, c;
  int w;
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
  
  exit(0);
}
