/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_div.c
 *
 * Multiplies two numbers in gf_2^w
 */

#include <stdio.h>
#include <getopt.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>

#include "gf_complete.h"
#include "gf_method.h"
#include "gf_general.h"

void usage(int why)
{
  fprintf(stderr, "usage: gf_div a b w [method] - does division of a and b in GF(2^w)\n");
  if (why == 'W') {
    fprintf(stderr, "Bad w.\n");
    fprintf(stderr, "Legal w are: 1 - 32, 64 and 128.\n");
    fprintf(stderr, "Append 'h' to w to treat a, b and the quotient as hexadecimal.\n");
    fprintf(stderr, "w=128 is hex only (i.e. '128' will be an error - do '128h')\n");
  }
  if (why == 'A') fprintf(stderr, "Bad a\n");
  if (why == 'B') fprintf(stderr, "Bad b\n");
  if (why == 'M') {
    fprintf(stderr, "Bad Method Specification: ");
    gf_error();
  }
  exit(1);
}

int main(int argc, char **argv)
{
  int hex, w;
  gf_t gf;
  gf_general_t a, b, c;
  char output[50];

  if (argc < 4) usage(' ');

  if (sscanf(argv[3], "%d", &w) == 0) usage('W');
  if (w <= 0 || (w > 32 && w != 64 && w != 128)) usage('W');

  hex = (strchr(argv[3], 'h') != NULL);
  if (!hex && w == 128) usage('W');

  if (argc == 4) {
    if (gf_init_easy(&gf, w) == 0) usage('M');
  } else {
    if (create_gf_from_argv(&gf, w, argc, argv, 4) == 0) usage('M');
  }
 
  if (!gf_general_s_to_val(&a, w, argv[1], hex)) usage('A');
  if (!gf_general_s_to_val(&b, w, argv[2], hex)) usage('B');

  gf_general_divide(&gf, &a, &b, &c);
  gf_general_val_to_s(&c, w, output, hex);
  
  printf("%s\n", output);
  exit(0);
}
