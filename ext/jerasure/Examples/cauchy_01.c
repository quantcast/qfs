/* *
 * Copyright (c) 2014, James S. Plank and Kevin Greenan
 * All rights reserved.
 *
 * Jerasure - A C/C++ Library for a Variety of Reed-Solomon and RAID-6 Erasure
 * Coding Techniques
 *
 * Revision 2.0: Galois Field backend now links to GF-Complete
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *  - Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 *  - Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 *  - Neither the name of the University of Tennessee nor the names of its
 *    contributors may be used to endorse or promote products derived
 *    from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
 * INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
 * BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
 * OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
 * AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
 * LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
 * WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

/* Jerasure's authors:
 
   Revision 2.x - 2014: James S. Plank and Kevin M. Greenan
   Revision 1.2 - 2008: James S. Plank, Scott Simmerman and Catherine D. Schuman.
   Revision 1.0 - 2007: James S. Plank
 */

#include <stdio.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "cauchy.h"
#include "jerasure.h"
#include "reed_sol.h"

#define talloc(type, num) (type *) malloc(sizeof(type)*(num))

static void usage(char *s)
{
  fprintf(stderr, "usage: cauchy_01 n w - Converts the value n to a bitmatrix using GF(2^w).\n");
  fprintf(stderr, "       \n");
  fprintf(stderr, "       It prints the bitmatrix, and reports on the numberof ones.\n");
  fprintf(stderr, "       Use 0x to input n in hexadecimal.\n");
  fprintf(stderr, "       W must be <= 32.\n");
  fprintf(stderr, "       \n");
  fprintf(stderr, "This demonstrates: cauchy_n_ones()\n");
  fprintf(stderr, "                   jerasure_matrix_to_bitmatrix()\n");
  fprintf(stderr, "                   jerasure_print_bitmatrix()\n");
  if (s != NULL) fprintf(stderr, "\n%s\n", s);
  exit(1);
}

int main(int argc, char **argv)
{
  int n;
  int i, no, w;
  int *bitmatrix;
  
  if (argc != 3) usage(NULL);
  if (sscanf(argv[1], "0x%x", &n) == 0) {
    if (sscanf(argv[1], "%d", &n) == 0) usage("Bad n");
  }
  if (sscanf(argv[2], "%d", &w) == 0 || w <= 0 || w > 32) usage("Bad w");
  if (w == 31) {
    if (n & 0x80000000L) usage("Bad n/w combination (n not between 0 and 2^w-1)\n");
  } else if (w < 31) {
    if (n >= (1 << w)) usage("Bad n/w combination (n not between 0 and 2^w-1)\n");
  }

  bitmatrix = jerasure_matrix_to_bitmatrix(1, 1, w, &n);
  printf("<HTML><title>cauchy_01 %u %d</title>\n", w, n);
  printf("<HTML><h3>cauchy_01 %u %d</h3>\n", w, n);
  printf("<pre>\n");
  if (w == 32) {
    printf("Converted the value 0x%x to the following bitmatrix:\n\n", n);
  } else {
    printf("Converted the value %d (0x%x) to the following bitmatrix:\n\n", n, n);
  }
  jerasure_print_bitmatrix(bitmatrix, w, w, w);
  printf("\n");

  no = 0;
  for (i = 0; i < w*w; i++) no += bitmatrix[i];
  if (no != cauchy_n_ones(n, w)) { 
    fprintf(stderr, "Jerasure error: # ones in the bitmatrix (%d) doesn't match cauchy_n_ones() (%d).\n",
       no, cauchy_n_ones(n, w));
    exit(1);
  }

  printf("# Ones: %d\n", cauchy_n_ones(n, w));

  return 0;
}
