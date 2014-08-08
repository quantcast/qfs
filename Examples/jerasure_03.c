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

   Revision 2.x - 2014: James S. Plank and Kevin M. Greenan.
   Revision 1.2 - 2008: James S. Plank, Scott Simmerman and Catherine D. Schuman.
   Revision 1.0 - 2007: James S. Plank.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "jerasure.h"

#define talloc(type, num) (type *) malloc(sizeof(type)*(num))

static void usage(char *s)
{
  fprintf(stderr, "usage: jerasure_03 k w - Creates a kxk Cauchy matrix in GF(2^w). \n\n");
  fprintf(stderr, "       k must be < 2^w.  Element i,j is 1/(i+(2^w-j-1)).  (If that is\n");
  fprintf(stderr, "       If that is 1/0, then it sets it to zero).  \n");
  fprintf(stderr, "       It then tests whether that matrix is invertible.\n");
  fprintf(stderr, "       If it is invertible, then it prints out the inverse.\n");
  fprintf(stderr, "       Finally, it prints the product of the matrix and its inverse.\n");
  fprintf(stderr, "       \n");
  fprintf(stderr, "This demonstrates: jerasure_print_matrix()\n");
  fprintf(stderr, "                   jerasure_invertible_matrix()\n");
  fprintf(stderr, "                   jerasure_invert_matrix()\n");
  fprintf(stderr, "                   jerasure_matrix_multiply().\n");
  if (s != NULL) fprintf(stderr, "%s\n", s);
  exit(1);
}

int main(int argc, char **argv)
{
  unsigned int k, w, i, j, n;
  int *matrix;
  int *matrix_copy;
  int *inverse;
  int *identity;

  if (argc != 3) usage(NULL);
  if (sscanf(argv[1], "%d", &k) == 0 || k <= 0) usage("Bad k");
  if (sscanf(argv[2], "%d", &w) == 0 || w <= 0 || w > 31) usage("Bad w");
  if (k >= (1 << w)) usage("K too big");

  matrix = talloc(int, k*k);
  matrix_copy = talloc(int, k*k);
  inverse = talloc(int, k*k);

  for (i = 0; i < k; i++) {
    for (j = 0; j < k; j++) {
      n = i ^ ((1 << w)-1-j);
      matrix[i*k+j] = (n == 0) ? 0 : galois_single_divide(1, n, w);
    }
  }

  printf("<HTML><TITLE>jerasure_03");
  for (i = 1; i < argc; i++) printf(" %s", argv[i]);
  printf("</TITLE>\n");
  printf("<h3>jerasure_03");
  for (i = 1; i < argc; i++) printf(" %s", argv[i]);
  printf("</h3>\n");
  printf("<pre>\n");

  printf("The Cauchy Matrix:\n");
  jerasure_print_matrix(matrix, k, k, w);
  memcpy(matrix_copy, matrix, sizeof(int)*k*k);
  i = jerasure_invertible_matrix(matrix_copy, k, w);
  printf("\nInvertible: %s\n", (i == 1) ? "Yes" : "No");
  if (i == 1) {
    printf("\nInverse:\n");
    memcpy(matrix_copy, matrix, sizeof(int)*k*k);
    i = jerasure_invert_matrix(matrix_copy, inverse, k, w);
    jerasure_print_matrix(inverse, k, k, w);
    identity = jerasure_matrix_multiply(inverse, matrix, k, k, k, k, w);
    printf("\nInverse times matrix (should be identity):\n");
    jerasure_print_matrix(identity, k, k, w);
  }
  return 0;
}

