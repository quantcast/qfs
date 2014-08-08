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
#include "reed_sol.h"

#define talloc(type, num) (type *) malloc(sizeof(type)*(num))

static void usage(char *s)
{
  fprintf(stderr, "usage: reed_sol_02 k m w - Vandermonde matrices in GF(2^w).\n");
  fprintf(stderr, "       \n");
  fprintf(stderr, "       k+m must be <= 2^w.  This simply prints out the \n");
  fprintf(stderr, "       Vandermonde matrix in GF(2^w), and then the generator\n");
  fprintf(stderr, "       matrix that is constructed from it.  See [Plank-Ding-05] for\n");
  fprintf(stderr, "       information on how this construction proceeds\n");
  fprintf(stderr, "       \n");
  fprintf(stderr, "This demonstrates: reed_sol_extended_vandermonde_matrix()\n");
  fprintf(stderr, "                   reed_sol_big_vandermonde_coding_matrix()\n");
  fprintf(stderr, "                   reed_sol_vandermonde_coding_matrix()\n");
  fprintf(stderr, "                   jerasure_print_matrix()\n");
  if (s != NULL) fprintf(stderr, "%s\n", s);
  exit(1);
}

int main(int argc, char **argv)
{
  int k, w, m;
  int *matrix;
  
  if (argc != 4) usage(NULL);
  if (sscanf(argv[1], "%d", &k) == 0 || k <= 0) usage("Bad k");
  if (sscanf(argv[2], "%d", &m) == 0 || m <= 0) usage("Bad m");
  if (sscanf(argv[3], "%d", &w) == 0 || w <= 0 || w > 32) usage("Bad w");
  if (w <= 30 && k + m > (1 << w)) usage("k + m is too big");

  matrix = reed_sol_extended_vandermonde_matrix(k+m, k, w);

  printf("<HTML><TITLE>reed_sol_02 %d %d %d</title>\n", k, m, w);
  printf("<h3>reed_sol_02 %d %d %d</h3>\n", k, m, w);
  printf("<pre>\n");
  printf("Extended Vandermonde Matrix:\n\n");
  jerasure_print_matrix(matrix, k+m, k, w);
  printf("\n");

  matrix = reed_sol_big_vandermonde_distribution_matrix(k+m, k, w);
  printf("Vandermonde Generator Matrix (G^T):\n\n");
  jerasure_print_matrix(matrix, k+m, k, w);
  printf("\n");

  matrix = reed_sol_vandermonde_coding_matrix(k, m, w);
  printf("Vandermonde Coding Matrix:\n\n");
  jerasure_print_matrix(matrix, m, k, w);
  printf("\n");

  
  return 0;
}
