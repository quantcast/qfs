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
#include <stdint.h>
#include <string.h>
#include <gf_rand.h>
#include "jerasure.h"
#include "reed_sol.h"

#define talloc(type, num) (type *) malloc(sizeof(type)*(num))

static void usage(char *s)
{
  fprintf(stderr, "usage: reed_sol_04 w seed - Shows reed_sol_galois_wXX_region_multby_2\n");
  fprintf(stderr, "       \n");
  fprintf(stderr, "       w must be 8, 16 or 32.  Sets up an array of 4 random words in\n");
  fprintf(stderr, "       GF(2^w) and multiplies them by two.  \n");
  fprintf(stderr, "       \n");
  fprintf(stderr, "This demonstrates: reed_sol_galois_w08_region_multby_2()\n");
  fprintf(stderr, "                   reed_sol_galois_w16_region_multby_2()\n");
  fprintf(stderr, "                   reed_sol_galois_w32_region_multby_2()\n");
  if (s != NULL) fprintf(stderr, "%s\n", s);
  exit(1);
}

int main(int argc, char **argv)
{
  unsigned char *x, *y;
  unsigned short *xs, *ys;
  unsigned int *xi, *yi;
  uint32_t seed;
  int *a32, *copy;
  int i;
  int w;
  
  if (argc != 3) usage(NULL);
  if (sscanf(argv[1], "%d", &w) == 0 || (w != 8 && w != 16 && w != 32)) usage("Bad w");
  if (sscanf(argv[2], "%d", &seed) == 0) usage("Bad seed");

  printf("<HTML><TITLE>reed_sol_04 %d %d</title>\n", w, seed);
  printf("<h3>reed_sol_04 %d %d</h3>\n", w, seed);
  printf("<pre>\n");

  MOA_Seed(seed);
  a32 = talloc(int, 4);
  copy = talloc(int, 4);
  y = (unsigned char *) a32;
  for (i = 0; i < 4*sizeof(int); i++) y[i] = MOA_Random_W(8, 1);
  memcpy(copy, a32, sizeof(int)*4);

  if (w == 8) {
    x = (unsigned char *) copy;
    y = (unsigned char *) a32;
    reed_sol_galois_w08_region_multby_2((char *) a32, sizeof(int)*4);
    for (i = 0; i < 4*sizeof(int)/sizeof(char); i++) {
       printf("Char %2d: %3u *2 = %3u\n", i, x[i], y[i]);
    }
  } else if (w == 16) {
    xs = (unsigned short *) copy;
    ys = (unsigned short *) a32;
    reed_sol_galois_w16_region_multby_2((char *) a32, sizeof(int)*4);
    for (i = 0; i < 4*sizeof(int)/sizeof(short); i++) {
       printf("Short %2d: %5u *2 = %5u\n", i, xs[i], ys[i]);
    }
  } else if (w == 32) {
    xi = (unsigned int *) copy;
    yi = (unsigned int *) a32;
    reed_sol_galois_w16_region_multby_2((char *) a32, sizeof(int)*4);
    for (i = 0; i < 4*sizeof(int)/sizeof(int); i++) {
       printf("Int %2d: %10u *2 = %10u\n", i, xi[i], yi[i]);
    }
  } 

  return 0;
}
