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
#include <gf_complete.h>
#include <gf_method.h>
#include <gf_rand.h>
#include <stdint.h>
#include <sys/time.h>
#include "jerasure.h"
#include "reed_sol.h"

#define BUFSIZE 4096

static void *malloc16(int size) {
    void *mem = malloc(size+16+sizeof(void*));
    void **ptr = (void**)((long)(mem+16+sizeof(void*)) & ~(15));
    ptr[-1] = mem;
    return ptr;
}

#if 0
// Unused for now.
static void free16(void *ptr) {
    free(((void**)ptr)[-1]);
}
#endif

#define talloc(type, num) (type *) malloc16(sizeof(type)*(num))

static void usage(char *s)
{
  fprintf(stderr, "usage: reed_sol_test_gf k m w seed (additional GF args) - Tests Reed-Solomon in GF(2^w).\n");
  fprintf(stderr, "       \n");
  fprintf(stderr, "       w must be 8, 16 or 32.  k+m must be <= 2^w.\n");
  fprintf(stderr, "       See the README for information on the additional GF args.\n");
  fprintf(stderr, "       Set up a Vandermonde-based distribution matrix and encodes k devices of\n");
  fprintf(stderr, "       %d bytes each with it.  Then it decodes.\n", BUFSIZE);
  fprintf(stderr, "       \n");
  fprintf(stderr, "This tests:        jerasure_matrix_encode()\n");
  fprintf(stderr, "                   jerasure_matrix_decode()\n");
  fprintf(stderr, "                   jerasure_print_matrix()\n");
  fprintf(stderr, "                   galois_change_technique()\n");
  fprintf(stderr, "                   reed_sol_vandermonde_coding_matrix()\n");
  if (s != NULL) fprintf(stderr, "%s\n", s);
  exit(1);
}

gf_t* get_gf(int w, int argc, char **argv, int starting)
{
  gf_t *gf = (gf_t*)malloc(sizeof(gf_t));
  if (create_gf_from_argv(gf, w, argc, argv, starting) == 0) {
    free(gf);
    gf = NULL;
  }
  return gf;
}

int main(int argc, char **argv)
{
  int k, w, i, m;
  int *matrix;
  char **data, **coding, **old_values;
  int *erasures, *erased;
  gf_t *gf = NULL;
  uint32_t seed;
  
  if (argc < 6) usage("Not enough command line arguments");  
  if (sscanf(argv[1], "%d", &k) == 0 || k <= 0) usage("Bad k");
  if (sscanf(argv[2], "%d", &m) == 0 || m <= 0) usage("Bad m");
  if (sscanf(argv[3], "%d", &w) == 0 || (w != 8 && w != 16 && w != 32)) usage("Bad w");
  if (sscanf(argv[4], "%d", &seed) == 0) usage("Bad seed");
  if (w <= 16 && k + m > (1 << w)) usage("k + m is too big");

  MOA_Seed(seed);

  gf = get_gf(w, argc, argv, 5); 

  if (gf == NULL) {
    usage("Invalid arguments given for GF!\n");
  }

  galois_change_technique(gf, w); 

  matrix = reed_sol_vandermonde_coding_matrix(k, m, w);

  printf("<HTML><TITLE>reed_sol_test_gf");
  for (i = 1; i < argc; i++) printf(" %s", argv[i]);
  printf("</TITLE>\n");
  printf("<h3>reed_sol_test_gf");
  for (i = 1; i < argc; i++) printf(" %s", argv[i]);
  printf("</h3>\n");
  printf("<pre>\n");

  printf("Last m rows of the generator matrix (G^T):\n\n");
  jerasure_print_matrix(matrix, m, k, w);
  printf("\n");

  data = talloc(char *, k);
  for (i = 0; i < k; i++) {
    data[i] = talloc(char, BUFSIZE);
    MOA_Fill_Random_Region(data[i], BUFSIZE);
  }

  coding = talloc(char *, m);
  old_values = talloc(char *, m);
  for (i = 0; i < m; i++) {
    coding[i] = talloc(char, BUFSIZE);
    old_values[i] = talloc(char, BUFSIZE);
  }

  jerasure_matrix_encode(k, m, w, matrix, data, coding, BUFSIZE);
  
  erasures = talloc(int, (m+1));
  erased = talloc(int, (k+m));
  for (i = 0; i < m+k; i++) erased[i] = 0;
  for (i = 0; i < m; ) {
    erasures[i] = ((unsigned int)MOA_Random_W(w,1))%(k+m);
    if (erased[erasures[i]] == 0) {
      erased[erasures[i]] = 1;
      memcpy(old_values[i], (erasures[i] < k) ? data[erasures[i]] : coding[erasures[i]-k], BUFSIZE);
      bzero((erasures[i] < k) ? data[erasures[i]] : coding[erasures[i]-k], BUFSIZE);
      i++;
    }
  }
  erasures[i] = -1;

  i = jerasure_matrix_decode(k, m, w, matrix, 1, erasures, data, coding, BUFSIZE);

  for (i = 0; i < m; i++) {
    if (erasures[i] < k) {
      if (memcmp(data[erasures[i]], old_values[i], BUFSIZE)) {
        fprintf(stderr, "Decoding failed for %d!\n", erasures[i]);
        exit(1);
      }
    } else {
      if (memcmp(coding[erasures[i]-k], old_values[i], BUFSIZE)) {
        fprintf(stderr, "Decoding failed for %d!\n", erasures[i]);
        exit(1);
      }
    }
  }
  
  printf("Encoding and decoding were both successful.\n");
  return 0;
}
