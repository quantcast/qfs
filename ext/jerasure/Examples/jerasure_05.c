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

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <gf_rand.h>
#include "jerasure.h"

#define talloc(type, num) (type *) malloc(sizeof(type)*(num))

static void usage(char *s)
{
  fprintf(stderr, "usage: jerasure_05 k m w size seed - Does a simple Reed-Solomon coding example in GF(2^w).\n");
  fprintf(stderr, "       \n");
  fprintf(stderr, "       k+m must be <= 2^w.  w can be 8, 16 or 32.\n");
  fprintf(stderr, "       It sets up a Cauchy generator matrix and encodes\n");
  fprintf(stderr, "       k devices of size bytes with it.  Then it decodes.\n");
  fprintf(stderr, "       After that, it decodes device 0 by using jerasure_make_decoding_matrix()\n");
  fprintf(stderr, "       and jerasure_matrix_dotprod().\n");
  fprintf(stderr, "       \n");
  fprintf(stderr, "This demonstrates: jerasure_matrix_encode()\n");
  fprintf(stderr, "                   jerasure_matrix_decode()\n");
  fprintf(stderr, "                   jerasure_print_matrix()\n");
  fprintf(stderr, "                   jerasure_make_decoding_matrix()\n");
  fprintf(stderr, "                   jerasure_matrix_dotprod()\n");
  if (s != NULL) fprintf(stderr, "\n%s\n\n", s);
  exit(1);
}

static void print_data_and_coding(int k, int m, int w, int size, 
		char **data, char **coding) 
{
  int i, j, x;
  int n, sp;

  if(k > m) n = k;
  else n = m;
  sp = size * 2 + size/(w/8) + 8;

  printf("%-*sCoding\n", sp, "Data");
  for(i = 0; i < n; i++) {
	  if(i < k) {
		  printf("D%-2d:", i);
		  for(j=0;j< size; j+=(w/8)) { 
			  printf(" ");
			  for(x=0;x < w/8;x++){
				printf("%02x", (unsigned char)data[i][j+x]);
			  }
		  }
		  printf("    ");
	  }
	  else printf("%*s", sp, "");
	  if(i < m) {
		  printf("C%-2d:", i);
		  for(j=0;j< size; j+=(w/8)) { 
			  printf(" ");
			  for(x=0;x < w/8;x++){
				printf("%02x", (unsigned char)coding[i][j+x]);
			  }
		  }
	  }
	  printf("\n");
  }
	printf("\n");
}

int main(int argc, char **argv)
{
  int k, m, w, size;
  int i, j;
  int *matrix;
  char **data, **coding;
  int *erasures, *erased;
  int *decoding_matrix, *dm_ids;
  uint32_t seed;
  
  if (argc != 6) usage(NULL);
  if (sscanf(argv[1], "%d", &k) == 0 || k <= 0) usage("Bad k");
  if (sscanf(argv[2], "%d", &m) == 0 || m <= 0) usage("Bad m");
  if (sscanf(argv[3], "%d", &w) == 0 || (w != 8 && w != 16 && w != 32)) usage("Bad w");
  if (w < 32 && k + m > (1 << w)) usage("k + m must be <= 2 ^ w");
  if (sscanf(argv[4], "%d", &size) == 0 || size % sizeof(long) != 0) 
		usage("size must be multiple of sizeof(long)");
  if (sscanf(argv[5], "%d", &seed) == 0) usage("Bad seed");

  matrix = talloc(int, m*k);
  for (i = 0; i < m; i++) {
    for (j = 0; j < k; j++) {
      matrix[i*k+j] = galois_single_divide(1, i ^ (m + j), w);
    }
  }

  printf("<HTML><TITLE>jerasure_05");
  for (i = 1; i < argc; i++) printf(" %s", argv[i]);
  printf("</TITLE>\n");
  printf("<h3>jerasure_05");
  for (i = 1; i < argc; i++) printf(" %s", argv[i]);
  printf("</h3>\n");
  printf("<pre>\n");

  printf("The Coding Matrix (the last m rows of the Generator Matrix G^T):\n\n");
  jerasure_print_matrix(matrix, m, k, w);
  printf("\n");

  MOA_Seed(seed);
  data = talloc(char *, k);
  for (i = 0; i < k; i++) {
    data[i] = talloc(char, size);
    MOA_Fill_Random_Region(data[i], size);
  }

  coding = talloc(char *, m);
  for (i = 0; i < m; i++) {
    coding[i] = talloc(char, size);
  }

  jerasure_matrix_encode(k, m, w, matrix, data, coding, size);
  
  printf("Encoding Complete:\n\n");
  print_data_and_coding(k, m, w, size, data, coding);

  erasures = talloc(int, (m+1));
  erased = talloc(int, (k+m));
  for (i = 0; i < m+k; i++) erased[i] = 0;
  for (i = 0; i < m; ) {
    erasures[i] = (MOA_Random_W(w, 1))%(k+m);
    if (erased[erasures[i]] == 0) {
      erased[erasures[i]] = 1;
	  
      bzero((erasures[i] < k) ? data[erasures[i]] : coding[erasures[i]-k], size);
      i++;
    }
  }
  erasures[i] = -1;

  printf("Erased %d random devices:\n\n", m);
  print_data_and_coding(k, m, w, size, data, coding);
  
  i = jerasure_matrix_decode(k, m, w, matrix, 0, erasures, data, coding, size);

  printf("State of the system after decoding:\n\n");
  print_data_and_coding(k, m, w, size, data, coding);
  
  decoding_matrix = talloc(int, k*k);
  dm_ids = talloc(int, k);

  for (i = 0; i < m; i++) erased[i] = 1;
  for (; i < k+m; i++) erased[i] = 0;

  jerasure_make_decoding_matrix(k, m, w, matrix, erased, decoding_matrix, dm_ids);

  printf("Suppose we erase the first %d devices.  Here is the decoding matrix:\n\n", m);
  jerasure_print_matrix(decoding_matrix, k, k, w);
  printf("\n");
  printf("And dm_ids:\n\n");
  jerasure_print_matrix(dm_ids, 1, k, w);

  bzero(data[0], size);
  jerasure_matrix_dotprod(k, w, decoding_matrix, dm_ids, 0, data, coding, size);

  printf("\nAfter calling jerasure_matrix_dotprod, we calculate the value of device #0 to be:\n\n");
  printf("D0 :");
  for(i=0;i< size; i+=(w/8)) {
	  printf(" ");
	  for(j=0;j < w/8;j++){
		printf("%02x", (unsigned char)data[0][i+j]);
	  }
  }
  printf("\n\n");

  return 0;
}
