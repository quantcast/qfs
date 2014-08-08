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
#include <gf_rand.h>
#include "jerasure.h"
#include "reed_sol.h"

#define talloc(type, num) (type *) malloc(sizeof(type)*(num))

static void usage(char *s)
{
  fprintf(stderr, "usage: reed_sol_03 k w seed - Does a simple RAID-6 coding example in GF(2^w).\n");
  fprintf(stderr, "       \n");
  fprintf(stderr, "       w must be 8, 16 or 32.  k+2 must be <= 2^w.  It sets up a classic\n");
  fprintf(stderr, "       RAID-6 coding matrix based on Anvin's optimization and encodes\n");
  fprintf(stderr, "       %ld-byte devices with it.  Then it decodes.\n", sizeof(long));
  fprintf(stderr, "       \n");
  fprintf(stderr, "This demonstrates: reed_sol_r6_encode()\n");
  fprintf(stderr, "                   reed_sol_r6_coding_matrix()\n");
  fprintf(stderr, "                   jerasure_matrix_decode()\n");
  fprintf(stderr, "                   jerasure_print_matrix()\n");
  if (s != NULL) fprintf(stderr, "%s\n", s);
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
  long l;
  unsigned char uc;
  int k, w, i, j, m;
  int *matrix;
  char **data, **coding, **dcopy, **ccopy;
  int *erasures, *erased;
  uint32_t seed;
  
  if (argc != 4) usage(NULL);
  if (sscanf(argv[1], "%d", &k) == 0 || k <= 0) usage("Bad k");
  if (sscanf(argv[2], "%d", &w) == 0 || (w != 8 && w != 16 && w != 32)) usage("Bad w");
  if (sscanf(argv[3], "%d", &seed) == 0) usage("Bad seed");
  m = 2;
  if (w <= 16 && k + m > (1 << w)) usage("k + m is too big");

  MOA_Seed(seed);
  matrix = reed_sol_r6_coding_matrix(k, w);

  printf("<HTML><TITLE>reed_sol_03 %d %d %d</title>\n", k, w, seed);
  printf("<h3>reed_sol_03 %d %d %d</h3>\n", k, w, seed);
  printf("<pre>\n");

  printf("Last 2 rows of the Generator Matrix:\n\n");
  jerasure_print_matrix(matrix, m, k, w);
  printf("\n");

  data = talloc(char *, k);
  dcopy = talloc(char *, k);
  for (i = 0; i < k; i++) {
    data[i] = talloc(char, sizeof(long));
    dcopy[i] = talloc(char, sizeof(long));
    for (j = 0; j < sizeof(long); j++) {
      uc = MOA_Random_W(8, 1) %256;
      data[i][j] = (char) uc;   
    }
    memcpy(dcopy[i], data[i], sizeof(long));
  }

  coding = talloc(char *, m);
  ccopy = talloc(char *, m);
  for (i = 0; i < m; i++) {
    coding[i] = talloc(char, sizeof(long));
    ccopy[i] = talloc(char, sizeof(long));
  }

  reed_sol_r6_encode(k, w, data, coding, sizeof(long));
  for (i = 0; i < m; i++) {
    memcpy(ccopy[i], coding[i], sizeof(long));
  }
  
  printf("Encoding Complete:\n\n");
  print_data_and_coding(k, m, w, sizeof(long), data, coding);

  erasures = talloc(int, (m+1));
  erased = talloc(int, (k+m));
  for (i = 0; i < m+k; i++) erased[i] = 0;
  l = 0;
  for (i = 0; i < m; ) {
    erasures[i] = ((unsigned int) MOA_Random_W(w, 1))%(k+m);
    if (erased[erasures[i]] == 0) {
      erased[erasures[i]] = 1;
      memcpy((erasures[i] < k) ? data[erasures[i]] : coding[erasures[i]-k], &l, sizeof(long));
      i++;
    }
  }
  erasures[i] = -1;

  printf("Erased %d random devices:\n\n", m);
  print_data_and_coding(k, m, w, sizeof(long), data, coding);
  
  i = jerasure_matrix_decode(k, m, w, matrix, 1, erasures, data, coding, sizeof(long));

  printf("State of the system after decoding:\n\n");
  print_data_and_coding(k, m, w, sizeof(long), data, coding);
  
  for (i = 0; i < k; i++) if (memcmp(data[i], dcopy[i], sizeof(long)) != 0) {
    printf("ERROR: D%x after decoding does not match its state before decoding!<br>\n", i);
  }
  for (i = 0; i < m; i++) if (memcmp(coding[i], ccopy[i], sizeof(long)) != 0) {
    printf("ERROR: C%x after decoding does not match its state before decoding!<br>\n", i);
  }

  return 0;
}
