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
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <gf_rand.h>
#include "jerasure.h"

#define talloc(type, num) (type *) malloc(sizeof(type)*(num))

static void usage(char *s)
{
  fprintf(stderr, "usage: jerasure_08 k w seed - Example schedule cache usage with RAID-6\n");
  fprintf(stderr, "       \n");
  fprintf(stderr, "       m=2.  k+m must be <= 2^w.  It sets up a RAID-6 generator matrix and encodes\n");
  fprintf(stderr, "       k sets of w*%ld bytes. It creates a schedule cache for decoding.\n", sizeof(long));
  fprintf(stderr, "       It demonstrates using the schedule cache for both encoding and decoding.\n");
  fprintf(stderr, "       Then it demonstrates using jerasure_do_parity() to re-encode the first.\n");
  fprintf(stderr, "       coding device\n");
  fprintf(stderr, "       \n");
  fprintf(stderr, "This demonstrates: jerasure_generate_schedule_cache()\n");
  fprintf(stderr, "                   jerasure_smart_bitmatrix_to_schedule()\n");
  fprintf(stderr, "                   jerasure_schedule_encode()\n");
  fprintf(stderr, "                   jerasure_schedule_decode_cache()\n");
  fprintf(stderr, "                   jerasure_free_schedule()\n");
  fprintf(stderr, "                   jerasure_free_schedule_cache()\n");
  fprintf(stderr, "                   jerasure_get_stats()\n");
  fprintf(stderr, "                   jerasure_do_parity()\n");
  if (s != NULL) fprintf(stderr, "%s\n", s);
  exit(1);
}

static void print_array(char **ptrs, int ndevices, int size, int packetsize, char *label)
{
  int i, j, x;
  unsigned char *up;

  printf("<center><table border=3 cellpadding=3><tr><td></td>\n");

  for (i = 0; i < ndevices; i++) printf("<td align=center>%s%x</td>\n", label, i);
  printf("</tr>\n");
  printf("<td align=right><pre>");
  for (j = 0; j < size/packetsize; j++) printf("Packet %d\n", j);
  printf("</pre></td>\n");
  for (i = 0; i < ndevices; i++) {
    printf("<td><pre>");
    up = (unsigned char *) ptrs[i];
    for (j = 0; j < size/packetsize; j++) {
      for (x = 0; x < packetsize; x++) {
        if (x > 0 && x%4 == 0) printf(" ");
        printf("%02x", up[j*packetsize+x]);
      }
      printf("\n");
    }
    printf("</td>\n");
  }
  printf("</tr></table></center>\n");
}

int main(int argc, char **argv)
{
  int k, w, i, j, m;
  int *matrix, *bitmatrix;
  char **data, **coding;
  int **smart, ***cache;
  int *erasures, *erased;
  double stats[3];
  uint32_t seed;
  
  if (argc != 4) usage("Wrong number of arguments");
  if (sscanf(argv[1], "%d", &k) == 0 || k <= 0) usage("Bad k");
  if (sscanf(argv[2], "%d", &w) == 0 || w <= 0 || w > 32) usage("Bad m");
  if (sscanf(argv[3], "%d", &seed) == 0) usage("Bad seed");
  m = 2;
  if (w < 30 && (k+m) > (1 << w)) usage("k + m is too big");

  MOA_Seed(seed);

  matrix = talloc(int, m*k);
  for (j = 0; j < k; j++) matrix[j] = 1;
  i = 1;
  for (j = 0; j < k; j++) {
    matrix[k+j] = i;
    i = galois_single_multiply(i, 2, w);
  }
  bitmatrix = jerasure_matrix_to_bitmatrix(k, m, w, matrix);

  smart = jerasure_smart_bitmatrix_to_schedule(k, m, w, bitmatrix);
  cache = jerasure_generate_schedule_cache(k, m, w, bitmatrix, 1);

  data = talloc(char *, k);
  for (i = 0; i < k; i++) {
    data[i] = talloc(char, sizeof(long)*w);
    MOA_Fill_Random_Region(data[i], sizeof(long)*w);
  }

  coding = talloc(char *, m);
  for (i = 0; i < m; i++) {
    coding[i] = talloc(char, sizeof(long)*w);
  }

  jerasure_schedule_encode(k, m, w, smart, data, coding, w*sizeof(long), sizeof(long));
  jerasure_get_stats(stats);

  printf("<HTML><TITLE>jerasure_08");
  for (i = 1; i < argc; i++) printf(" %s", argv[i]);
  printf("</TITLE>\n");
  printf("<h3>jerasure_08");
  for (i = 1; i < argc; i++) printf(" %s", argv[i]);
  printf("</h3>\n");
  printf("<hr>\n");

  printf("Encoding Complete: - %.0lf XOR'd bytes.  Here is the state of the system:\n<p>\n", stats[0]);
  printf("<p>\n");
  print_array(data, k, sizeof(long)*w, sizeof(long), "D");
  printf("<p>\n");
  print_array(coding, m, sizeof(long)*w, sizeof(long), "C");
  printf("<hr>\n");

  erasures = talloc(int, (m+1));
  erasures[0] = k;
  erasures[1] = k+1;
  erasures[2] = -1;
  for (j = 0; j < m; j++) bzero(coding[j], sizeof(long)*w);

  jerasure_schedule_decode_cache(k, m, w, cache, erasures, data, coding, w*sizeof(long), sizeof(long));
  jerasure_get_stats(stats);
  printf("Encoding Using the Schedule Cache: - %.0lf XOR'd bytes\n\n", stats[0]);
  printf("<p>\n");
  print_array(data, k, sizeof(long)*w, sizeof(long), "D");
  printf("<p>\n");
  print_array(coding, m, sizeof(long)*w, sizeof(long), "C");
  printf("<hr>\n");

  erased = talloc(int, (k+m));
  for (i = 0; i < m+k; i++) erased[i] = 0;
  for (i = 0; i < m; ) {
    erasures[i] = MOA_Random_W(w, 1)%(k+m);
    if (erased[erasures[i]] == 0) {
      erased[erasures[i]] = 1;
      bzero((erasures[i] < k) ? data[erasures[i]] : coding[erasures[i]-k], sizeof(long)*w);
      i++;
    }
  }
  erasures[i] = -1;

  printf("Erased %d random devices:\n\n", m);
  printf("<p>\n");
  print_array(data, k, sizeof(long)*w, sizeof(long), "D");
  printf("<p>\n");
  print_array(coding, m, sizeof(long)*w, sizeof(long), "C");
  printf("<hr>\n");
  
  jerasure_schedule_decode_cache(k, m, w, cache, erasures, data, coding, w*sizeof(long), sizeof(long));
  jerasure_get_stats(stats);

  printf("State of the system after decoding: %.0lf XOR'd bytes\n\n", stats[0]);
  printf("<p>\n");
  print_array(data, k, sizeof(long)*w, sizeof(long), "D");
  printf("<p>\n");
  print_array(coding, m, sizeof(long)*w, sizeof(long), "C");
  printf("<hr>\n");
  
  bzero(coding[0], sizeof(long)*w);
  printf("Erased the first coding device:\n\n");
  printf("<p>\n");
  print_array(data, k, sizeof(long)*w, sizeof(long), "D");
  printf("<p>\n");
  print_array(coding, m, sizeof(long)*w, sizeof(long), "C");
  printf("<hr>\n");
  
  jerasure_do_parity(k, data, coding[0], sizeof(long)*w);
  printf("State of the system after using\n");
  printf("<b>jerasure_do_parity()</b> to re-encode it:\n\n");
  printf("<p>\n");
  print_array(data, k, sizeof(long)*w, sizeof(long), "D");
  printf("<p>\n");
  print_array(coding, m, sizeof(long)*w, sizeof(long), "C");
  printf("<hr>\n");
  
  jerasure_free_schedule(smart);
  jerasure_free_schedule_cache(k, m, cache);
  
  printf("Smart schedule and cache freed.\n\n");

  return 0;
}
