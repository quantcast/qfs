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
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <gf_rand.h>
#include "jerasure.h"
#include "cauchy.h"

#define talloc(type, num) (type *) malloc(sizeof(type)*(num))

static void usage(char *s)
{
  fprintf(stderr, "usage: cauchy_02 k m w seed - CRS coding example using Bloemer's original matrix.\n");
  fprintf(stderr, "       \n");
  fprintf(stderr, "k+m must be <= 2^w\n");
  fprintf(stderr, "This sets up a generator matrix (G^T) in GF(2^w) whose last m rows are\n");
  fprintf(stderr, "created from a Cauchy matrix, using the original definition from [Bloemer95].\n");
  fprintf(stderr, "It converts this matrix to a bitmatrix, and then it encodes w packets from\n");
  fprintf(stderr, "each of k disks (simulated) onto w packets on each of m disks.  Packets are \n");
  fprintf(stderr, "simply longs.  Then, it deletes m random disks, and decodes.  \n");
  fprintf(stderr, "\n");
  fprintf(stderr, "The encoding and decoding are done twice, first, with jerasure_bitmatrix_encode()\n");
  fprintf(stderr, "and jerasure_bitmatrix_decode(), and second using 'smart' scheduling with\n");
  fprintf(stderr, "jerasure_schedule_encode() and jerasure_schedule_decode_lazy().\n");

  fprintf(stderr, "\n");
  fprintf(stderr, "This demonstrates: cauchy_original_coding_matrix()\n");
  fprintf(stderr, "                   jerasure_bitmatrix_encode()\n");
  fprintf(stderr, "                   jerasure_bitmatrix_decode()\n");
  fprintf(stderr, "                   cauchy_n_ones()\n");
  fprintf(stderr, "                   jerasure_smart_bitmatrix_to_schedule()\n");
  fprintf(stderr, "                   jerasure_schedule_encode()\n");
  fprintf(stderr, "                   jerasure_schedule_decode_lazy()\n");
  fprintf(stderr, "                   jerasure_print_matrix()\n");
  fprintf(stderr, "                   jerasure_print_bitmatrix()\n");
  fprintf(stderr, "                   jerasure_get_stats()\n");
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
  int k, w, i, m;
  int *matrix, *bitmatrix, **schedule;
  char **data, **coding, **dcopy, **ccopy;
  int no;
  int *erasures, *erased;
  double mstats[3], sstats[3];
  uint32_t seed;
  
  if (argc != 5) usage(NULL);
  if (sscanf(argv[1], "%d", &k) == 0 || k <= 0) usage("Bad k");
  if (sscanf(argv[2], "%d", &m) == 0 || m <= 0) usage("Bad m");
  if (sscanf(argv[3], "%d", &w) == 0 || w <= 0 || w > 32) usage("Bad w");
  if (sscanf(argv[4], "%d", &seed) == 0) usage("Bad seed");
  if (w < 30 && (k+m) > (1 << w)) usage("k + m is too big");

  matrix = cauchy_original_coding_matrix(k, m, w);
  if (matrix == NULL) {
    usage("couldn't make coding matrix");
  }

  /* Print out header information to the output file. */
  printf("<HTML>\n");
  printf("<TITLE>Jerasure Example Output: cauchy_02 %d %d %d %d</TITLE>\n", k, m, w, seed);
  printf("<h2>Jerasure Example Output: cauchy_02 %d %d %d %d</h3>\n", k, m, w, seed);

  printf("<hr>\n");
  printf("Parameters:\n");
  printf("<UL><LI> Number of data disks <i>(k)</i>: %d\n", k);
  printf("<LI> Number of coding disks <i>(m)</i>: %d\n", m);
  printf("<LI> Word size of the Galois Field: <i>(w)</i>: %d\n", w);
  printf("<LI> Seed for the random number generator: %d\n", seed);
  printf("<LI> Number of bytes stored per disk: %ld\n", sizeof(long)*w);
  printf("<LI> Number of packets stored per disk: %d\n", w);
  printf("<LI> Number of bytes per packet: %ld\n", sizeof(long));
  printf("</UL>\n");

  /* Print out the matrix and the bitmatrix */
  printf("<hr>\n");
  printf("Here is the matrix, which was created with <b>cauchy_original_coding_matrix()</b>.\n");
  printf("This is not the best matrix to use, but we include it to show an example\n");
  printf("of <b>cauchy_original_coding_matrix()</b>.  For the best matrix and encoding/decoding\n");
  printf("methodology, see <b>cauchy_04.</b><p><pre>\n");

  jerasure_print_matrix(matrix, m, k, w);
  printf("</pre>\n");

  bitmatrix = jerasure_matrix_to_bitmatrix(k, m, w, matrix);

  no = 0;
  for (i = 0; i < k*m; i++) {
    no += cauchy_n_ones(matrix[i], w);
  }

  printf("The bitmatrix, which has %d one%s:<p><pre>\n", no, (no == 1) ? "" : "s");
  jerasure_print_bitmatrix(bitmatrix, m*w, k*w, w);
  printf("</pre>\n");
  printf("<hr>\n");
  MOA_Seed(seed);

  data = talloc(char *, k);
  dcopy = talloc(char *, k);
  for (i = 0; i < k; i++) {
    data[i] = talloc(char, sizeof(long)*w);
    dcopy[i] = talloc(char, sizeof(long)*w);
    MOA_Fill_Random_Region(data[i], sizeof(long)*w);
    memcpy(dcopy[i], data[i], sizeof(long)*w);
  }

  printf("Here are the packets on the data disks:<p>\n");
  print_array(data, k, sizeof(long)*w, sizeof(long), "D");

  coding = talloc(char *, m);
  ccopy = talloc(char *, m);
  for (i = 0; i < m; i++) {
    coding[i] = talloc(char, sizeof(long)*w);
    ccopy[i] = talloc(char, sizeof(long)*w);
  }

  jerasure_bitmatrix_encode(k, m, w, bitmatrix, data, coding, w*sizeof(long), sizeof(long));
  jerasure_get_stats(mstats);

  schedule = jerasure_smart_bitmatrix_to_schedule(k, m, w, bitmatrix);
  jerasure_schedule_encode(k, m, w, schedule, data, ccopy, w*sizeof(long), sizeof(long));
  jerasure_get_stats(sstats);

  printf("<p>Encoding with jerasure_bitmatrix_encode() - Bytes XOR'd: %.0lf.<br>\n", mstats[0]);
  printf("Encoding with jerasure_schedule_encode() - Bytes XOR'd: %.0lf.<br>\n", sstats[0]);

  for (i = 0; i < m; i++) {
    if (memcmp(coding[i], ccopy[i], sizeof(long)*w) != 0) {
      printf("Problem: the two encodings don't match on disk C%x\n", i);
      exit(0);
    }
  }

  printf("Here are the packets on the coding disks.<br>\n");
  print_array(coding, m, sizeof(long)*w, sizeof(long), "C");
  printf("<hr>\n");

  erasures = talloc(int, (m+1));
  erased = talloc(int, (k+m));
  for (i = 0; i < m+k; i++) erased[i] = 0;
  for (i = 0; i < m; ) {
    erasures[i] = MOA_Random_W(31, 1)%(k+m);
    if (erased[erasures[i]] == 0) {
      erased[erasures[i]] = 1;
      bzero((erasures[i] < k) ? data[erasures[i]] : coding[erasures[i]-k], sizeof(long)*w);
      i++;
    }
  }
  erasures[i] = -1;
  printf("Erasures on the following devices:");
  for (i = 0; erasures[i] != -1; i++) {
    printf(" %c%x", ((erasures[i] < k) ? 'D' : 'C'), (erasures[i] < k ? erasures[i] : erasures[i]-k));
  }
  printf("<br>\nHere is the state of the system:\n<p>\n");
  print_array(data, k, sizeof(long)*w, sizeof(long), "D");
  printf("<p>\n");
  print_array(coding, m, sizeof(long)*w, sizeof(long), "C");
  printf("<hr>\n");

  jerasure_bitmatrix_decode(k, m, w, bitmatrix, 0, erasures, data, coding, w*sizeof(long), sizeof(long));
  jerasure_get_stats(mstats);

  printf("<p>Decoded with jerasure_bitmatrix_decode - Bytes XOR'd: %.0lf.<br>\n", mstats[0]);

  for (i = 0; i < k; i++) if (memcmp(data[i], dcopy[i], sizeof(long)*w) != 0) {
    printf("ERROR: D%x after decoding does not match its state before decoding!<br>\n", i);
  }
  for (i = 0; i < m; i++) if (memcmp(coding[i], ccopy[i], sizeof(long)*w) != 0) {
    printf("ERROR: C%x after decoding does not match its state before decoding!<br>\n", i);
  }

  for (i = 0; erasures[i] != -1; i++) {
    bzero((erasures[i] < k) ? data[erasures[i]] : coding[erasures[i]-k], sizeof(long)*w);
  }

  jerasure_schedule_decode_lazy(k, m, w, bitmatrix, erasures, data, coding, w*sizeof(long), sizeof(long), 1);
  jerasure_get_stats(sstats);

  printf("jerasure_schedule_decode_lazy - Bytes XOR'd: %.0lf.<br>\n", sstats[0]);

  for (i = 0; i < k; i++) if (memcmp(data[i], dcopy[i], sizeof(long)*w) != 0) {
    printf("ERROR: D%x after decoding does not match its state before decoding!<br>\n", i);
  }
  for (i = 0; i < m; i++) if (memcmp(coding[i], ccopy[i], sizeof(long)*w) != 0) {
    printf("ERROR: C%x after decoding does not match its state before decoding!<br>\n", i);
  }

  printf("Here is the state of the system:\n<p>\n");
  print_array(data, k, sizeof(long)*w, sizeof(long), "D");
  printf("<p>\n");
  print_array(coding, m, sizeof(long)*w, sizeof(long), "C");
  printf("<hr>\n");

  return 0;
}
