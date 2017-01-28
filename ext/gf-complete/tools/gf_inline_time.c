/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_inline_time.c
 *
 * Times inline single multiplication when w = 4, 8 or 16
 */

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <sys/time.h>

#include "gf_complete.h"
#include "gf_rand.h"

void
timer_start (double *t)
{
    struct timeval  tv;

    gettimeofday (&tv, NULL);
    *t = (double)tv.tv_sec + (double)tv.tv_usec * 1e-6;
}

double
timer_split (const double *t)
{
    struct timeval  tv;
    double  cur_t;

    gettimeofday (&tv, NULL);
    cur_t = (double)tv.tv_sec + (double)tv.tv_usec * 1e-6;
    return (cur_t - *t);
}

void problem(char *s)
{
  fprintf(stderr, "Timing test failed.\n");
  fprintf(stderr, "%s\n", s);
  exit(1);
}

void usage(char *s)
{
  fprintf(stderr, "usage: gf_inline_time w seed #elts iterations - does timing of single multiplies\n");
  fprintf(stderr, "\n");
  fprintf(stderr, "Legal w are: 4, 8 or 16\n");
  fprintf(stderr, "\n");
  fprintf(stderr, "Use -1 for time(0) as a seed.\n");
  fprintf(stderr, "\n");
  if (s != NULL) fprintf(stderr, "%s\n", s);
  exit(1);
}

int main(int argc, char **argv)
{
  int w, j, i, size, iterations;
  gf_t      gf;
  double timer, elapsed, dnum, num;
  uint8_t *ra = NULL, *rb = NULL, *mult4, *mult8;
  uint16_t *ra16 = NULL, *rb16 = NULL, *log16, *alog16;
  time_t t0;
  
  if (argc != 5) usage(NULL);
  if (sscanf(argv[1], "%d", &w) == 0) usage("Bad w\n");
  if (w != 4 && w != 8 && w != 16) usage("Bad w\n");
  if (sscanf(argv[2], "%ld", &t0) == 0) usage("Bad seed\n");
  if (sscanf(argv[3], "%d", &size) == 0) usage("Bad #elts\n");
  if (sscanf(argv[4], "%d", &iterations) == 0) usage("Bad iterations\n");
  if (t0 == -1) t0 = time(0);
  MOA_Seed(t0);

  num = size;

  gf_init_easy(&gf, w);
  
  printf("Seed: %ld\n", t0);

  if (w == 4 || w == 8) {
    ra = (uint8_t *) malloc(size);
    rb = (uint8_t *) malloc(size);

    if (ra == NULL || rb == NULL) { perror("malloc"); exit(1); }
  } else if (w == 16) {
    ra16 = (uint16_t *) malloc(size*2);
    rb16 = (uint16_t *) malloc(size*2);

    if (ra16 == NULL || rb16 == NULL) { perror("malloc"); exit(1); }
  }

  if (w == 4) {
    mult4 = gf_w4_get_mult_table(&gf);
    if (mult4 == NULL) {
      printf("Couldn't get inline multiplication table.\n");
      exit(1);
    }
    elapsed = 0;
    dnum = 0;
    for (i = 0; i < iterations; i++) {
      for (j = 0; j < size; j++) {
        ra[j] = MOA_Random_W(w, 1);
        rb[j] = MOA_Random_W(w, 1);
      }
      timer_start(&timer);
      for (j = 0; j < size; j++) {
        ra[j] = GF_W4_INLINE_MULTDIV(mult4, ra[j], rb[j]);
      }
      dnum += num;
      elapsed += timer_split(&timer);
    }
    printf("Inline mult:   %10.6lf s   Mops: %10.3lf    %10.3lf Mega-ops/s\n",
           elapsed, dnum/1024.0/1024.0, dnum/1024.0/1024.0/elapsed);

  } else if (w == 8) {
    mult8 = gf_w8_get_mult_table(&gf);
    if (mult8 == NULL) {
      printf("Couldn't get inline multiplication table.\n");
      exit(1);
    }
    elapsed = 0;
    dnum = 0;
    for (i = 0; i < iterations; i++) {
      for (j = 0; j < size; j++) {
        ra[j] = MOA_Random_W(w, 1);
        rb[j] = MOA_Random_W(w, 1);
      }
      timer_start(&timer);
      for (j = 0; j < size; j++) {
        ra[j] = GF_W8_INLINE_MULTDIV(mult8, ra[j], rb[j]);
      }
      dnum += num;
      elapsed += timer_split(&timer);
    }
    printf("Inline mult:   %10.6lf s   Mops: %10.3lf    %10.3lf Mega-ops/s\n",
           elapsed, dnum/1024.0/1024.0, dnum/1024.0/1024.0/elapsed);
  } else if (w == 16) {
    log16 = gf_w16_get_log_table(&gf);
    alog16 = gf_w16_get_mult_alog_table(&gf);
    if (log16 == NULL) {
      printf("Couldn't get inline multiplication table.\n");
      exit(1);
    }
    elapsed = 0;
    dnum = 0;
    for (i = 0; i < iterations; i++) {
      for (j = 0; j < size; j++) {
        ra16[j] = MOA_Random_W(w, 1);
        rb16[j] = MOA_Random_W(w, 1);
      }
      timer_start(&timer);
      for (j = 0; j < size; j++) {
        ra16[j] = GF_W16_INLINE_MULT(log16, alog16, ra16[j], rb16[j]);
      }
      dnum += num;
      elapsed += timer_split(&timer);
    }
    printf("Inline mult:   %10.6lf s   Mops: %10.3lf    %10.3lf Mega-ops/s\n",
           elapsed, dnum/1024.0/1024.0, dnum/1024.0/1024.0/elapsed);
  }
  free (ra);
  free (rb);
  free (ra16);
  free (rb16);
  return 0;
}
