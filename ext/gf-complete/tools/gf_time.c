/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_time.c
 *
 * Performs timing for gf arithmetic
 */

#include "config.h"

#ifdef HAVE_POSIX_MEMALIGN
#ifndef _XOPEN_SOURCE
#define _XOPEN_SOURCE 600
#endif
#endif

#include <stdio.h>
#include <getopt.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include <sys/time.h>

#include "gf_complete.h"
#include "gf_method.h"
#include "gf_rand.h"
#include "gf_general.h"

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

char *BM = "Bad Method: ";

void usage(char *s)
{
  fprintf(stderr, "usage: gf_time w tests seed size(bytes) iterations [method [params]] - does timing\n");
  fprintf(stderr, "\n");
  fprintf(stderr, "does unit testing in GF(2^w)\n");
  fprintf(stderr, "\n");
  fprintf(stderr, "Legal w are: 1 - 32, 64 and 128\n");
  fprintf(stderr, "\n");
  fprintf(stderr, "Tests may be any combination of:\n");
  fprintf(stderr, "       A: All\n");
  fprintf(stderr, "       S: All Single Operations\n");
  fprintf(stderr, "       R: All Region Operations\n");
  fprintf(stderr, "       M: Single: Multiplications\n");
  fprintf(stderr, "       D: Single: Divisions\n");
  fprintf(stderr, "       I: Single: Inverses\n");
  fprintf(stderr, "       G: Region: Buffer-Constant Multiplication\n");
  fprintf(stderr, "       0: Region: Doing nothing, and bzero()\n");
  fprintf(stderr, "       1: Region: Memcpy() and XOR\n");
  fprintf(stderr, "       2: Region: Multiplying by two\n");
  fprintf(stderr, "\n");
  fprintf(stderr, "Use -1 for time(0) as a seed.\n");
  fprintf(stderr, "\n");
  if (s == BM) {
    fprintf(stderr, "%s", BM);
    gf_error();
  } else if (s != NULL) {
    fprintf(stderr, "%s\n", s);
  }
  exit(1);
}

int main(int argc, char **argv)
{
  int w, it, i, size, iterations, xor;
  char tests[100];
  char test;
  char *single_tests = "MDI";
  char *region_tests = "G012";
  char *tstrings[256];
  void *tmethods[256];
  gf_t      gf;
  double timer, elapsed, ds, di, dnum;
  int num;
  time_t t0;
  uint8_t *ra, *rb;
  gf_general_t a;
#ifndef HAVE_POSIX_MEMALIGN
  uint8_t *malloc_ra, *malloc_rb;
#endif

  
  if (argc < 6) usage(NULL);
  
  if (sscanf(argv[1], "%d", &w) == 0){
    usage("Bad w[-pp]\n");
  }

  
  if (sscanf(argv[3], "%ld", &t0) == 0) usage("Bad seed\n");
  if (sscanf(argv[4], "%d", &size) == 0) usage("Bad size\n");
  if (sscanf(argv[5], "%d", &iterations) == 0) usage("Bad iterations\n");
  if (t0 == -1) t0 = time(0);
  MOA_Seed(t0);

  ds = size;
  di = iterations;

  if ((w > 32 && w != 64 && w != 128) || w < 0) usage("Bad w");
  if ((size * 8) % w != 0) usage ("Bad size -- must be a multiple of w*8\n");
  
  if (!create_gf_from_argv(&gf, w, argc, argv, 6)) usage(BM);

  strcpy(tests, "");
  for (i = 0; argv[2][i] != '\0'; i++) {
    switch(argv[2][i]) {
      case 'A': strcat(tests, single_tests); 
                strcat(tests, region_tests); 
                break;
      case 'S': strcat(tests, single_tests); break;
      case 'R': strcat(tests, region_tests); break;
      case 'G': strcat(tests, "G"); break;
      case '0': strcat(tests, "0"); break;
      case '1': strcat(tests, "1"); break;
      case '2': strcat(tests, "2"); break;
      case 'M': strcat(tests, "M"); break;
      case 'D': strcat(tests, "D"); break;
      case 'I': strcat(tests, "I"); break;
      default: usage("Bad tests");
    }
  }

  tstrings['M'] = "Multiply";
  tstrings['D'] = "Divide";
  tstrings['I'] = "Inverse";
  tstrings['G'] = "Region-Random";
  tstrings['0'] = "Region-By-Zero";
  tstrings['1'] = "Region-By-One";
  tstrings['2'] = "Region-By-Two";

  tmethods['M'] = (void *) gf.multiply.w32;
  tmethods['D'] = (void *) gf.divide.w32;
  tmethods['I'] = (void *) gf.inverse.w32;
  tmethods['G'] = (void *) gf.multiply_region.w32;
  tmethods['0'] = (void *) gf.multiply_region.w32;
  tmethods['1'] = (void *) gf.multiply_region.w32;
  tmethods['2'] = (void *) gf.multiply_region.w32;

  printf("Seed: %ld\n", t0);

#ifdef HAVE_POSIX_MEMALIGN
  if (posix_memalign((void **) &ra, 16, size))
    ra = NULL;
  if (posix_memalign((void **) &rb, 16, size))
    rb = NULL;
#else
  malloc_ra = (uint8_t *) malloc(size + 15);
  malloc_rb = (uint8_t *) malloc(size + 15);
  ra = (uint8_t *) (((uintptr_t) malloc_ra + 15) & ~((uintptr_t) 0xf));
  rb = (uint8_t *) (((uintptr_t) malloc_rb + 15) & ~((uintptr_t) 0xf));
#endif

  if (ra == NULL || rb == NULL) { perror("malloc"); exit(1); }

  for (i = 0; i < 3; i++) {
    test = single_tests[i];
    if (strchr(tests, test) != NULL) {
      if (tmethods[(int)test] == NULL) {
        printf("No %s method.\n", tstrings[(int)test]);
      } else {
        elapsed = 0;
        dnum = 0;
        for (it = 0; it < iterations; it++) {
          gf_general_set_up_single_timing_test(w, ra, rb, size);
          timer_start(&timer);
          num = gf_general_do_single_timing_test(&gf, ra, rb, size, test);
          dnum += num;
          elapsed += timer_split(&timer);
        }
        printf("%14s:           %10.6lf s   Mops: %10.3lf    %10.3lf Mega-ops/s\n", 
               tstrings[(int)test], elapsed, 
               dnum/1024.0/1024.0, dnum/1024.0/1024.0/elapsed);
      }
    }
  }

  for (i = 0; i < 4; i++) {
    test = region_tests[i];
    if (strchr(tests, test) != NULL) {
      if (tmethods[(int)test] == NULL) {
        printf("No %s method.\n", tstrings[(int)test]);
      } else {
        if (test == '0') gf_general_set_zero(&a, w);
        if (test == '1') gf_general_set_one(&a, w);
        if (test == '2') gf_general_set_two(&a, w);

        for (xor = 0; xor < 2; xor++) {
          elapsed = 0;
          for (it = 0; it < iterations; it++) {
            if (test == 'G') gf_general_set_random(&a, w, 1);
            gf_general_set_up_single_timing_test(8, ra, rb, size);
            timer_start(&timer);
            gf_general_do_region_multiply(&gf, &a, ra, rb, size, xor);
            elapsed += timer_split(&timer);
          }
          printf("%14s: XOR: %d    %10.6lf s     MB: %10.3lf    %10.3lf MB/s\n", 
               tstrings[(int)test], xor, elapsed, 
               ds*di/1024.0/1024.0, ds*di/1024.0/1024.0/elapsed);
        }
      }
    }
  }
  return 0;
}
