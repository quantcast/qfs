/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_unit.c
 *
 * Performs unit testing for gf arithmetic
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
#include <time.h>
#include <signal.h>

#include "gf_complete.h"
#include "gf_int.h"
#include "gf_method.h"
#include "gf_rand.h"
#include "gf_general.h"

#define REGION_SIZE (16384)
#define RMASK (0x00000000ffffffffLL)
#define LMASK (0xffffffff00000000LL)

void problem(char *s)
{
  fprintf(stderr, "Unit test failed.\n");
  fprintf(stderr, "%s\n", s);
  exit(1);
}

char *BM = "Bad Method: ";

void usage(char *s)
{
  fprintf(stderr, "usage: gf_unit w tests seed [method] - does unit testing in GF(2^w)\n");
  fprintf(stderr, "\n");
  fprintf(stderr, "Legal w are: 1 - 32, 64 and 128\n");
  fprintf(stderr, "           128 is hex only (i.e. '128' will be an error - do '128h')\n");
  fprintf(stderr, "\n");
  fprintf(stderr, "Tests may be any combination of:\n");
  fprintf(stderr, "       A: All\n");
  fprintf(stderr, "       S: Single operations (multiplication/division)\n");
  fprintf(stderr, "       R: Region operations\n");
  fprintf(stderr, "       V: Verbose Output\n");
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

void SigHandler(int v)
{
  fprintf(stderr, "Problem: SegFault!\n");
  fflush(stdout);
  exit(2);
}

int main(int argc, char **argv)
{
  signal(SIGSEGV, SigHandler);

  int w, i, verbose, single, region, top;
  int s_start, d_start, bytes, xor, alignment_test;
  gf_t   gf, gf_def;
  time_t t0;
  gf_internal_t *h;
  gf_general_t *a, *b, *c, *d;
  uint8_t a8, b8, c8, *mult4 = NULL, *mult8 = NULL;
  uint16_t a16, b16, c16, *log16 = NULL, *alog16 = NULL;
  char as[50], bs[50], cs[50], ds[50];
  uint32_t mask = 0;
  char *ra, *rb, *rc, *rd, *target;
  int align;
#ifndef HAVE_POSIX_MEMALIGN
  char *malloc_ra, *malloc_rb, *malloc_rc, *malloc_rd;
#endif


  if (argc < 4) usage(NULL);

  if (sscanf(argv[1], "%d", &w) == 0){
    usage("Bad w\n");
  }

  if (sscanf(argv[3], "%ld", &t0) == 0) usage("Bad seed\n");
  if (t0 == -1) t0 = time(0);
  MOA_Seed(t0);

  if (w > 32 && w != 64 && w != 128) usage("Bad w");

  if (create_gf_from_argv(&gf, w, argc, argv, 4) == 0) {
    usage(BM);
  }

  printf("Args: ");
  for (i = 1; i < argc; i++) {
    printf ("%s ", argv[i]);
  }
  printf("/ size (bytes): %d\n", gf_size(&gf));

  for (i = 0; i < strlen(argv[2]); i++) {
    if (strchr("ASRV", argv[2][i]) == NULL) usage("Bad test\n");
  }

  h = (gf_internal_t *) gf.scratch;
  a = (gf_general_t *) malloc(sizeof(gf_general_t));
  b = (gf_general_t *) malloc(sizeof(gf_general_t));
  c = (gf_general_t *) malloc(sizeof(gf_general_t));
  d = (gf_general_t *) malloc(sizeof(gf_general_t));

#if HAVE_POSIX_MEMALIGN
  if (posix_memalign((void **) &ra, 16, sizeof(char)*REGION_SIZE))
    ra = NULL;
  if (posix_memalign((void **) &rb, 16, sizeof(char)*REGION_SIZE))
    rb = NULL;
  if (posix_memalign((void **) &rc, 16, sizeof(char)*REGION_SIZE))
    rc = NULL;
  if (posix_memalign((void **) &rd, 16, sizeof(char)*REGION_SIZE))
    rd = NULL;
#else
  //15 bytes extra to make sure it's 16byte aligned
  malloc_ra = (char *) malloc(sizeof(char)*REGION_SIZE+15);
  malloc_rb = (char *) malloc(sizeof(char)*REGION_SIZE+15);
  malloc_rc = (char *) malloc(sizeof(char)*REGION_SIZE+15);
  malloc_rd = (char *) malloc(sizeof(char)*REGION_SIZE+15);
  ra = (uint8_t *) (((uintptr_t) malloc_ra + 15) & ~((uintptr_t) 0xf));
  rb = (uint8_t *) (((uintptr_t) malloc_rb + 15) & ~((uintptr_t) 0xf));
  rc = (uint8_t *) (((uintptr_t) malloc_rc + 15) & ~((uintptr_t) 0xf));
  rd = (uint8_t *) (((uintptr_t) malloc_rd + 15) & ~((uintptr_t) 0xf));
#endif

  if (w <= 32) {
    mask = 0;
    for (i = 0; i < w; i++) mask |= (1 << i);
  }

  verbose = (strchr(argv[2], 'V') != NULL);
  single = (strchr(argv[2], 'S') != NULL || strchr(argv[2], 'A') != NULL);
  region = (strchr(argv[2], 'R') != NULL || strchr(argv[2], 'A') != NULL);

  if (!gf_init_hard(&gf_def, w, GF_MULT_DEFAULT, GF_REGION_DEFAULT, GF_DIVIDE_DEFAULT,
      (h->mult_type != GF_MULT_COMPOSITE) ? h->prim_poly : 0, 0, 0, NULL, NULL))
    problem("No default for this value of w");

  if (w == 4) {
    mult4 = gf_w4_get_mult_table(&gf);
  } else if (w == 8) {
    mult8 = gf_w8_get_mult_table(&gf);
  } else if (w == 16) {
    log16 = gf_w16_get_log_table(&gf);
    alog16 = gf_w16_get_mult_alog_table(&gf);
  }

  if (verbose) printf("Seed: %ld\n", t0);

  if (single) {
    
    if (gf.multiply.w32 == NULL) problem("No multiplication operation defined.");
    if (verbose) { printf("Testing single multiplications/divisions.\n"); fflush(stdout); }
    if (w <= 10) {
      top = (1 << w)*(1 << w);
    } else {
      top = 1024*1024;
    }
    for (i = 0; i < top; i++) {
      if (w <= 10) {
        a->w32 = i % (1 << w);
        b->w32 = (i >> w);

      //Allen: the following conditions were being run 10 times each. That didn't seem like nearly enough to
      //me for these special cases, so I converted to doing this mod stuff to easily make the number of times
      //run both larger and proportional to the total size of the run.
      } else {
        switch (i % 32)
        {
          case 0: 
            gf_general_set_zero(a, w);
            gf_general_set_random(b, w, 1);
            break;
          case 1:
            gf_general_set_random(a, w, 1);
            gf_general_set_zero(b, w);
            break;
          case 2:
            gf_general_set_one(a, w);
            gf_general_set_random(b, w, 1);
            break;
          case 3:
            gf_general_set_random(a, w, 1);
            gf_general_set_one(b, w);
            break;
          default:
            gf_general_set_random(a, w, 1);
            gf_general_set_random(b, w, 1);
        }
      }

      //Allen: the following special cases for w=64 are based on the code below for w=128.
      //These w=64 cases are based on Dr. Plank's suggestion because some of the methods for w=64
      //involve splitting it in two. I think they're less likely to give errors than the 128-bit case
      //though, because the 128 bit case is always split in two.
      //As with w=128, I'm arbitrarily deciding to do this sort of thing with a quarter of the cases
      if (w == 64) {
        switch (i % 32)
        {
          case 0: if (!gf_general_is_one(a, w)) a->w64 &= RMASK; break;
          case 1: if (!gf_general_is_one(a, w)) a->w64 &= LMASK; break;
          case 2: if (!gf_general_is_one(a, w)) a->w64 &= RMASK; if (!gf_general_is_one(b, w)) b->w64 &= RMASK; break;
          case 3: if (!gf_general_is_one(a, w)) a->w64 &= RMASK; if (!gf_general_is_one(b, w)) b->w64 &= LMASK; break;
          case 4: if (!gf_general_is_one(a, w)) a->w64 &= LMASK; if (!gf_general_is_one(b, w)) b->w64 &= RMASK; break;
          case 5: if (!gf_general_is_one(a, w)) a->w64 &= LMASK; if (!gf_general_is_one(b, w)) b->w64 &= LMASK; break;
          case 6: if (!gf_general_is_one(b, w)) b->w64 &= RMASK; break;
          case 7: if (!gf_general_is_one(b, w)) b->w64 &= LMASK; break;
        }
      }

      //Allen: for w=128, we have important special cases where one half or the other of the number is all
      //zeros. The probability of hitting such a number randomly is 1^-64, so if we don't force these cases
      //we'll probably never hit them. This could be implemented more efficiently by changing the set-random
      //function for w=128, but I think this is easier to follow.
      //I'm arbitrarily deciding to do this sort of thing with a quarter of the cases
      if (w == 128) {
        switch (i % 32)
        {
          case 0: if (!gf_general_is_one(a, w)) a->w128[0] = 0; break;
          case 1: if (!gf_general_is_one(a, w)) a->w128[1] = 0; break;
          case 2: if (!gf_general_is_one(a, w)) a->w128[0] = 0; if (!gf_general_is_one(b, w)) b->w128[0] = 0; break;
          case 3: if (!gf_general_is_one(a, w)) a->w128[0] = 0; if (!gf_general_is_one(b, w)) b->w128[1] = 0; break;
          case 4: if (!gf_general_is_one(a, w)) a->w128[1] = 0; if (!gf_general_is_one(b, w)) b->w128[0] = 0; break;
          case 5: if (!gf_general_is_one(a, w)) a->w128[1] = 0; if (!gf_general_is_one(b, w)) b->w128[1] = 0; break;
          case 6: if (!gf_general_is_one(b, w)) b->w128[0] = 0; break;
          case 7: if (!gf_general_is_one(b, w)) b->w128[1] = 0; break;
        }
      }

      gf_general_multiply(&gf, a, b, c);
      
      /* If w is 4, 8 or 16, then there are inline multiplication/division methods.  
         Test them here. */

      if (w == 4 && mult4 != NULL) {
        a8 = a->w32;
        b8 = b->w32;
        c8 = GF_W4_INLINE_MULTDIV(mult4, a8, b8);
        if (c8 != c->w32) {
          printf("Error in inline multiplication. %d * %d.  Inline = %d.  Default = %d.\n",
             a8, b8, c8, c->w32);
          exit(1);
        }
      }

      if (w == 8 && mult8 != NULL) {
        a8 = a->w32;
        b8 = b->w32;
        c8 = GF_W8_INLINE_MULTDIV(mult8, a8, b8);
        if (c8 != c->w32) {
          printf("Error in inline multiplication. %d * %d.  Inline = %d.  Default = %d.\n",
             a8, b8, c8, c->w32);
          exit(1);
        }
      }

      if (w == 16 && log16 != NULL) {
        a16 = a->w32;
        b16 = b->w32;
        c16 = GF_W16_INLINE_MULT(log16, alog16, a16, b16);
        if (c16 != c->w32) {
          printf("Error in inline multiplication. %d * %d.  Inline = %d.  Default = %d.\n",
             a16, b16, c16, c->w32);
          printf("%d %d\n", log16[a16], log16[b16]);
          top = log16[a16] + log16[b16];
          printf("%d %d\n", top, alog16[top]);
          exit(1);
        }
      }

      /* If this is not composite, then first test against the default: */

      if (h->mult_type != GF_MULT_COMPOSITE) {
        gf_general_multiply(&gf_def, a, b, d);

        if (!gf_general_are_equal(c, d, w)) {
          gf_general_val_to_s(a, w, as, 1);
          gf_general_val_to_s(b, w, bs, 1);
          gf_general_val_to_s(c, w, cs, 1);
          gf_general_val_to_s(d, w, ds, 1);
          printf("Error in single multiplication (all numbers in hex):\n\n");
          printf("  gf.multiply(gf, %s, %s) = %s\n", as, bs, cs);
          printf("  The default gf multiplier returned %s\n", ds);
          exit(1);
        }
      }

      /* Now, we also need to double-check by other means, in case the default is wanky, 
         and when we're performing composite operations. Start with 0 and 1, where we know
         what the result should be. */

      if (gf_general_is_zero(a, w) || gf_general_is_zero(b, w) || 
          gf_general_is_one(a, w)  || gf_general_is_one(b, w)) {
        if (((gf_general_is_zero(a, w) || gf_general_is_zero(b, w)) && !gf_general_is_zero(c, w)) ||
            (gf_general_is_one(a, w) && !gf_general_are_equal(b, c, w)) ||
            (gf_general_is_one(b, w) && !gf_general_are_equal(a, c, w))) {
          gf_general_val_to_s(a, w, as, 1);
          gf_general_val_to_s(b, w, bs, 1);
          gf_general_val_to_s(c, w, cs, 1);
          printf("Error in single multiplication (all numbers in hex):\n\n");
          printf("  gf.multiply(gf, %s, %s) = %s, which is clearly wrong.\n", as, bs, cs);
          exit(1);
        }
      }

      /* Dumb check to make sure that it's not returning numbers that are too big: */

      if (w < 32 && (c->w32 & mask) != c->w32) {
        gf_general_val_to_s(a, w, as, 1);
        gf_general_val_to_s(b, w, bs, 1);
        gf_general_val_to_s(c, w, cs, 1);
        printf("Error in single multiplication (all numbers in hex):\n\n");
        printf("  gf.multiply.w32(gf, %s, %s) = %s, which is too big.\n", as, bs, cs);
        exit(1);
      }

      /* Finally, let's check to see that multiplication and division work together */

      if (!gf_general_is_zero(a, w)) {
        gf_general_divide(&gf, c, a, d);
        if (!gf_general_are_equal(b, d, w)) {
          gf_general_val_to_s(a, w, as, 1);
          gf_general_val_to_s(b, w, bs, 1);
          gf_general_val_to_s(c, w, cs, 1);
          gf_general_val_to_s(d, w, ds, 1);
          printf("Error in single multiplication/division (all numbers in hex):\n\n");
          printf("  gf.multiply(gf, %s, %s) = %s, but gf.divide(gf, %s, %s) = %s\n", as, bs, cs, cs, as, ds);
          exit(1);
        }
      }

    }
  }

  if (region) {
    if (verbose) { printf("Testing region multiplications\n"); fflush(stdout); }
    for (i = 0; i < 1024; i++) {
      //Allen: changing to a switch thing as with the single ops to make things proportional
      switch (i % 32)
      {
        case 0:
          gf_general_set_zero(a, w);
          break;
        case 1:
          gf_general_set_one(a, w);
          break;
        case 2:
          gf_general_set_two(a, w);
          break;
        default:
          gf_general_set_random(a, w, 1);
      }
      MOA_Fill_Random_Region(ra, REGION_SIZE);
      MOA_Fill_Random_Region(rb, REGION_SIZE);
      xor = (i/32)%2;
      align = w/8;
      if (align == 0) align = 1;
      if (align > 16) align = 16;

      /* JSP - Cauchy test.  When w < 32 & it doesn't equal 4, 8 or 16, the default is
         equal to GF_REGION_CAUCHY, even if GF_REGION_CAUCHY is not set. We are testing
         three alignments here:

         1. Anything goes -- no alignment guaranteed.
         2. Perfect alignment.  Here src and dest must be aligned wrt each other,
            and bytes must be a multiple of 16*w.  
         3. Imperfect alignment.  Here we'll have src and dest be aligned wrt each 
            other, but bytes is simply a multiple of w.  That means some XOR's will
            be aligned, and some won't.
       */

      if ((h->region_type & GF_REGION_CAUCHY) || (w < 32 && w != 4 && w != 8 && w != 16)) {
        alignment_test = (i%3);
        
        s_start = MOA_Random_W(5, 1);
        if (alignment_test == 0) {
          d_start = MOA_Random_W(5, 1);
        } else {
          d_start = s_start;
        }

        bytes = (d_start > s_start) ? REGION_SIZE - d_start : REGION_SIZE - s_start;
        bytes -= MOA_Random_W(5, 1);
        if (alignment_test == 1) {
          bytes -= (bytes % (w*16));
        } else {
          bytes -= (bytes % w);
        }

        target = rb;
 
      /* JSP - Otherwise, we're testing a non-cauchy test, and alignment
        must be more strict.  We have to make sure that the regions are
        aligned wrt each other on 16-byte pointers.  */

      } else {
        s_start = MOA_Random_W(5, 1) * align;
        d_start = s_start;
        bytes = REGION_SIZE - s_start - MOA_Random_W(5, 1);
        bytes -= (bytes % align);

        if (h->mult_type == GF_MULT_COMPOSITE && (h->region_type & GF_REGION_ALTMAP)) {
          target = rb ;
        } else {
          target = (i/64)%2 ? rb : ra;
        }
      }

      memcpy(rc, ra, REGION_SIZE);
      memcpy(rd, target, REGION_SIZE);
      gf_general_do_region_multiply(&gf, a, ra+s_start, target+d_start, bytes, xor);
      gf_general_do_region_check(&gf, a, rc+s_start, rd+d_start, target+d_start, bytes, xor);
    }
  }

  free(a);
  free(b);
  free(c);
  free(d);
#ifdef HAVE_POSIX_MEMALIGN
  free(ra);
  free(rb);
  free(rc);
  free(rd);
#else
  free(malloc_ra);
  free(malloc_rb);
  free(malloc_rc);
  free(malloc_rd);
#endif
  
  return 0;
}
