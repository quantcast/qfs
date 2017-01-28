/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_methods.c
 *
 * Lists supported methods (incomplete w.r.t. GROUP and COMPOSITE)
 */

#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <stdlib.h>

#include "gf_complete.h"
#include "gf_method.h"
#include "gf_int.h"

#define BNMULTS (8)
static char *BMULTS[BNMULTS] = { "CARRY_FREE", "GROUP48", 
                               "TABLE", "LOG", "SPLIT4", "SPLIT8", "SPLIT88", "COMPOSITE" };
#define NMULTS (17)
static char *MULTS[NMULTS] = { "SHIFT", "CARRY_FREE", "CARRY_FREE_GK", "GROUP44", "GROUP48", "BYTWO_p", "BYTWO_b",
                               "TABLE", "LOG", "LOG_ZERO", "LOG_ZERO_EXT", "SPLIT2",
                               "SPLIT4", "SPLIT8", "SPLIT16", "SPLIT88", "COMPOSITE" };

/* Make sure CAUCHY is last */

#define NREGIONS (7) 
static char *REGIONS[NREGIONS] = { "DOUBLE", "QUAD", "LAZY", "SIMD", "NOSIMD",
                                   "ALTMAP", "CAUCHY" };

#define BNREGIONS (4) 
static char *BREGIONS[BNREGIONS] = { "DOUBLE", "QUAD", "ALTMAP", "CAUCHY" };

#define NDIVS (2)
static char *divides[NDIVS] = { "MATRIX", "EUCLID" }; 

void usage(char *s)
{
   fprintf(stderr, "usage: gf_methods w -BADC -LXUMDRB\n");
   fprintf(stderr, "\n");
   fprintf(stderr, "       w can be 1-32, 64, 128\n");
   fprintf(stderr, "\n");
   fprintf(stderr, "       -B lists basic methods that are useful\n");
   fprintf(stderr, "       -A does a nearly exhaustive listing\n");
   fprintf(stderr, "       -D adds EUCLID and MATRIX division\n");
   fprintf(stderr, "       -C adds CAUCHY when possible\n");
   fprintf(stderr, "       Combinations are fine.\n");
   fprintf(stderr, "\n");
   fprintf(stderr, "       -L Simply lists methods\n");
   fprintf(stderr, "       -X List methods and functions selected (compile with DEBUG_FUNCTIONS)\n");
   fprintf(stderr, "       -U Produces calls to gf_unit\n");
   fprintf(stderr, "       -M Produces calls to time_tool.sh for single multiplications\n");
   fprintf(stderr, "       -D Produces calls to time_tool.sh for single divisions\n");
   fprintf(stderr, "       -R Produces calls to time_tool.sh for region multiplications\n");
   fprintf(stderr, "       -B Produces calls to time_tool.sh for the fastest region multiplications\n");
   fprintf(stderr, "       Cannot combine L, U, T.\n");
   if (s != NULL) {
     fprintf(stderr, "\n");
     fprintf(stderr, "%s\n", s);
   }
   exit(1);
}

void print_methods(gf_t *gf)
{
#ifdef DEBUG_FUNCTIONS
    gf_internal_t *h = (gf_internal_t*) gf->scratch;

    printf("multiply = %s\n", h->multiply);
    printf("divide = %s\n", h->divide);
    printf("inverse = %s\n", h->inverse);
    printf("multiply_region = %s\n", h->multiply_region);
    printf("extract_word = %s\n", h->extract_word);
#endif
}

int main(int argc, char *argv[])
{
  int m, r, d, w, i, sa, j, k, reset, ok;
  int nregions;
  int nmults;
  char **regions;
  char **mults;
  int exhaustive = 0;
  int divide = 0;
  int cauchy = 0;
  int listing;
  char *gf_argv[50], *x;
  gf_t gf;
  char ls[10];
  char * w_str;

  if (argc != 4) usage(NULL);
  w = atoi(argv[1]);
  ok = (w >= 1 && w <= 32);
  if (w == 64) ok = 1;
  if (w == 128) ok = 1;
  if (!ok) usage("Bad w");
  
  if (argv[2][0] != '-' || argv[3][0] != '-' || strlen(argv[2]) == 1 || strlen(argv[3]) != 2) {
    usage(NULL);
  }
  for (i = 1; argv[2][i] != '\0'; i++) {
    switch(argv[2][i]) {
      case 'B': exhaustive = 0; break;
      case 'A': exhaustive = 1; break;
      case 'D': divide = 1; break;
      case 'C': cauchy = 1; break;
      default: usage("Bad -BADC");
    }
  }

  if (strchr("LXUMDRB", argv[3][1]) == NULL) { usage("Bad -LXUMDRB"); }
  listing = argv[3][1];

  if (listing == 'U') {
    w_str = "../test/gf_unit %d A -1";
  } else if (listing == 'L' || listing == 'X') {
    w_str = "w=%d:";
  } else {
    w_str = strdup("sh time_tool.sh X %d");
    x = strchr(w_str, 'X');
    *x = listing;
  }

  gf_argv[0] = "-";
  if (create_gf_from_argv(&gf, w, 1, gf_argv, 0) > 0) {
    printf(w_str, w);
    printf(" - \n");
    gf_free(&gf, 1);
  } else if (_gf_errno == GF_E_DEFAULT) {
    fprintf(stderr, "Unlabeled failed method: w=%d: -\n", 2);
    exit(1);
  }

  nregions = (exhaustive) ? NREGIONS : BNREGIONS;
  if (!cauchy) nregions--;
  regions = (exhaustive) ? REGIONS : BREGIONS;
  mults = (exhaustive) ? MULTS : BMULTS;
  nmults = (exhaustive) ? NMULTS : BNMULTS;


  for (m = 0; m < nmults; m++) {
    sa = 0;
    gf_argv[sa++] = "-m";
    if (strcmp(mults[m], "GROUP44") == 0) {
      gf_argv[sa++] = "GROUP";
      gf_argv[sa++] = "4";
      gf_argv[sa++] = "4";
    } else if (strcmp(mults[m], "GROUP48") == 0) {
      gf_argv[sa++] = "GROUP";
      gf_argv[sa++] = "4";
      gf_argv[sa++] = "8";
    } else if (strcmp(mults[m], "SPLIT2") == 0) {
      gf_argv[sa++] = "SPLIT";
      sprintf(ls, "%d", w);
      gf_argv[sa++] = ls;
      gf_argv[sa++] = "2";
    } else if (strcmp(mults[m], "SPLIT4") == 0) {
      gf_argv[sa++] = "SPLIT";
      sprintf(ls, "%d", w);
      gf_argv[sa++] = ls;
      gf_argv[sa++] = "4";
    } else if (strcmp(mults[m], "SPLIT8") == 0) {
      gf_argv[sa++] = "SPLIT";
      sprintf(ls, "%d", w);
      gf_argv[sa++] = ls;
      gf_argv[sa++] = "8";
    } else if (strcmp(mults[m], "SPLIT16") == 0) {
      gf_argv[sa++] = "SPLIT";
      sprintf(ls, "%d", w);
      gf_argv[sa++] = ls;
      gf_argv[sa++] = "16";
    } else if (strcmp(mults[m], "SPLIT88") == 0) {
      gf_argv[sa++] = "SPLIT";
      gf_argv[sa++] = "8";
      gf_argv[sa++] = "8";
    } else if (strcmp(mults[m], "COMPOSITE") == 0) {
      gf_argv[sa++] = "COMPOSITE";
      gf_argv[sa++] = "2";
      gf_argv[sa++] = "-";
    } else {
      gf_argv[sa++] = mults[m];
    }
    reset = sa;


    for (r = 0; r < (1 << nregions); r++) {
      sa = reset;
      for (k = 0; k < nregions; k++) {
        if (r & (1 << k)) {
          gf_argv[sa++] = "-r";
          gf_argv[sa++] = regions[k];
        }
      }
      gf_argv[sa++] = "-";

      /* printf("Hmmmm. %s", gf_argv[0]);
      for (j = 0; j < sa; j++) printf(" %s", gf_argv[j]);
      printf("\n");  */
  
      if (create_gf_from_argv(&gf, w, sa, gf_argv, 0) > 0) {
        printf(w_str, w);
        for (j = 0; j < sa; j++) printf(" %s", gf_argv[j]);
        printf("\n");
        if (listing == 'X')
          print_methods(&gf);
        gf_free(&gf, 1);
      } else if (_gf_errno == GF_E_DEFAULT) {
        fprintf(stderr, "Unlabeled failed method: w=%d:", w);
        for (j = 0; j < sa; j++) fprintf(stderr, " %s", gf_argv[j]);
        fprintf(stderr, "\n");
        exit(1);
      }
      sa--;
      if (divide) {
        for (d = 0; d < NDIVS; d++) {
          gf_argv[sa++] = "-d";
          gf_argv[sa++] = divides[d];
          /*          printf("w=%d:", w);
                      for (j = 0; j < sa; j++) printf(" %s", gf_argv[j]);
                      printf("\n"); */
          gf_argv[sa++] = "-";
          if (create_gf_from_argv(&gf, w, sa, gf_argv, 0) > 0) {
            printf(w_str, w);
            for (j = 0; j < sa; j++) printf(" %s", gf_argv[j]);
            printf("\n");
            if (listing == 'X')
              print_methods(&gf);
            gf_free(&gf, 1);
          } else if (_gf_errno == GF_E_DEFAULT) {
            fprintf(stderr, "Unlabeled failed method: w=%d:", w);
            for (j = 0; j < sa; j++) fprintf(stderr, " %s", gf_argv[j]);
            fprintf(stderr, "\n");
            exit(1);
          } 
          sa-=3;
        }
      }
    }
  }
  return 0;
}
