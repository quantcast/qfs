/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_poly.c - program to help find irreducible polynomials in composite fields,
 * using the Ben-Or algorithm.  
 * 
 * (This one was written by Jim) 
 * 
 * Please see the following paper for a description of the Ben-Or algorithm:
 * 
 * author    S. Gao and D. Panario
 * title     Tests and Constructions of Irreducible Polynomials over Finite Fields
 * booktitle Foundations of Computational Mathematics
 * year      1997
 * publisher Springer Verlag
 * pages     346-361
 * 
 * The basic technique is this.  You have a polynomial f(x) whose coefficients are
 * in a base field GF(2^w).  The polynomial is of degree n.  You need to do the 
 * following for all i from 1 to n/2:
 * 
 * Construct x^(2^w)^i modulo f.  That will be a polynomial of maximum degree n-1
 * with coefficients in GF(2^w).  You construct that polynomial by starting with x
 * and doubling it w times, each time taking the result modulo f.  Then you 
 * multiply that by itself i times, again each time taking the result modulo f.
 * 
 * When you're done, you need to "subtract" x -- since addition = subtraction = 
 * XOR, that means XOR x.  
 * 
 * Now, find the GCD of that last polynomial and f, using Euclid's algorithm.  If
 * the GCD is not one, then f is reducible.  If it is not reducible for each of
 * those i, then it is irreducible.
 * 
 * In this code, I am using a gf_general_t to represent elements of GF(2^w).  This
 * is so that I can use base fields that are GF(2^64) or GF(2^128). 
 * 
 * I have two main procedures.  The first is x_to_q_to_i_minus_x, which calculates
 * x^(2^w)^i - x, putting the result into a gf_general_t * called retval.
 * 
 * The second is gcd_one, which takes a polynomial of degree n and a second one
 * of degree n-1, and uses Euclid's algorithm to decide if their GCD == 1.
 * 
 * These can be made faster (e.g. calculate x^(2^w) once and store it).
 */

#include "gf_complete.h"
#include "gf_method.h"
#include "gf_general.h"
#include "gf_int.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

char *BM = "Bad Method: ";

void usage(char *s)
{
  fprintf(stderr, "usage: gf_poly w(base-field) method power:coef [ power:coef .. ]\n");
  fprintf(stderr, "\n");
  fprintf(stderr, "       use - for the default method.\n");
  fprintf(stderr, "       use 0x in front of the coefficient if it's in hex\n");
  fprintf(stderr, "       \n");
  fprintf(stderr, "       For example, to test whether x^2 + 2x + 1 is irreducible\n");
  fprintf(stderr, "       in GF(2^16), the call is:\n");
  fprintf(stderr, "       \n");
  fprintf(stderr, "       gf_poly 16 - 2:1 1:2 0:1\n");
  fprintf(stderr, "       \n");
  fprintf(stderr, "       See the user's manual for more information.\n");
  if (s != NULL) {
    fprintf(stderr, "\n");
    if (s == BM) {
      fprintf(stderr, "%s", s);
      gf_error();
    } else {
      fprintf(stderr, "%s\n", s);
    }
  }
  exit(1);
}

int gcd_one(gf_t *gf, int w, int n, gf_general_t *poly, gf_general_t *prod)
{
  gf_general_t *a, *b, zero, factor, p;
  int i, j, da, db;

  gf_general_set_zero(&zero, w);

  a = (gf_general_t *) malloc(sizeof(gf_general_t) * n+1);
  b = (gf_general_t *) malloc(sizeof(gf_general_t) * n);
  for (i = 0; i <= n; i++) gf_general_add(gf, &zero, poly+i, a+i);
  for (i = 0; i < n; i++) gf_general_add(gf, &zero, prod+i, b+i);

  da = n;
  while (1) {
    for (db = n-1; db >= 0 && gf_general_is_zero(b+db, w); db--) ;
    if (db < 0) return 0;
    if (db == 0) return 1;
    for (j = da; j >= db; j--) {
      if (!gf_general_is_zero(a+j, w)) {
        gf_general_divide(gf, a+j, b+db, &factor);
        for (i = 0; i <= db; i++) {
          gf_general_multiply(gf, b+i, &factor, &p); 
          gf_general_add(gf, &p, a+(i+j-db), a+(i+j-db));
        }
      }
    }
    for (i = 0; i < n; i++) {
      gf_general_add(gf, a+i, &zero, &p);
      gf_general_add(gf, b+i, &zero, a+i);
      gf_general_add(gf, &p, &zero, b+i);
    }
  }

}

void x_to_q_to_i_minus_x(gf_t *gf, int w, int n, gf_general_t *poly, int logq, int i, gf_general_t *retval)
{
  gf_general_t x;
  gf_general_t *x_to_q;
  gf_general_t *product;
  gf_general_t p, zero, factor;
  int j, k, lq;

  gf_general_set_zero(&zero, w);
  product = (gf_general_t *) malloc(sizeof(gf_general_t) * n*2);
  x_to_q = (gf_general_t *) malloc(sizeof(gf_general_t) * n);
  for (j = 0; j < n; j++) gf_general_set_zero(x_to_q+j, w);
  gf_general_set_one(x_to_q+1, w);

  for (lq = 0; lq < logq; lq++) {
    for (j = 0; j < n*2; j++) gf_general_set_zero(product+j, w);
    for (j = 0; j < n; j++) {
      for (k = 0; k < n; k++) {
        gf_general_multiply(gf, x_to_q+j, x_to_q+k, &p);
        gf_general_add(gf, product+(j+k), &p, product+(j+k));
      }
    }
    for (j = n*2-1; j >= n; j--) {
      if (!gf_general_is_zero(product+j, w)) {
        gf_general_add(gf, product+j, &zero, &factor);
        for (k = 0; k <= n; k++) {
          gf_general_multiply(gf, poly+k, &factor, &p);
          gf_general_add(gf, product+(j-n+k), &p, product+(j-n+k));
        }
      }
    }
    for (j = 0; j < n; j++) gf_general_add(gf, product+j, &zero, x_to_q+j);
  }
  for (j = 0; j < n; j++) gf_general_set_zero(retval+j, w);
  gf_general_set_one(retval, w);

  while (i > 0) {
    for (j = 0; j < n*2; j++) gf_general_set_zero(product+j, w);
    for (j = 0; j < n; j++) {
      for (k = 0; k < n; k++) {
        gf_general_multiply(gf, x_to_q+j, retval+k, &p);
        gf_general_add(gf, product+(j+k), &p, product+(j+k));
      }
    }
    for (j = n*2-1; j >= n; j--) {
      if (!gf_general_is_zero(product+j, w)) {
        gf_general_add(gf, product+j, &zero, &factor);
        for (k = 0; k <= n; k++) {
          gf_general_multiply(gf, poly+k, &factor, &p);
          gf_general_add(gf, product+(j-n+k), &p, product+(j-n+k));
        }
      }
    }
    for (j = 0; j < n; j++) gf_general_add(gf, product+j, &zero, retval+j);
    i--;
  }

  gf_general_set_one(&x, w);
  gf_general_add(gf, &x, retval+1, retval+1);

  free(product);
  free(x_to_q);
}

int main(int argc, char **argv)
{
  int w, i, power, n, ap, success;
  gf_t gf;
  gf_general_t *poly, *prod;
  char *string, *ptr;
  char buf[100];

  if (argc < 4) usage(NULL);

  if (sscanf(argv[1], "%d", &w) != 1 || w <= 0) usage("Bad w.");
  ap = create_gf_from_argv(&gf, w, argc, argv, 2);

  if (ap == 0) usage(BM);

  if (ap == argc) usage("No powers/coefficients given.");

  n = -1;
  for (i = ap; i < argc; i++) {
    if (strchr(argv[i], ':') == NULL || sscanf(argv[i], "%d:", &power) != 1) {
      string = (char *) malloc(sizeof(char)*(strlen(argv[i]+100)));
      sprintf(string, "Argument '%s' not in proper format of power:coefficient\n", argv[i]);
      usage(string);
    }
    if (power < 0) {
      usage("Can't have negative powers\n");
    } else {
      n = power;
    }
  }
  // in case the for-loop header fails
  assert (n >= 0);

  poly = (gf_general_t *) malloc(sizeof(gf_general_t)*(n+1));
  for (i = 0; i <= n; i++) gf_general_set_zero(poly+i, w);
  prod = (gf_general_t *) malloc(sizeof(gf_general_t)*n);

  for (i = ap; i < argc; i++) {
    sscanf(argv[i], "%d:", &power);
    ptr = strchr(argv[i], ':');
    ptr++;
    if (strncmp(ptr, "0x", 2) == 0) {
      success = gf_general_s_to_val(poly+power, w, ptr+2, 1);
    } else {
      success = gf_general_s_to_val(poly+power, w, ptr, 0);
    }
    if (success == 0) {
      string = (char *) malloc(sizeof(char)*(strlen(argv[i]+100)));
      sprintf(string, "Argument '%s' not in proper format of power:coefficient\n", argv[i]);
      usage(string);
    }
  }

  printf("Poly:");
  for (power = n; power >= 0; power--) {
    if (!gf_general_is_zero(poly+power, w)) {
      printf("%s", (power == n) ? " " : " + ");
      if (!gf_general_is_one(poly+power, w)) {
        gf_general_val_to_s(poly+power, w, buf, 1);
        if (n > 0) {
          printf("(0x%s)", buf);
        } else {
          printf("0x%s", buf);
        }
      }
      if (power == 0) {
        if (gf_general_is_one(poly+power, w)) printf("1");
      } else if (power == 1) {
        printf("x");
      } else {
        printf("x^%d", power);
      }
    }
  }
  printf("\n");

  if (!gf_general_is_one(poly+n, w)) {
    printf("\n");
    printf("Can't do Ben-Or, because the polynomial is not monic.\n");
    exit(0);
  }

  for (i = 1; i <= n/2; i++) {
    x_to_q_to_i_minus_x(&gf, w, n, poly, w, i, prod); 
    if (!gcd_one(&gf, w, n, poly, prod)) {
      printf("Reducible.\n");
      exit(0);
    }
  }
  
  printf("Irreducible.\n");
  exit(0);
}
