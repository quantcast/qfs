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

/*
 * C. Tian Comment 2014: 1. removed most of the functions that are not needed for GF(8)
 *                       2. changed the function interfaces to allow initilizd gf_t as the last argument for thread safety reason	                
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "galois.h"

gf_t* galois_init_default_field(int w)
{
  // CT: only deals with w=8 here
  if(w!=8)
	return(NULL);
  gf_t *gfp=(gf_t*)malloc(sizeof(gf_t));
  if (gfp== NULL) {
      fprintf(stderr, "ERROR -- cannot allocate memory for Galois field w=%d\n", w);
      exit(1);   
  }

  if (!gf_init_easy(gfp, w)) {
    fprintf(stderr, "ERROR -- cannot init default Galois field for w=%d\n", w);
    exit(1);
  }
  return(gfp);
}


int galois_single_multiply(int x, int y, int w, gf_t* gfp)
{
  if (x == 0 || y == 0) return 0;
  
  if (gfp == NULL) {
        fprintf(stderr, "ERROR -- field not correctly initialized w=8\n");
	exit(1);
  }

  return gfp->multiply.w32(gfp, x, y);  
}

int galois_single_divide(int x, int y, int w, gf_t* gfp)
{
  if (x == 0) return 0;
  if (y == 0) return -1;
  
  if (gfp == NULL) {
        fprintf(stderr, "ERROR -- field not correctly initialized w=8\n");
	exit(1);
  }
  return gfp->divide.w32(gfp, x, y);
}

void galois_w08_region_multiply(char *region,      /* Region to multiply */
                                  int multby,       /* Number to multiply by */
                                  int nbytes,        /* Number of bytes in region */
                                  char *r2,          /* If r2 != NULL, products go here */
                                  int add,
				  gf_t* gfp)
{
  
  if (gfp == NULL) {
        fprintf(stderr, "ERROR -- field not correctly initialized w=8\n");
	exit(1);
  }
  gfp->multiply_region.w32(gfp, region, r2, multby, nbytes, add);
}

void galois_w8_region_xor(void *src, void *dest, int nbytes, gf_t* gfp)
{
   if (gfp == NULL) {
        fprintf(stderr, "ERROR -- field not correctly initialized w=8\n");
	exit(1);
  }
  // CT comment: we can use NULL here because when the value to be multiplied is 0 or 1, the gf_t structure is not actually used
  gfp->multiply_region.w32(NULL, src, dest, 1, nbytes, 1); 

}

void galois_region_xor(char *src, char *dest, int nbytes, gf_t* gfp)
{
  if (nbytes >= 16) {
    galois_w8_region_xor(src, dest, nbytes, gfp);
  } else {
    int i = 0;
    for (i = 0; i < nbytes; i++) {
      *dest ^= *src;
      dest++;
      src++;
    } 
  }
}

int galois_inverse(int y, int w, gf_t* gfp)
{
  if (y == 0) return -1;
  return galois_single_divide(1, y, w, gfp);
}
