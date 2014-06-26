/* *
 * Copyright (c) 2013, James S. Plank and Kevin Greenan
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

/*
 * C. Tian Comment 2014: 1. removed most of the functions that are not needed for GF(8)
 *                       2. changed the function interfaces to allow initilizd gf_t as the last argument for thread safety reason	                
 */


#ifndef _GALOIS_H
#define _GALOIS_H

#include <stdio.h>
#include <stdlib.h>
#include <gf_complete.h>

extern int galois_single_multiply(int a, int b, int w, gf_t* gfp);
extern int galois_single_divide(int a, int b, int w, gf_t* gfp);
extern int galois_inverse(int x, int w, gf_t* gfp);

void galois_region_xor(           char *src,         // Source Region 
                                  char *dest,        // Dest Region (holds result) 
                                  int nbytes,       // Number of bytes in region 
				  gf_t* gfp);       // the field pointer, should be initialized
/* These multiply regions in w=8.  They are much faster
   than calling galois_single_multiply.  The regions must be long word aligned. */

void galois_w08_region_multiply(char *region,       /* Region to multiply */
                                  int multby,       /* Number to multiply by */
                                  int nbytes,       /* Number of bytes in region */
                                  char *r2,         /* If r2 != NULL, products go here.  
                                                       Otherwise region is overwritten */
                                  int add,         /* If (r2 != NULL && add) the produce is XOR'd with r2 */
				  gf_t* gfp);       // the field pointer, should be initialized
 
gf_t* galois_init_default_field(int w);

#endif
