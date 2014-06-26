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
  C. Tian Comment 2014: 1. removed most of the functions that are not needed for GF(8)
                        2. changed the function interfaces to allow initilizd gf_t as the last argument for thread safety reason
	                
*/




#ifndef _JERASURE_H
#define _JERASURE_H

#ifdef __cplusplus
extern "C" {
#endif

/* This uses procedures from the Galois Field arithmetic library */

#include "galois.h"

/* ------------------------------------------------------------ */
/* In all of the routines below:

   k = Number of data devices
   m = Number of coding devices
   w = Word size

   data_ptrs = An array of k pointers to data which is size bytes.  
               Size must be a multiple of sizeof(long).
               Pointers must also be longword aligned.
 
   coding_ptrs = An array of m pointers to coding data which is size bytes.

   packetsize = The size of a coding block with bitmatrix coding. 
                When you code with a bitmatrix, you will use w packets
                of size packetsize.

   matrix = an array of k*m integers.  
            It represents an m by k matrix.
            Element i,j is in matrix[i*k+j];

   bitmatrix = an array of k*m*w*w integers.
            It represents an mw by kw matrix.
            Element i,j is in matrix[i*k*w+j];

   erasures = an array of id's of erased devices. 
              Id's are integers between 0 and k+m-1.
              Id's 0 to k-1 are id's of data devices.
              Id's k to k+m-1 are id's of coding devices: 
                  Coding device id = id-k.
              If there are e erasures, erasures[e] = -1.

   schedule = an array of schedule operations.  

              If there are m operations, then schedule[m][0] = -1.

   operation = an array of 5 integers:

          0 = operation: 0 for copy, 1 for xor (-1 for end)
          1 = source device (0 - k+m-1)
          2 = source packet (0 - w-1)
          3 = destination device (0 - k+m-1)
          4 = destination packet (0 - w-1)
 */

/* ------------------------------------------------------------ */
/* Encoding - these are all straightforward.  jerasure_matrix_encode only 
   works with w = 8.  */

void jerasure_matrix_encode(int k, int m, int w, int *matrix,
                          char **data_ptrs, char **coding_ptrs, int size, gf_t* gfp);

/* ------------------------------------------------------------ */
/* Decoding. -------------------------------------------------- */

/* These return integers, because the matrix may not be invertible. 
   
   The parameter row_k_ones should be set to 1 if row k of the matrix
   (or rows kw to (k+1)w+1) of th distribution matrix are all ones
   (or all identity matrices).  Then you can improve the performance
   of decoding when there is more than one failure, and the parity
   device didn't fail.  You do it by decoding all but one of the data
   devices, and then decoding the last data device from the data devices
   and the parity device.

   jerasure_schedule_decode_lazy generates the schedule on the fly.

   jerasure_matrix_decode only works when w = 8|16|32.

   jerasure_make_decoding_matrix/bitmatrix make the k*k decoding matrix
         (or wk*wk bitmatrix) by taking the rows corresponding to k
         non-erased devices of the distribution matrix, and then
         inverting that matrix.

         You should already have allocated the decoding matrix and
         dm_ids, which is a vector of k integers.  These will be
         filled in appropriately.  dm_ids[i] is the id of element
         i of the survivors vector.  I.e. row i of the decoding matrix
         times dm_ids equals data drive i.

         Both of these routines take "erased" instead of "erasures".
         Erased is a vector with k+m elements, which has 0 or 1 for 
         each device's id, according to whether the device is erased.
 
   jerasure_erasures_to_erased allocates and returns erased from erasures.
    
 */
int *jerasure_erasures_to_erased(int k, int m, int *erasures);


int jerasure_matrix_decode_adaptive(int k, int m, int w, int *matrix, int *erasures,
                          char **data_ptrs, char **coding_ptrs, int size, gf_t* gfp); 
/* ------------------------------------------------------------ */
/* These perform dot products and schedules. -------------------*/
/*
   src_ids is a matrix of k id's (0 - k-1 for data devices, k - k+m-1
   for coding devices) that identify the source devices.  Dest_id is
   the id of the destination device.

   jerasure_matrix_dotprod only works when w = 8|16|32.

   jerasure_do_scheduled_operations executes the schedule on w*packetsize worth of
   bytes from each device.  ptrs is an array of pointers which should have as many
   elements as the highest referenced device in the schedule.

 */
 
void jerasure_matrix_dotprod(int k, int w, int *matrix_row,
                          int *src_ids, int dest_id,
                          char **data_ptrs, char **coding_ptrs, int size, gf_t* gfp);
/* ------------------------------------------------------------ */
/* Matrix Inversion ------------------------------------------- */
/*
   The two matrix inversion functions work on rows*rows matrices of
   ints.  If a bitmatrix, then each int will just be zero or one.
   Otherwise, they will be elements of gf(2^w).  Obviously, you can
   do bit matrices with crs_invert_matrix() and set w = 1, but
   crs_invert_bitmatrix will be more efficient.

   The two invertible functions return whether a matrix is invertible.
   They are more efficient than the inverstion functions.

   Mat will be destroyed when the matrix inversion or invertible
   testing is done.  Sorry.

   Inv must be allocated by the caller.

   The two invert_matrix functions return 0 on success, and -1 if the
   matrix is uninvertible.

   The two invertible function simply return whether the matrix is
   invertible.  (0 or 1). Mat will be destroyed.
 */

int jerasure_invert_matrix(int *mat, int *inv, int rows, int w, gf_t* gfp);

#ifdef __cplusplus
}
#endif
#endif
