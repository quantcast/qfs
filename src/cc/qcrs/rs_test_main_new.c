/*---------------------------------------------------------- -*- Mode: C -*-----
 * $Id$
 *
 * Created 2010/07/24
 * Author: Dan Adkins
 * Edited by Chao Tian (AT&T) 2014/3/25
 *
 * Copyright 2010-2011 Quantcast Corp.
 *
 * This file is part of Kosmos File System (KFS).
 *
 * Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * \file rs_table.h
 * \brief Reed Solomon encoder and decoder unit test.
 *
 *------------------------------------------------------------------------------
 */
#define _XOPEN_SOURCE 600

#include "jerasure.h"
#include "rs.h"
#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include "cauchy.h"

/* Return 0 if equal, -1 otherwise. */
static int
compare(int nblocks, int blocksize, void **x, void **y)
{
    int i;

    for (i = 0; i < nblocks; i++)
        if (memcmp(x[i], y[i], blocksize) != 0)
            return (i+1);
    return 0;
}

static void
mkrand(void *buf, int size)
{
    char *p;
    int i;

    p = buf;
    for (i = 0; i < size; i++)
        p[i] = rand();
}

void *data[RS_LIB_MAX_DATA_BLOCKS+RS_LIB_MAX_RECOVERY_BLOCKS];
void *orig[RS_LIB_MAX_DATA_BLOCKS+RS_LIB_MAX_RECOVERY_BLOCKS];
void *p[RS_LIB_MAX_DATA_BLOCKS+RS_LIB_MAX_RECOVERY_BLOCKS];

int main(int argc, char **argv)
{
    if (argc > 1 && (!strcmp(argv[1], "-h") || !strcmp(argv[1], "--help")) || argc < 4) {
        printf("Usage: %s [data blocks] [recovery blocks] [block size] [packet size] [perf iterations]\n"
               "       This tests the Reed Solomon encoder and decoder.\n"
               "       0 < data blocks <= %d.\n"
	       "       0 < recovery blocks <=%d.\n"	
	       "       0 < block size <=4M.\n"	
	       "       0 < packet size <=4096.\n"	
               "       Use perf iterations for performance test (omit it for correctness test).\n"
               "       Defaults: data blocks=6, block size=%d\n", argv[0],
               RS_LIB_MAX_DATA_BLOCKS, RS_LIB_MAX_RECOVERY_BLOCKS, (64 << 10));
        exit(0);
    }
    srand ( time(NULL) );
    int i, j, k, n, m, err;
    const int N = argc > 1 ? atoi(argv[1]) : 6;
    const int M = argc > 2 ? atoi(argv[2]) : 3;
    int BLOCKSIZE = argc > 3 ? atoi(argv[3]) : (64 << 10);
    const int PACKETSIZE = argc > 4 ? atoi(argv[4]) : (64 << 10);
    const int ALPHABETSIZE = 8;
    int* matrix=NULL;
    int* bitmatrix=NULL;
    int** schedule=NULL;
    int* erasures = NULL;
    int* erased = NULL;
    gf_t* gfp = galois_init_default_field(8);
    if(gfp==NULL) {
	printf("Cannot initialize Galois field\n");
	return 1;
    }
   
    if (N <= 0 || N > RS_LIB_MAX_DATA_BLOCKS || M>RS_LIB_MAX_RECOVERY_BLOCKS) {
        printf("0 < data blocks <= %d, 0 < recovery blocks <= %d, N+M<%d\n", RS_LIB_MAX_DATA_BLOCKS,RS_LIB_MAX_RECOVERY_BLOCKS,RS_LIB_MAX_DATA_BLOCKS);
        return 1;
    }
	
    BLOCKSIZE = (BLOCKSIZE/(ALPHABETSIZE*PACKETSIZE))*(ALPHABETSIZE*PACKETSIZE);
    for (i = 0; i < N+M; i++) {
        if ((err = posix_memalign(data + i, 16, BLOCKSIZE)) ||
                (err = posix_memalign(orig + i, 16, BLOCKSIZE))) {
            printf("%s\n", strerror(err));
            return 1;
        }
        memset(data[i], 0, BLOCKSIZE);
    }
    erasures=malloc(sizeof(int)*(M+1));
    erased = malloc(sizeof(int)*(M+N));
    
    memcpy(p,data,sizeof(void*)*(RS_LIB_MAX_DATA_BLOCKS+RS_LIB_MAX_RECOVERY_BLOCKS));

    

    if (argc > 5) {
        clock_t clk, tclk = 0;
        double  tbytes = 0;
        // Performance test.
        n = atoi(argv[5]);
        for (i = 0; i < N+M; i++)
            mkrand(data[i], BLOCKSIZE); 
	{
		matrix = cauchy_good_general_coding_matrix(N, M, ALPHABETSIZE, gfp);
	   	if (matrix == NULL) {
		 printf("couldn't make coding matrix.\n");
	    	}
		clk = clock();
		for (i = 0; i < n; i++)
			jerasure_matrix_encode(N, M, ALPHABETSIZE, matrix,
				(char**)data, (char**)(data+N), BLOCKSIZE,gfp);

		if(matrix!=NULL){
			free(matrix);
			matrix = NULL;
		}
		clk = clock() - clk;
	}

	printf("%.3e,  ", BLOCKSIZE * N * (double)CLOCKS_PER_SEC * n /((double)clk > 0 ? (double)clk : 1e-10));
	
	for(i=0;i<20;i++){
		memset(erased,0,sizeof(int)*(M+N));
		j=0;
		while(j<M){
			k=rand()%(M+N);
			if(erased[k]!=1){
				erasures[j] = k;
				j++;
				erased[k]=1;
			}
		}
		erasures[M] = -1;	
	
		for(j=0;j<M;j++){
			if(erasures[j]>=N)
				data[erasures[j]] = 0; // no recovery of parity
		}             
                clk = clock();

		matrix = cauchy_good_general_coding_matrix(N, M, ALPHABETSIZE,gfp);
		if (matrix == NULL) {
			printf("couldn't make coding matrix.\n");
		}
		for (m = 0; m < n; m++)
			jerasure_matrix_decode_adaptive(N, M, ALPHABETSIZE, matrix,erasures,
				(char**)data, (char**)(data+N),  BLOCKSIZE,gfp);
		if(matrix!=NULL){
			free(matrix);	
			matrix = NULL;
		}
		

                clk = clock() - clk;
		memcpy(data,p,sizeof(void*)*(RS_LIB_MAX_DATA_BLOCKS+RS_LIB_MAX_RECOVERY_BLOCKS));

		/*printf("decode missing: ");
		for(j=0;j<M;j++)
			printf("%d ", erasures[j]);
                printf(" %.3e clocks %.3e sec %.3e bytes/sec\n",
                     (double)clk, (double)clk/CLOCKS_PER_SEC,
                     BLOCKSIZE * N * (double)CLOCKS_PER_SEC * n /
                         ((double)clk > 0 ? (double)clk : 1e-10));
		*/
                tbytes += (double)BLOCKSIZE * N * n;
                tclk += clk;
                
        }
	/*
        printf("decode average:      "
            " %.3e clocks %.3e sec %.3e bytes/sec\n",
            (double)tclk, (double)tclk/CLOCKS_PER_SEC,
            tbytes * (double)CLOCKS_PER_SEC /
                ((double)tclk > 0 ? (double)tclk : 1e-10));
	*/
	printf("%.3e; \n", tbytes * (double)CLOCKS_PER_SEC /((double)tclk > 0 ? (double)tclk : 1e-10));
	if(erasures!=NULL);
		free(erasures);
	if(erased!=NULL);
		free(erased);
	if(gfp!=NULL)
		gf_free(gfp,1);
        return 0;
    }

    // testing the correctness

    for (n = 0; n < 120; n++) {
        for (i = 0; i < N; i++)
               mkrand(data[i], BLOCKSIZE);   

	
	matrix = cauchy_good_general_coding_matrix(N, M, ALPHABETSIZE, gfp);
	if (matrix == NULL) {
		printf("couldn't make coding matrix.\n");
	}
		
	jerasure_matrix_encode(N, M, ALPHABETSIZE, matrix,
		(char**)data, (char**)(data+N), BLOCKSIZE, gfp);

	if(matrix!=NULL){
		free(matrix);
		matrix = NULL;
	}	

        for (i = 0; i < N+M; i++)
            memmove(orig[i], data[i], BLOCKSIZE);


	memset(erased,0,sizeof(int)*(M+N));
	j=0;
	while(j<M){
		k=rand()%(M+N);
		if(erased[k]!=1){
			erasures[j] = k;
			memset(data[k],0,BLOCKSIZE);
			j++;
			erased[k]=1;
		}
	}
	erasures[M] = -1;

	matrix = cauchy_good_general_coding_matrix(N, M, ALPHABETSIZE, gfp);
	if (matrix == NULL) {
		printf("couldn't make coding matrix.\n");
	}
	jerasure_matrix_decode_adaptive(N, M, ALPHABETSIZE, matrix,erasures,
			(char**)data, (char**)(data+N),  BLOCKSIZE, gfp);
	if(matrix!=NULL){
		free(matrix);	
		matrix = NULL;
	}
	if ((j=compare(N+M, BLOCKSIZE, data, orig)) != 0) {
                    printf("FAILED: %d-th block\n", j-1);
                    return 1;
        }		        
    }
    printf("RAMDOM ERASURE TEST PASS\n");
               
    if(erasures!=NULL);
	free(erasures);
    if(erased!=NULL);
	free(erased);
    if(gfp!=NULL)
	gf_free(gfp,1);
    return 0;
}
