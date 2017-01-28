/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_cpu.h
 *
 * Identifies whether the CPU supports SIMD instructions at runtime.
 */

#pragma once

extern int gf_cpu_supports_intel_pclmul;
extern int gf_cpu_supports_intel_sse4;
extern int gf_cpu_supports_intel_ssse3;
extern int gf_cpu_supports_intel_sse3;
extern int gf_cpu_supports_intel_sse2;
extern int gf_cpu_supports_arm_neon;

void gf_cpu_identify(void);
