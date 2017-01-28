/*
 * GF-Complete: A Comprehensive Open Source Library for Galois Field Arithmetic
 * James S. Plank, Ethan L. Miller, Kevin M. Greenan,
 * Benjamin A. Arnold, John A. Burnum, Adam W. Disney, Allen C. McBride.
 *
 * gf_cpu.h
 *
 * Identifies whether the CPU supports SIMD instructions at runtime.
 */

#include <stdio.h>
#include <stdlib.h>

int gf_cpu_identified = 0;

int gf_cpu_supports_intel_pclmul = 0;
int gf_cpu_supports_intel_sse4 = 0;
int gf_cpu_supports_intel_ssse3 = 0;
int gf_cpu_supports_intel_sse3 = 0;
int gf_cpu_supports_intel_sse2 = 0;
int gf_cpu_supports_arm_neon = 0;

#if defined(__x86_64__)

/* CPUID Feature Bits */

/* ECX */
#define GF_CPU_SSE3     (1 << 0)
#define GF_CPU_PCLMUL   (1 << 1)
#define GF_CPU_SSSE3    (1 << 9)
#define GF_CPU_SSE41    (1 << 19)
#define GF_CPU_SSE42    (1 << 20)

/* EDX */
#define GF_CPU_SSE2     (1 << 26)

#if defined(_MSC_VER)

#define cpuid(info, x)    __cpuidex(info, x, 0)

#elif defined(__GNUC__)

#include <cpuid.h>
void cpuid(int info[4], int InfoType){
    __cpuid_count(InfoType, 0, info[0], info[1], info[2], info[3]);
}

#else

#error please add a way to detect CPU SIMD support at runtime 

#endif

void gf_cpu_identify(void)
{
  if (gf_cpu_identified) {
      return;
  }

  int reg[4];

  cpuid(reg, 1);

#if defined(INTEL_SSE4_PCLMUL)
  if ((reg[2] & GF_CPU_PCLMUL) != 0 && !getenv("GF_COMPLETE_DISABLE_SSE4_PCLMUL")) {
      gf_cpu_supports_intel_pclmul = 1;
#ifdef DEBUG_CPU_DETECTION
      printf("#gf_cpu_supports_intel_pclmul\n");
#endif
  }
#endif

#if defined(INTEL_SSE4)
  if (((reg[2] & GF_CPU_SSE42) != 0 || (reg[2] & GF_CPU_SSE41) != 0) && !getenv("GF_COMPLETE_DISABLE_SSE4")) {
      gf_cpu_supports_intel_sse4 = 1;
#ifdef DEBUG_CPU_DETECTION
      printf("#gf_cpu_supports_intel_sse4\n");
#endif
  }
#endif

#if defined(INTEL_SSSE3)
  if ((reg[2] & GF_CPU_SSSE3) != 0 && !getenv("GF_COMPLETE_DISABLE_SSSE3")) {
      gf_cpu_supports_intel_ssse3 = 1;
#ifdef DEBUG_CPU_DETECTION
      printf("#gf_cpu_supports_intel_ssse3\n");
#endif
  }
#endif

#if defined(INTEL_SSE3)
  if ((reg[2] & GF_CPU_SSE3) != 0 && !getenv("GF_COMPLETE_DISABLE_SSE3")) {
      gf_cpu_supports_intel_sse3 = 1;
#ifdef DEBUG_CPU_DETECTION
      printf("#gf_cpu_supports_intel_sse3\n");
#endif
  }
#endif

#if defined(INTEL_SSE2)
  if ((reg[3] & GF_CPU_SSE2) != 0 && !getenv("GF_COMPLETE_DISABLE_SSE2")) {
      gf_cpu_supports_intel_sse2 = 1;
#ifdef DEBUG_CPU_DETECTION
      printf("#gf_cpu_supports_intel_sse2\n");
#endif
  }
#endif

  gf_cpu_identified = 1;
}

#elif defined(__arm__) || defined(__aarch64__)

#ifdef __linux__

#include <stdio.h>
#include <unistd.h>
#include <elf.h>
#include <linux/auxvec.h>
#include <asm/hwcap.h>
#include <fcntl.h>

unsigned long get_hwcap(unsigned long type) {
    unsigned long hwcap = 0; 
    int fd = open("/proc/self/auxv", O_RDONLY);
    if (fd > 0) {
        Elf32_auxv_t auxv;
        while (read(fd, &auxv, sizeof(Elf32_auxv_t))) {
            if (auxv.a_type == type) {
                hwcap = auxv.a_un.a_val;
                break;
            }
        }
        close(fd);
    }

    return hwcap;
}

#endif // linux

void gf_cpu_identify(void)
{
  if (gf_cpu_identified) {
      return;
  }

#if defined(ARM_NEON)
  if (!getenv("GF_COMPLETE_DISABLE_NEON")) {
#if __linux__ && __arm__
	  gf_cpu_supports_arm_neon = (get_hwcap(AT_HWCAP) & HWCAP_NEON) > 0;
#elif __aarch64__
    // ASIMD is supported on all aarch64 architectures
	  gf_cpu_supports_arm_neon = 1;
#else
    // we assume that NEON is supported if the compiler supports
    // NEON and we dont have a reliable way to detect runtime support.
	  gf_cpu_supports_arm_neon = 1;
#endif

#ifdef DEBUG_CPU_DETECTION
    if (gf_cpu_supports_arm_neon) {
      printf("#gf_cpu_supports_arm_neon\n");
    }
#endif
  }
#endif // defined(ARM_NEON)

  gf_cpu_identified = 1;
}

#else // defined(__arm__) || defined(__aarch64__)

int gf_cpu_identify(void)
{
    gf_cpu_identified = 1;
    return 0;
}

#endif
