#
# This macro is based on http://www.gnu.org/software/autoconf-archive/ax_ext.html
# but simplified to do compile time SIMD checks only
#

AC_DEFUN([AX_EXT],
[
  AC_REQUIRE([AC_CANONICAL_HOST])

  case $host_cpu in
    aarch64*)
      AC_DEFINE(HAVE_ARCH_AARCH64,,[targeting AArch64])
      SIMD_FLAGS="$SIMD_FLAGS -DARCH_AARCH64"

      AC_CACHE_CHECK([whether NEON is enabled], [ax_cv_have_neon_ext], [ax_cv_have_neon_ext=yes])
      if test "$ax_cv_have_neon_ext" = yes; then
        AX_CHECK_COMPILE_FLAG(-march=armv8-a+simd, [SIMD_FLAGS="$SIMD_FLAGS -march=armv8-a+simd -DARM_NEON"], [ax_cv_have_neon_ext=no])
      fi
      ;;

    arm*)
      AC_CACHE_CHECK([whether NEON is enabled], [ax_cv_have_neon_ext], [ax_cv_have_neon_ext=yes])
      if test "$ax_cv_have_neon_ext" = yes; then
        AX_CHECK_COMPILE_FLAG(-mfpu=neon, [SIMD_FLAGS="$SIMD_FLAGS -mfpu=neon -DARM_NEON"], [ax_cv_have_neon_ext=no])
      fi
      ;;

    powerpc*)
      AC_CACHE_CHECK([whether altivec is enabled], [ax_cv_have_altivec_ext], [ax_cv_have_altivec_ext=yes])
      if test "$ax_cv_have_altivec_ext" = yes; then
        AX_CHECK_COMPILE_FLAG(-faltivec, [SIMD_FLAGS="$SIMD_FLAGS -faltivec"], [ax_cv_have_altivec_ext=no])
      fi
      ;;

    i[[3456]]86*|x86_64*|amd64*)

      AC_CACHE_CHECK([whether sse is enabled], [ax_cv_have_sse_ext], [ax_cv_have_sse_ext=yes])
      if test "$ax_cv_have_sse_ext" = yes; then
        AX_CHECK_COMPILE_FLAG(-msse, [SIMD_FLAGS="$SIMD_FLAGS -msse -DINTEL_SSE"], [ax_cv_have_sse_ext=no])
      fi

      AC_CACHE_CHECK([whether sse2 is enabled], [ax_cv_have_sse2_ext], [ax_cv_have_sse2_ext=yes])
      if test "$ax_cv_have_sse2_ext" = yes; then
        AX_CHECK_COMPILE_FLAG(-msse2, [SIMD_FLAGS="$SIMD_FLAGS -msse2 -DINTEL_SSE2"], [ax_cv_have_sse2_ext=no])
      fi

      AC_CACHE_CHECK([whether sse3 is enabled], [ax_cv_have_sse3_ext], [ax_cv_have_sse3_ext=yes])
      if test "$ax_cv_have_sse3_ext" = yes; then
        AX_CHECK_COMPILE_FLAG(-msse3, [SIMD_FLAGS="$SIMD_FLAGS -msse3 -DINTEL_SSE3"], [ax_cv_have_sse3_ext=no])
      fi

      AC_CACHE_CHECK([whether ssse3 is enabled], [ax_cv_have_ssse3_ext], [ax_cv_have_ssse3_ext=yes])
      if test "$ax_cv_have_ssse3_ext" = yes; then
        AX_CHECK_COMPILE_FLAG(-mssse3, [SIMD_FLAGS="$SIMD_FLAGS -mssse3 -DINTEL_SSSE3"], [ax_cv_have_ssse3_ext=no])
      fi

      AC_CACHE_CHECK([whether pclmuldq is enabled], [ax_cv_have_pclmuldq_ext], [ax_cv_have_pclmuldq_ext=yes])
      if test "$ax_cv_have_pclmuldq_ext" = yes; then
        AX_CHECK_COMPILE_FLAG(-mpclmul, [SIMD_FLAGS="$SIMD_FLAGS -mpclmul -DINTEL_SSE4_PCLMUL"], [ax_cv_have_pclmuldq_ext=no])
      fi

      AC_CACHE_CHECK([whether sse4.1 is enabled], [ax_cv_have_sse41_ext], [ax_cv_have_sse41_ext=yes])
      if test "$ax_cv_have_sse41_ext" = yes; then
        AX_CHECK_COMPILE_FLAG(-msse4.1, [SIMD_FLAGS="$SIMD_FLAGS -msse4.1 -DINTEL_SSE4"], [ax_cv_have_sse41_ext=no])
      fi

      AC_CACHE_CHECK([whether sse4.2 is enabled], [ax_cv_have_sse42_ext], [ax_cv_have_sse42_ext=yes])
      if test "$ax_cv_have_sse42_ext" = yes; then
        AX_CHECK_COMPILE_FLAG(-msse4.2, [SIMD_FLAGS="$SIMD_FLAGS -msse4.2 -DINTEL_SSE4"], [ax_cv_have_sse42_ext=no])
      fi
      ;;
  esac

  AC_SUBST(SIMD_FLAGS)
])
