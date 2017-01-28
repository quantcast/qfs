#!/bin/bash -e

# this scripts has a number of tests for SIMD. It can be invoked
# on the host or on a QEMU machine.

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
host_cpu=`uname -p`
results=${script_dir}/test_simd.results
nprocs=$(grep -c ^processor /proc/cpuinfo)

# runs unit tests and save the results
test_unit(){
    { ./configure && make clean && make; } || { echo "Compile FAILED" >> ${results}; return 1; }
    make -j$nprocs check || { echo "gf_methods $i FAILED" >> ${results}; ((++failed)); }
    cat tools/test-suite.log >> ${results} || true
}

# build with DEBUG_FUNCTIONS and save all methods selected
# to a results file
test_functions() {
    failed=0

    { ./configure --enable-debug-func && make clean && make; } || { echo "Compile FAILED" >> ${results}; return 1; }
    for i in 128 64 32 16 8 4; do
        { ${script_dir}/gf_methods $i -ACD -X >> ${results}; } || { echo "gf_methods $i FAILED" >> ${results}; ((++failed)); }
    done

    return ${failed}
}

# build with DEBUG_CPU_FUNCTIONS and print out CPU detection
test_detection() {
    failed=0

    { ./configure --enable-debug-cpu && make clean && make; } || { echo "Compile FAILED" >> ${results}; return 1; }
    { ${script_dir}/gf_methods 32 -ACD -L | grep '#' >> ${results}; } || { echo "gf_methods $i FAILED" >> ${results}; ((++failed)); }

    return ${failed}
}

compile_arm() {
    failed=0

    echo -n "Compiling with NO SIMD support..." >> ${results}
    { ./configure --disable-neon && make clean && make && echo "SUCCESS" >> ${results}; } || { echo "FAIL" >> ${results}; ((++failed)); }

    echo -n "Compiling with FULL SIMD support..." >> ${results}
    { ./configure && make clean && make && echo "SUCCESS" >> ${results}; } || { echo "FAIL" >> ${results}; ((++failed)); }

    return ${failed}
}

compile_intel() {
    failed=0

    echo -n "Compiling with NO SIMD support..." >> ${results}
    { ./configure && make clean && make && echo "SUCCESS" >> ${results}; } || { echo "FAIL" >> ${results}; ((++failed)); }

    echo -n "Compiling with SSE2 only..." >> ${results}
    export ax_cv_have_sse_ext=no 
    export ax_cv_have_sse2_ext=yes
    export ax_cv_have_sse3_ext=no
    export ax_cv_have_ssse3_ext=no
    export ax_cv_have_sse41_ext=no
    export ax_cv_have_sse42_ext=no
    export ax_cv_have_pclmuldq_ext=no
    { ./configure && make clean && make && echo "SUCCESS" >> ${results}; } || { echo "FAIL" >> ${results}; ((++failed)); }

    echo -n "Compiling with SSE2,SSE3 only..." >> ${results}
    export ax_cv_have_sse_ext=no 
    export ax_cv_have_sse2_ext=yes
    export ax_cv_have_sse3_ext=yes
    export ax_cv_have_ssse3_ext=no
    export ax_cv_have_sse41_ext=no
    export ax_cv_have_sse42_ext=no
    export ax_cv_have_pclmuldq_ext=no
    { ./configure && make clean && make && echo "SUCCESS" >> ${results}; } || { echo "FAIL" >> ${results}; ((++failed)); }

    echo -n "Compiling with SSE2,SSE3,SSSE3 only..." >> ${results}
    export ax_cv_have_sse_ext=no 
    export ax_cv_have_sse2_ext=yes
    export ax_cv_have_sse3_ext=yes
    export ax_cv_have_ssse3_ext=yes
    export ax_cv_have_sse41_ext=no
    export ax_cv_have_sse42_ext=no
    export ax_cv_have_pclmuldq_ext=no
    { ./configure && make clean && make && echo "SUCCESS" >> ${results}; } || { echo "FAIL" >> ${results}; ((++failed)); }

    echo -n "Compiling with SSE2,SSE3,SSSE3,SSE4_1 only..." >> ${results}
    export ax_cv_have_sse_ext=no 
    export ax_cv_have_sse2_ext=yes
    export ax_cv_have_sse3_ext=yes
    export ax_cv_have_ssse3_ext=yes
    export ax_cv_have_sse41_ext=yes
    export ax_cv_have_sse42_ext=no
    export ax_cv_have_pclmuldq_ext=no
    { ./configure && make clean && make && echo "SUCCESS" >> ${results}; } || { echo "FAIL" >> ${results}; ((++failed)); }

    echo -n "Compiling with SSE2,SSE3,SSSE3,SSE4_2 only..." >> ${results}
    export ax_cv_have_sse_ext=no 
    export ax_cv_have_sse2_ext=yes
    export ax_cv_have_sse3_ext=yes
    export ax_cv_have_ssse3_ext=yes
    export ax_cv_have_sse41_ext=no
    export ax_cv_have_sse42_ext=yes
    export ax_cv_have_pclmuldq_ext=no
    { ./configure && make clean && make && echo "SUCCESS" >> ${results}; } || { echo "FAIL" >> ${results}; ((++failed)); }

    echo -n "Compiling with FULL SIMD support..." >> ${results}
    export ax_cv_have_sse_ext=no 
    export ax_cv_have_sse2_ext=yes
    export ax_cv_have_sse3_ext=yes
    export ax_cv_have_ssse3_ext=yes
    export ax_cv_have_sse41_ext=yes
    export ax_cv_have_sse42_ext=yes
    export ax_cv_have_pclmuldq_ext=yes
    { ./configure && make clean && make && echo "SUCCESS" >> ${results}; } || { echo "FAIL" >> ${results}; ((++failed)); }

    return ${failed}
}

# test that we can compile the source code with different
# SIMD options. We assume that we are running on processor 
# full SIMD support
test_compile() {
    case $host_cpu in
        aarch64*|arm*) compile_arm ;;
        i[[3456]]86*|x86_64*|amd64*) compile_intel ;;
    esac
}

# disable through build flags
runtime_arm_flags() {
    failed=0

    echo "====NO SIMD support..." >> ${1}
    { ./configure --disable-neon --enable-debug-func && make clean && make; } || { echo "Compile FAILED" >> ${1}; return 1; }
    for i in 128 64 32 16 8 4; do
        { ${script_dir}/gf_methods $i -ACD -X >> ${1}; } || { echo "gf_methods $i FAILED" >> ${1}; ((++failed)); }
    done

    echo "====FULL SIMD support..." >> ${1}
    { ./configure --enable-debug-func && make clean && make; } || { echo "Compile FAILED" >> ${1}; return 1; }
    for i in 128 64 32 16 8 4; do
        { ${script_dir}/gf_methods $i -ACD -X >> ${1}; } || { echo "gf_methods $i FAILED" >> ${1}; ((++failed)); }
    done

    return ${failed}
}

# build once with FULL SIMD and disable at runtime through environment
runtime_arm_env() {
    failed=0

    { ./configure --enable-debug-func && make clean && make; } || { echo "Compile FAILED" >> ${1}; return 1; }

    echo "====NO SIMD support..." >> ${1}
    export GF_COMPLETE_DISABLE_NEON=1
    for i in 128 64 32 16 8 4; do
        { ${script_dir}/gf_methods $i -ACD -X >> ${1}; } || { echo "gf_methods $i FAILED" >> ${1}; ((++failed)); }
    done

    echo "====FULL SIMD support..." >> ${1}
    unset GF_COMPLETE_DISABLE_NEON
    for i in 128 64 32 16 8 4; do
        { ${script_dir}/gf_methods $i -ACD -X >> ${1}; } || { echo "gf_methods $i FAILED" >> ${1}; ((++failed)); }
    done

    return ${failed}
}

runtime_intel_flags() {
    failed=0

    echo "====NO SIMD support..." >> ${1}
    { ./configure --disable-sse --enable-debug-func && make clean && make; } || { echo "FAIL" >> ${1}; ((++failed)); }
    for i in 128 64 32 16 8 4; do
        { ${script_dir}/gf_methods $i -ACD -X >> ${1}; } || { echo "gf_methods $i FAILED" >> ${1}; ((++failed)); }
    done

   echo "====SSE2 support..." >> ${1}
    export ax_cv_have_sse_ext=no 
    export ax_cv_have_sse2_ext=yes
    export ax_cv_have_sse3_ext=no
    export ax_cv_have_ssse3_ext=no
    export ax_cv_have_sse41_ext=no
    export ax_cv_have_sse42_ext=no
    export ax_cv_have_pclmuldq_ext=no
    { ./configure --enable-debug-func && make clean && make; } || { echo "FAIL" >> ${1}; ((++failed)); }
    for i in 128 64 32 16 8 4; do
        { ${script_dir}/gf_methods $i -ACD -X >> ${1}; } || { echo "gf_methods $i FAILED" >> ${1}; ((++failed)); }
    done

    echo "====SSE2,SSE3 support..." >> ${1}
    export ax_cv_have_sse_ext=no 
    export ax_cv_have_sse2_ext=yes
    export ax_cv_have_sse3_ext=yes
    export ax_cv_have_ssse3_ext=no
    export ax_cv_have_sse41_ext=no
    export ax_cv_have_sse42_ext=no
    export ax_cv_have_pclmuldq_ext=no
    { ./configure --enable-debug-func && make clean && make; } || { echo "FAIL" >> ${1}; ((++failed)); }
    for i in 128 64 32 16 8 4; do
        { ${script_dir}/gf_methods $i -ACD -X >> ${1}; } || { echo "gf_methods $i FAILED" >> ${1}; ((++failed)); }
    done

    echo "====SSE2,SSE3,SSSE3 support..." >> ${1}
    export ax_cv_have_sse_ext=no 
    export ax_cv_have_sse2_ext=yes
    export ax_cv_have_sse3_ext=yes
    export ax_cv_have_ssse3_ext=yes
    export ax_cv_have_sse41_ext=no
    export ax_cv_have_sse42_ext=no
    export ax_cv_have_pclmuldq_ext=no
    { ./configure --enable-debug-func && make clean && make; } || { echo "FAIL" >> ${1}; ((++failed)); }
    for i in 128 64 32 16 8 4; do
        { ${script_dir}/gf_methods $i -ACD -X >> ${1}; } || { echo "gf_methods $i FAILED" >> ${1}; ((++failed)); }
    done

    echo "====SSE2,SSE3,SSSE3,SSE4_1 support..." >> ${1}
    export ax_cv_have_sse_ext=no 
    export ax_cv_have_sse2_ext=yes
    export ax_cv_have_sse3_ext=yes
    export ax_cv_have_ssse3_ext=yes
    export ax_cv_have_sse41_ext=yes
    export ax_cv_have_sse42_ext=no
    export ax_cv_have_pclmuldq_ext=no
    { ./configure --enable-debug-func && make clean && make; } || { echo "FAIL" >> ${1}; ((++failed)); }
    for i in 128 64 32 16 8 4; do
        { ${script_dir}/gf_methods $i -ACD -X >> ${1}; } || { echo "gf_methods $i FAILED" >> ${1}; ((++failed)); }
    done

    echo "====SSE2,SSE3,SSSE3,SSE4_2 support..." >> ${1}
    export ax_cv_have_sse_ext=no 
    export ax_cv_have_sse2_ext=yes
    export ax_cv_have_sse3_ext=yes
    export ax_cv_have_ssse3_ext=yes
    export ax_cv_have_sse41_ext=no
    export ax_cv_have_sse42_ext=yes
    export ax_cv_have_pclmuldq_ext=no
    { ./configure --enable-debug-func && make clean && make; } || { echo "FAIL" >> ${1}; ((++failed)); }
    for i in 128 64 32 16 8 4; do
        { ${script_dir}/gf_methods $i -ACD -X >> ${1}; } || { echo "gf_methods $i FAILED" >> ${1}; ((++failed)); }
    done

    echo "====FULL SIMD support..." >> ${1}
    { ./configure --enable-debug-func && make clean && make; } || { echo "FAIL" >> ${1}; ((++failed)); }
    for i in 128 64 32 16 8 4; do
        { ${script_dir}/gf_methods $i -ACD -X >> ${1}; } || { echo "gf_methods $i FAILED" >> ${1}; ((++failed)); }
    done

    return ${failed}
}

runtime_intel_env() {
    failed=0

    # compile a build with full SIMD support
    { ./configure --enable-debug-func && make clean && make; } || { echo "Compile FAILED" >> ${1}; return 1; }

    echo "====NO SIMD support..." >> ${1}
    export GF_COMPLETE_DISABLE_SSE2=1
    export GF_COMPLETE_DISABLE_SSE3=1
    export GF_COMPLETE_DISABLE_SSSE3=1
    export GF_COMPLETE_DISABLE_SSE4=1
    export GF_COMPLETE_DISABLE_SSE4_PCLMUL=1
    for i in 128 64 32 16 8 4; do
        { ${script_dir}/gf_methods $i -ACD -X >> ${1}; } || { echo "gf_methods $i FAILED" >> ${1}; ((++failed)); }
    done

    echo "====SSE2 support..." >> ${1}
    unset GF_COMPLETE_DISABLE_SSE2
    export GF_COMPLETE_DISABLE_SSE3=1
    export GF_COMPLETE_DISABLE_SSSE3=1
    export GF_COMPLETE_DISABLE_SSE4=1
    export GF_COMPLETE_DISABLE_SSE4_PCLMUL=1
    for i in 128 64 32 16 8 4; do
        { ${script_dir}/gf_methods $i -ACD -X >> ${1}; } || { echo "gf_methods $i FAILED" >> ${1}; ((++failed)); }
    done

    echo "====SSE2,SSE3 support..." >> ${1}
    unset GF_COMPLETE_DISABLE_SSE2
    unset GF_COMPLETE_DISABLE_SSE3
    export GF_COMPLETE_DISABLE_SSSE3=1
    export GF_COMPLETE_DISABLE_SSE4=1
    export GF_COMPLETE_DISABLE_SSE4_PCLMUL=1
    for i in 128 64 32 16 8 4; do
        { ${script_dir}/gf_methods $i -ACD -X >> ${1}; } || { echo "gf_methods $i FAILED" >> ${1}; ((++failed)); }
    done

    echo "====SSE2,SSE3,SSSE3 support..." >> ${1}
    unset GF_COMPLETE_DISABLE_SSE2
    unset GF_COMPLETE_DISABLE_SSE3
    unset GF_COMPLETE_DISABLE_SSSE3
    export GF_COMPLETE_DISABLE_SSE4=1
    export GF_COMPLETE_DISABLE_SSE4_PCLMUL=1
    for i in 128 64 32 16 8 4; do
        { ${script_dir}/gf_methods $i -ACD -X >> ${1}; } || { echo "gf_methods $i FAILED" >> ${1}; ((++failed)); }
    done

    echo "====SSE2,SSE3,SSSE3,SSE4_1 support..." >> ${1}
    unset GF_COMPLETE_DISABLE_SSE2
    unset GF_COMPLETE_DISABLE_SSE3
    unset GF_COMPLETE_DISABLE_SSSE3
    unset GF_COMPLETE_DISABLE_SSE4
    export GF_COMPLETE_DISABLE_SSE4_PCLMUL=1
    for i in 128 64 32 16 8 4; do
        { ${script_dir}/gf_methods $i -ACD -X >> ${1}; } || { echo "gf_methods $i FAILED" >> ${1}; ((++failed)); }
    done

    echo "====SSE2,SSE3,SSSE3,SSE4_2 support..." >> ${1}
    unset GF_COMPLETE_DISABLE_SSE2
    unset GF_COMPLETE_DISABLE_SSE3
    unset GF_COMPLETE_DISABLE_SSSE3
    unset GF_COMPLETE_DISABLE_SSE4
    export GF_COMPLETE_DISABLE_SSE4_PCLMUL=1
    for i in 128 64 32 16 8 4; do
        { ${script_dir}/gf_methods $i -ACD -X >> ${1}; } || { echo "gf_methods $i FAILED" >> ${1}; ((++failed)); }
    done

    echo "====FULL SIMD support..." >> ${1}
    unset GF_COMPLETE_DISABLE_SSE2
    unset GF_COMPLETE_DISABLE_SSE3
    unset GF_COMPLETE_DISABLE_SSSE3
    unset GF_COMPLETE_DISABLE_SSE4
    unset GF_COMPLETE_DISABLE_SSE4_PCLMUL
    for i in 128 64 32 16 8 4; do
        { ${script_dir}/gf_methods $i -ACD -X >> ${1}; } || { echo "gf_methods $i FAILED" >> ${1}; ((++failed)); }
    done

    return ${failed}
}

test_runtime() {
    rm -f ${results}.left
    rm -f ${results}.right
    
    case $host_cpu in
        aarch64*|arm*) 
            runtime_arm_flags ${results}.left
            runtime_arm_env ${results}.right
            ;;
        i[[3456]]86*|x86_64*|amd64*)
            runtime_intel_flags ${results}.left
            runtime_intel_env ${results}.right
            ;;
    esac

    echo "======LEFT======" > ${results}
    cat ${results}.left >> ${results}
    echo "======RIGHT======" >> ${results}
    cat ${results}.right >> ${results}
    echo "======RESULT======" >> ${results}
    if diff "${results}.left" "${results}.right"; then
        echo SUCCESS >> ${results}
        return 0
    else
        echo SUCCESS >> ${results}
        return 1
    fi
}

cd ${script_dir}/..
rm -f ${results}

test_$1
exit $?
