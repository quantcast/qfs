#
#
# Copyright (c) 2013, James S. Plank and Kevin Greenan
# All rights reserved.
#
# Jerasure - A C/C++ Library for a Variety of Reed-Solomon and RAID-6 Erasure
# Coding Techniques
#
# Revision 2.0: Galois Field backend now links to GF-Complete
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions
# are met:
#
#  - Redistributions of source code must retain the above copyright
#    notice, this list of conditions and the following disclaimer.
#
#  - Redistributions in binary form must reproduce the above copyright
#    notice, this list of conditions and the following disclaimer in
#    the documentation and/or other materials provided with the
#    distribution.
#
#  - Neither the name of the University of Tennessee nor the names of its
#    contributors may be used to endorse or promote products derived
#    from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT,
# INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING,
# BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS
# OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED
# AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT
# LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY
# WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
# POSSIBILITY OF SUCH DAMAGE.
#
GF_METHODS=${GF_COMPLETE_DIR:-/usr/local/bin}/gf_methods
k=12
m=3
seed=1370

# Test all w=8
${GF_METHODS} 8 -B -L | awk -F: '{ if ($1 == "w=8") print $2; }' |
while read method; do
  echo "Testing ${k} ${m} 8 $seed ${method}"
  $VALGRIND ./reed_sol_test_gf ${k} ${m} 8 $seed ${method} | tail -n 1
  if [[ $? != "0" ]]; then
    echo "Failed test for ${k} ${m} 8 $seed ${method}"
    exit 1
  fi
done

if [[ $? == "1" ]]; then
  exit 1
fi


# Test all w=16
${GF_METHODS} 16 -B -L | awk -F: '{ if ($1 == "w=16") print $2; }' |
while read method; do
  echo "Testing ${k} ${m} 16 $seed ${method}"
  $VALGRIND ./reed_sol_test_gf ${k} ${m} 16 $seed ${method} | tail -n 1
  if [[ $? != "0" ]]; then
    echo "Failed test for ${k} ${m} 16 $seed ${method}"
    exit 1
  fi
done


if [[ $? == "1" ]]; then
  exit 1
fi

# Test all w=32
${GF_METHODS} 32 -B -L | awk -F: '{ if ($1 == "w=32") print $2; }' |
while read method; do
  echo "Testing ${k} ${m} 32 $seed ${method}"
  $VALGRIND ./reed_sol_test_gf ${k} ${m} 32 $seed ${method} | tail -n 1
  if [[ $? != "0" ]]; then
    echo "Failed test for ${k} ${m} 32 $seed ${method}"
    exit 1
  fi
done


if [[ $? == "1" ]]; then
  exit 1
fi

echo "Passed all tests!"
