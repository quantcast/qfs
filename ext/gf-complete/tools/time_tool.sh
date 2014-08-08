# time_tool.sh - Shell script to test various timings.  
# This is a rough tester -- its job is to work quickly rather than precisely.
# (Jim Plank)

#!/bin/sh

if [ $# -lt 3 ]; then
  echo 'usage sh time_tool.sh M|D|R|B w method' >&2
  exit 1
fi

op=$1
w=$2

shift ; shift

method="$*"

if [ $op != M -a $op != D -a $op != R -a $op != B ]; then
  echo 'usage sh time_tool.sh M|D|R|B w method' >&2
  echo 'You have to specify a test: ' >&2 
  echo '  M=Multiplication' >&2 
  echo '  D=Division' >&2 
  echo '  R=Regions' >&2 
  echo '  B=Best-Region' >&2 
  exit 1
fi

# First, use a 16K buffer to test the performance of single multiplies.

fac=`echo $w | awk '{ n = $1; while (n != 0 && n%2==0) n /= 2; print n }'`
if [ $fac -eq 0 ]; then
  echo 'usage sh time_tool.sh M|D|R|B w method' >&2
  echo 'Bad w' >&2
  exit 1
fi

bsize=16384
bsize=`echo $bsize $fac | awk '{ print $1 * $2 }'`

if [ `./gf_time $w M -1 $bsize 1 $method 2>&1 | wc | awk '{ print $1 }'` -gt 2 ]; then
  echo 'usage sh time_tool.sh w method' >&2
  echo "Bad method"
  exit 1
fi

if [ $op = M -o $op = D ]; then
  iter=1
  c1=`./gf_time $w $op -1 $bsize $iter $method`
  t=`echo $c1 | awk '{ printf "%d\n", $4*100 }'`
  s=`echo $c1 | awk '{ print $8 }'`
  bs=$s
  
  while [ $t -lt 1 ]; do
    bs=$s
    iter=`echo $iter | awk '{ print $1*2 }'`
    c1=`./gf_time $w $op -1 $bsize $iter $method`
    t=`echo $c1 | awk '{ printf "%d\n", $4*100 }'`
    s=`echo $c1 | awk '{ print $8 }'`
  done
  
  echo $op $bs | awk '{ printf "%s speed (MB/s): %8.2lf   W-Method: ", $1, $2 }'
  echo $w $method 
  exit 0
fi
  
bsize=16384
bsize=`echo $bsize $fac | awk '{ print $1 * $2 }'`

best=0
while [ $bsize -le 4194304 ]; do
  iter=1
  c1=`./gf_time $w G -1 $bsize $iter $method`
  t=`echo $c1 | awk '{ printf "%d\n", $6*500 }'`
  s=`echo $c1 | awk '{ print $10 }'`
  bs=$s

  while [ $t -lt 1 ]; do
    bs=$s
    iter=`echo $iter | awk '{ print $1*2 }'`
    c1=`./gf_time $w G -1 $bsize $iter $method`
    t=`echo $c1 | awk '{ printf "%d\n", $6*500 }'`
    s=`echo $c1 | awk '{ print $10 }'`
  done
  if [ $bsize -lt 1048576 ]; then
    str=`echo $bsize | awk '{ printf "%3dK\n", $1/1024 }'`
  else 
    str=`echo $bsize | awk '{ printf "%3dM\n", $1/1024/1024 }'`
  fi
  if [ $op = R ]; then
    echo $str $bs | awk '{ printf "Region Buffer-Size: %4s (MB/s): %8.2lf   W-Method: ", $1, $2 }'
    echo $w $method 
  fi
  best=`echo $best $bs | awk '{ print ($1 > $2) ? $1 : $2 }'`
  bsize=`echo $bsize | awk '{ print $1 * 2 }'`
done
echo $best | awk '{ printf "Region Best (MB/s): %8.2lf   W-Method: ", $1 }'
echo $w $method 
