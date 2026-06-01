  cd /work/bigo-qfs

  python bld/benchmarks/mstress/mstress.py \
    -m slave \
    -f qfs \
    -s localhost \
    -p 20000 \
    -t create \
    -a output/mstress_100k_1m_file.plan \
    -c localhost \
    -k localhost
