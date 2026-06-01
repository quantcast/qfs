 pkill -f 'mstress_100k_1m_file'
  pkill -f 'mstress_client'
  pkill -f 'metaserver'
  pkill -f 'chunkserver'

# clean cluster

  cd /work/bigo-qfs
  TS=$(date +%Y%m%d_%H%M%S)

  rm -f qfsbase/meta/metaserver.pid \
        qfsbase/chunk1/chunkserver.pid \
        qfsbase/chunk2/chunkserver.pid \
        qfsbase/chunk3/chunkserver.pid

  mv qfsbase/meta/logs qfsbase/meta/logs.bak.$TS 2>/dev/null || true
  mv qfsbase/meta/checkpoints qfsbase/meta/checkpoints.bak.$TS 2>/dev/null || true
  mkdir -p qfsbase/meta/logs qfsbase/meta/checkpoints

  for p in \
    qfsbase/chunk1/chunkdir11 \
    qfsbase/chunk1/chunkdir12 \
    qfsbase/chunk2/chunkdir21 \
    qfsbase/chunk3/chunkdir31
  do
    mv "$p" "$p.bak.$TS" 2>/dev/null || true
    mkdir -p "$p"
  done

# start meta
  cd /work/bigo-qfs

  bld/output/bin/metaserver -c qfsbase/meta/conf/MetaServer.prp qfsbase/meta/MetaServer.log

  setsid -f bld/output/bin/metaserver \
    qfsbase/meta/conf/MetaServer.prp \
    qfsbase/meta/MetaServer.log \
    >> qfsbase/meta/MetaServer.out 2>&1

# start chunk
 cd /work/bigo-qfs

  setsid -f bld/output/bin/chunkserver qfsbase/chunk1/conf/ChunkServer.prp qfsbase/chunk1/ChunkServer.log > qfsbase/chunk1/ChunkServer.out 2>&1
  setsid -f bld/output/bin/chunkserver qfsbase/chunk2/conf/ChunkServer.prp qfsbase/chunk2/ChunkServer.log > qfsbase/chunk2/ChunkServer.out 2>&1
  setsid -f bld/output/bin/chunkserver qfsbase/chunk3/conf/ChunkServer.prp qfsbase/chunk3/ChunkServer.log > qfsbase/chunk3/ChunkServer.out 2>&1

sleep 6

bld/output/bin/tools/qfsping -m -s 202.168.115.34 -p 20000
