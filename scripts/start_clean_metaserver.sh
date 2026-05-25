#!/usr/bin/env bash
set -euo pipefail

cd /work/bigo-qfs

META_BIN="bld/output/bin/metaserver"
META_CONF="qfsbase/meta/conf/MetaServer.prp"
META_LOG="qfsbase/meta/MetaServer.log"
META_OUT="qfsbase/meta/MetaServer.out"
META_BASE="qfsbase/meta"
TS="$(date +%Y%m%d_%H%M%S)"

stop_pid_file() {
  local pid_file="$1"
  if [ -f "${pid_file}" ]; then
    local pid
    pid="$(cat "${pid_file}" || true)"
    if [ -n "${pid}" ] && kill -0 "${pid}" 2>/dev/null; then
      kill "${pid}" || true
    fi
    rm -f "${pid_file}"
  fi
}

echo "Stopping existing metaserver, if any..."
stop_pid_file "${META_BASE}/metaserver.pid"
pkill -f "${META_BIN} ${META_CONF}" 2>/dev/null || true

echo "Stopping existing chunkservers, if any..."
for idx in 1 2 3; do
  stop_pid_file "qfsbase/chunk${idx}/chunkserver.pid"
done
pkill -f "bld/output/bin/chunkserver qfsbase/chunk" 2>/dev/null || true
sleep 1

echo "Archiving old meta logs/checkpoints..."
mkdir -p "${META_BASE}"
if [ -d "${META_BASE}/logs" ]; then
  mv "${META_BASE}/logs" "${META_BASE}/logs.bak.${TS}"
fi
if [ -d "${META_BASE}/checkpoints" ]; then
  mv "${META_BASE}/checkpoints" "${META_BASE}/checkpoints.bak.${TS}"
fi
mkdir -p "${META_BASE}/logs" "${META_BASE}/checkpoints"

echo "Archiving old chunkserver local state..."
for idx in 1 2 3; do
  CHUNK_BASE="qfsbase/chunk${idx}"
  for path in "${CHUNK_BASE}"/chunkdir*; do
    if [ -d "${path}" ]; then
      mv "${path}" "${path}.bak.${TS}"
      mkdir -p "${path}"
    fi
  done
done

echo "Creating clean filesystem..."
"${META_BIN}" \
  -c \
  "${META_CONF}" \
  "${META_LOG}" \
  > "${META_OUT}" 2>&1

echo "Starting metaserver..."
setsid -f "${META_BIN}" \
  "${META_CONF}" \
  "${META_LOG}" \
  >> "${META_OUT}" 2>&1

sleep 2

echo "Starting chunkservers..."
for idx in 1 2 3; do
  setsid -f bld/output/bin/chunkserver \
    "qfsbase/chunk${idx}/conf/ChunkServer.prp" \
    "qfsbase/chunk${idx}/ChunkServer.log" \
    > "qfsbase/chunk${idx}/ChunkServer.out" 2>&1
done

sleep 5

echo "Process:"
ps -ef | awk "/bld\/output\/bin\/metaserver|bld\/output\/bin\/chunkserver/ && !/awk/ {print}"

echo "Ping:"
bld/output/bin/tools/qfsping -m -s localhost -p 20000
