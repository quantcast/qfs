#!/usr/bin/env bash
set -euo pipefail

cd /work/bigo-qfs

PLAN_FILE="${1:-output/mstress_1m_after_lock.plan}"
QFS="bld/output/bin/tools/qfs"
QFS_CFG="qfsbase/client/clidefault.prp"
MSTRESS="bld/benchmarks/mstress/mstress.py"
META_HOST="localhost"
META_PORT="20000"

if [ ! -f "${PLAN_FILE}" ]; then
  echo "Plan file not found: ${PLAN_FILE}" >&2
  exit 1
fi

echo "Checking metaserver..."
bld/output/bin/tools/qfsping -m -s "${META_HOST}" -p "${META_PORT}"

echo "Cleaning /mstress..."
"${QFS}" \
  -D dfs.force.remove=true \
  -cfg "${QFS_CFG}" \
  -rmr /mstress >/dev/null 2>&1 || true

echo "Running create benchmark with plan: ${PLAN_FILE}"
python "${MSTRESS}" \
  -m slave \
  -f qfs \
  -s "${META_HOST}" \
  -p "${META_PORT}" \
  -t create \
  -a "${PLAN_FILE}" \
  -c localhost \
  -k localhost

echo "Summary:"
rg -n "paths created|failed|ERROR|FATAL" "${PLAN_FILE}"* 2>/dev/null || true
