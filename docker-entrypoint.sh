#!/bin/sh
set -eu

if [ "$#" -gt 0 ]; then
  exec qday-index "$@"
fi

: "${RPC_URL:?RPC_URL is required}"

from_block="${FROM_BLOCK:-1}"
confirm="${CONFIRM:-0}"
batch="${BATCH:-1000}"
raw_dir="${RAW_DIR:-/data/raw_txs}"
concurrency="${CONCURRENCY:-8}"
rpc_batch="${RPC_BATCH:-20}"

mkdir -p "$raw_dir"

set -- \
  qday-index \
  --from-block "$from_block" \
  --batch "$batch" \
  --rpc-url "$RPC_URL" \
  --raw-dir "$raw_dir" \
  --concurrency "$concurrency" \
  --rpc-batch "$rpc_batch" \
  --confirm "$confirm"

if [ -n "${TO_BLOCK:-}" ]; then
  set -- "$@" --to-block "$TO_BLOCK"
fi

if [ "${RESUME:-false}" = "true" ]; then
  set -- "$@" --resume
fi

exec "$@"
