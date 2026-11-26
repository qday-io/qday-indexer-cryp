#!/bin/sh
set -eu

if [ "$#" -gt 0 ]; then
  exec qday-index "$@"
fi

: "${FROM_BLOCK:?FROM_BLOCK is required}"
: "${TO_BLOCK:?TO_BLOCK is required}"
: "${RPC_URL:?RPC_URL is required}"

config="${CONFIG:-/config/address.txt}"
batch="${BATCH:-1000}"
raw_dir="${RAW_DIR:-/data/raw_txs}"
output="${OUTPUT:-/data/out.csv}"
concurrency="${CONCURRENCY:-8}"
rpc_batch="${RPC_BATCH:-20}"

mkdir -p "$raw_dir" "$(dirname "$output")"

set -- \
  qday-index \
  --config "$config" \
  --from-block "$FROM_BLOCK" \
  --to-block "$TO_BLOCK" \
  --batch "$batch" \
  --rpc-url "$RPC_URL" \
  --raw-dir "$raw_dir" \
  --output "$output" \
  --concurrency "$concurrency" \
  --rpc-batch "$rpc_batch"

if [ "${EXCLUDE_SEEDS_FROM_HOP2:-false}" = "true" ]; then
  set -- "$@" --exclude-seeds-from-hop2
fi

if [ "${RESUME:-false}" = "true" ]; then
  set -- "$@" --resume
fi

exec "$@"
