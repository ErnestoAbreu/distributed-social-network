#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PROTO_DIR="${ROOT_DIR}/proto"
OUT_DIR="${ROOT_DIR}/protos"

mkdir -p "${OUT_DIR}"

for proto_file in "${PROTO_DIR}"/*.proto; do
  python -m grpc_tools.protoc -I"${PROTO_DIR}" \
    --python_out="${OUT_DIR}" --grpc_python_out="${OUT_DIR}" \
    "$proto_file"
done

echo "Protos generated in ${OUT_DIR}"
