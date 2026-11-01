#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
BUILD_DIR="${BROKER_BUILD_DIR:-$ROOT_DIR/build}"

usage() {
    printf '%s\n' \
        'Usage: bash test/script/broker_submit_smoke.sh endpoint gateway_namespace gateway_order_id account_id symbol limit_price_cent quantity' \
        '       bash test/script/broker_submit_smoke.sh endpoint get|cancel gateway_namespace gateway_order_id' \
        '       bash test/script/broker_submit_smoke.sh --build-only' \
        'Builds a small RPC client without changing project CMakeLists.txt.' \
        'Requests may persist orders. Use an isolated broker test database.' \
        'This script does not start services, seed data, or clean databases.' \
        'RPC OK may still mean business REJECTED: inspect state_name.' \
        'Run the generated build/broker_submit_smoke directly for repeated requests.'
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    usage
    exit 0
fi
if [[ $# -ne 7 && $# -ne 4 && ! ( $# -eq 1 && "$1" == "--build-only" ) ]]; then
    usage >&2
    exit 2
fi
if [[ ! -f "$BUILD_DIR/CMakeCache.txt" ]]; then
    printf 'Configure the build directory first: %s\n' "$BUILD_DIR" >&2
    exit 2
fi
pkg-config --exists grpc++ protobuf
cmake --build "$BUILD_DIR" --target trade_broker_proto -j2 >&2
read -r -a grpc_flags <<< "$(pkg-config --cflags --libs grpc++ protobuf)"
"${CXX:-c++}" -std=c++20 -g -pthread \
    -I"$BUILD_DIR/generated" \
    "$ROOT_DIR/test/broker_submit_smoke.cpp" \
    "$BUILD_DIR/libtrade_broker_proto.a" \
    "${grpc_flags[@]}" \
    -o "$BUILD_DIR/broker_submit_smoke"

if [[ "${1:-}" == "--build-only" ]]; then
    printf 'Built: %s/broker_submit_smoke\n' "$BUILD_DIR"
    exit 0
fi
exec "$BUILD_DIR/broker_submit_smoke" "$@"
