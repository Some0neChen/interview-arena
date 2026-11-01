#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"

usage() {
    printf '%s\n' \
        "Usage: bash test/script/submit_order_acceptance.sh [build-directory]" \
        "Builds and runs local SubmitOrder/CancelOrder persistence, idempotency, and four-table transaction tests." \
        "Requires local MySQL at 127.0.0.1:3306 (a/111111), accounts/orders/fund_ledger/outbox_events schema," \
        "TRADE_MYSQL_DATABASE selects the database for both server and runner (default: arena)." \
        "and a configured CMake build with Boost 1.88. Test builds also need nlohmann_json." \
        "Port 13401 must be free: the script starts and stops only its own trade_server." \
        "Disposable outbox events, ledger entries, orders, and accounts are deleted in that order; AUTO_INCREMENT is not rewound." \
        "Outbox checks cover local persistence only, not Kafka delivery or consumer processing." \
        "Cancellation faults preinsert conflicting ledger/event rows on disposable test orders only." \
        "Only the current single-connection pool is tested; this is not a performance benchmark." \
        "Reports, server logs, and a fixture cleanup manifest are retained under /tmp." \
        "Exit codes: 0 passed, 1 failed, 2 setup failed. No sudo or Docker permission needed."
}

if [[ "${1:-}" == "--help" || "${1:-}" == "-h" ]]; then
    usage
    exit 0
fi
if [[ $# -gt 1 ]]; then
    usage >&2
    exit 2
fi

BUILD_DIR="${1:-$ROOT_DIR/build}"
if [[ "$BUILD_DIR" != /* ]]; then
    BUILD_DIR="$PWD/$BUILD_DIR"
fi

port_open() {
    timeout 1 bash -c 'exec 3<>"/dev/tcp/127.0.0.1/$1"' _ "$1" 2>/dev/null
}

if [[ ! -f "$BUILD_DIR/CMakeCache.txt" ]]; then
    printf '[ERROR] Configure CMake first: %s\n' "$BUILD_DIR" >&2
    exit 2
fi
if ! port_open 3306; then
    printf '[ERROR] Start the local MySQL service on port 3306 first.\n' >&2
    exit 2
fi
if port_open 13401; then
    printf '[ERROR] Port 13401 is occupied; no existing server was stopped.\n' >&2
    exit 2
fi

cmake -S "$ROOT_DIR" -B "$BUILD_DIR" -DARENA_BUILD_ACCEPTANCE_TESTS=ON || exit 2
cmake --build "$BUILD_DIR" --target trade_server submit_order_acceptance -j2 || exit 2

OUTPUT_DIR="$(mktemp -d /tmp/trade_submit_acceptance.XXXXXX)"
SERVER_BIN="$BUILD_DIR/trade_server"
RUNNER_BIN="$BUILD_DIR/submit_order_acceptance"
SERVER_PID=""
RUNNER_PID=""

cleanup() {
    local status=$? server_status=0 shutdown_start=0 shutdown_ms=0
    trap - EXIT INT TERM
    if [[ -n "$RUNNER_PID" ]]; then
        kill -TERM "$RUNNER_PID" 2>/dev/null || true
        wait "$RUNNER_PID" 2>/dev/null || true
    fi
    if [[ -n "$SERVER_PID" ]]; then
        shutdown_start="$(date +%s%N)"
        kill -TERM "$SERVER_PID" 2>/dev/null || true
        for ((i = 0; i < 120; ++i)); do
            if ! kill -0 "$SERVER_PID" 2>/dev/null; then
                break
            fi
            sleep 0.05
        done
        if kill -0 "$SERVER_PID" 2>/dev/null; then
            kill -KILL "$SERVER_PID" 2>/dev/null || true
            server_status=1
        fi
        wait "$SERVER_PID" || server_status=$?
        shutdown_ms=$(( ($(date +%s%N) - shutdown_start) / 1000000 ))
        printf 'server_exit=%s\nshutdown_ms=%s\n' "$server_status" "$shutdown_ms" > "$OUTPUT_DIR/lifecycle.txt"
        if [[ "$server_status" -ne 0 ]]; then
            printf '[FAIL] Owned test server did not exit normally.\n' >&2
            status=1
        else
            printf '[PASS] Owned test server exited normally (%s ms).\n' "$shutdown_ms"
        fi
    fi
    if [[ -f "$OUTPUT_DIR/fixtures.json" ]]; then
        if ! "$RUNNER_BIN" --cleanup "$OUTPUT_DIR/fixtures.json"; then
            printf '[FAIL] Fixture cleanup incomplete; inspect %s/fixtures.json.\n' "$OUTPUT_DIR" >&2
            status=1
        fi
    fi
    printf '[INFO] Results and logs: %s\n' "$OUTPUT_DIR"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

# Recheck after building: another service may have started meanwhile.
if port_open 13401; then
    printf '[ERROR] Port 13401 became occupied; no existing process was touched.\n' >&2
    exit 2
fi
(
    cd "$OUTPUT_DIR"
    exec "$SERVER_BIN"
) > "$OUTPUT_DIR/server.log" 2>&1 &
SERVER_PID=$!

ready=0
for ((i = 0; i < 100; ++i)); do
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        break
    fi
    if port_open 13401; then
        ready=1
        break
    fi
    sleep 0.05
done
if [[ "$ready" -ne 1 ]]; then
    printf '[ERROR] Owned test server failed to start. See %s/server.log.\n' "$OUTPUT_DIR" >&2
    exit 2
fi

timeout --kill-after=5s 90s "$RUNNER_BIN" "$OUTPUT_DIR/results.json" &
RUNNER_PID=$!
status=0
wait "$RUNNER_PID" || status=$?
RUNNER_PID=""
exit "$status"
