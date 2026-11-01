#!/usr/bin/env bash

set -euo pipefail

ROOT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)"
REMAINING=0
SERVER_BUDGET=0
if [[ "${1:-}" == "--remaining" ]]; then
    REMAINING=1
    shift
fi
if [[ "$REMAINING" -eq 1 && "${1:-}" == "--server-budget" ]]; then
    SERVER_BUDGET=1
    shift
fi

usage() {
    if [[ "$REMAINING" -eq 1 ]]; then
        printf '%s\n' \
            "Usage: bash test/script/m1_remaining_acceptance.sh [--server-budget] [build-directory]" \
            "M1.6 exact COMMIT faults; M1.7 deadline/shutdown checks; M1.8 pool-size 2/4 correctness." \
            "Reuses production Service/Reactor/Repository in a test-only startup executable." \
            "Requires MySQL:3306 and free loopback ports 13401, 13307, 13308; Python 3 is required." \
            "Creates and cleans an owned disposable database; original arena gets read-only snapshots." \
            "The MySQL proxy supports only small classic-protocol test traffic; TLS is disabled only in this harness." \
            "--server-budget tests the chosen server-owned 3s policy, including explicit TryCancel." \
            "Without the flag, preserves the historical client-deadline checks, not the chosen write policy." \
            "See test/script/README.md for admin access, output files, and policy boundaries."
        return
    fi
    printf '%s\n' \
        "Usage: bash test/script/submit_order_write_fault_acceptance.sh [build-directory]" \
        "M1.6-M1.8: bash test/script/m1_remaining_acceptance.sh [build-directory]" \
        "M1.5: creates an owned disposable MySQL database, runs the 71-check regression," \
        "then directly fails SubmitOrder ledger/outbox INSERTs using test-only CHECK constraints." \
        "Requires local MySQL:3306, free port 13401, and a configured Boost 1.88 build." \
        "Admin access: root TCP first; otherwise Docker Compose, with sudo -n if needed." \
        "If needed, run sudo -v yourself first; the script never prompts for a sudo password." \
        "Optional: TRADE_TEST_MYSQL_ADMIN_USER / TRADE_TEST_MYSQL_ADMIN_PASSWORD (local dev defaults)." \
        "The server and runner both use TRADE_MYSQL_DATABASE, set by this script to its new database." \
        "Only its temporary database grant is revoked, then its database is dropped." \
        "The original arena tables are read-only snapshots before/after, never altered by this script." \
        "Reports and logs remain under /tmp/trade_submit_write_fault.*; exit 0 pass, 1 fail, 2 setup error."
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
    printf '[ERROR] Start MySQL on port 3306 first.\n' >&2
    exit 2
fi
if port_open 13401; then
    printf '[ERROR] Port 13401 is occupied; no existing server was stopped.\n' >&2
    exit 2
fi

cmake -S "$ROOT_DIR" -B "$BUILD_DIR" -DARENA_BUILD_ACCEPTANCE_TESTS=ON || exit 2
cmake --build "$BUILD_DIR" --target trade_server submit_order_acceptance -j2 || exit 2
if [[ "$REMAINING" -eq 1 ]]; then
    command -v python3 >/dev/null || exit 2
    if port_open 13307 || port_open 13308; then
        printf '[ERROR] Fault proxy ports 13307/13308 must be free.\n' >&2
        exit 2
    fi
    cmake --build "$BUILD_DIR" --target trade_fault_server -j2 || exit 2
fi

if [[ "$SERVER_BUDGET" -eq 1 ]]; then
    OUTPUT_DIR="$(mktemp -d /tmp/trade_m1_server_budget.XXXXXX)"
elif [[ "$REMAINING" -eq 1 ]]; then
    OUTPUT_DIR="$(mktemp -d /tmp/trade_m1_remaining.XXXXXX)"
else
    OUTPUT_DIR="$(mktemp -d /tmp/trade_submit_write_fault.XXXXXX)"
fi
suffix="${OUTPUT_DIR##*.}"
# No '_' or '%' in a database name: database-level GRANT patterns must be exact.
TEST_DB="tradewritefault$(date +%s)${BASHPID}${suffix,,}"
[[ "$TEST_DB" =~ ^tradewritefault[a-z0-9]+$ && ${#TEST_DB} -le 64 ]] || exit 2
SERVER_BIN="$BUILD_DIR/trade_server"
RUNNER_BIN="$BUILD_DIR/submit_order_acceptance"
SERVER_PID=""
RUNNER_PID=""
PROXY_PID=""
CURRENT_PHASE=""
ADMIN_MODE=""
DOCKER=()
DB_CREATED=0
DB_GRANTED=0
BASELINE_SAVED=0
ADMIN_USER="${TRADE_TEST_MYSQL_ADMIN_USER:-root}"
ADMIN_PASSWORD="${TRADE_TEST_MYSQL_ADMIN_PASSWORD:-root_dev_only}"

admin_sql() {
    local database="${1:-}"
    if [[ "$ADMIN_MODE" == "tcp" ]]; then
        TRADE_TEST_MYSQL_ADMIN_USER="$ADMIN_USER" TRADE_TEST_MYSQL_ADMIN_PASSWORD="$ADMIN_PASSWORD" \
            timeout 10 "$RUNNER_BIN" --admin-sql "$database"
    else
        local database_option=()
        if [[ -n "$database" ]]; then database_option=("--database=$database"); fi
        # sudo may strip environment variables. Pass the password on stdin, not in argv.
        {
            printf '%s\n' "$ADMIN_PASSWORD"
            cat
        } | timeout 10 "${DOCKER[@]}" compose -f "$ROOT_DIR/docker/compose.yaml" \
            exec -T mysql sh -c '
                IFS= read -r MYSQL_PWD || exit 2
                export MYSQL_PWD
                user="$1"
                shift
                exec mysql --user="$user" --batch --skip-column-names --default-character-set=utf8mb4 "$@"
            ' sh "$ADMIN_USER" "${database_option[@]}"
    fi
}

stop_server() {
    local server_status=0 shutdown_start=0 shutdown_ms=0
    if [[ -n "$SERVER_PID" ]]; then
        shutdown_start="$(date +%s%N)"
        kill -TERM "$SERVER_PID" 2>/dev/null
        for ((i = 0; i < 120; ++i)); do
            if ! kill -0 "$SERVER_PID" 2>/dev/null; then break; fi
            sleep 0.05
        done
        if kill -0 "$SERVER_PID" 2>/dev/null; then
            kill -KILL "$SERVER_PID" 2>/dev/null
            server_status=1
        fi
        wait "$SERVER_PID" || server_status=$?
        shutdown_ms=$(( ($(date +%s%N) - shutdown_start) / 1000000 ))
        printf 'server_exit=%s\nshutdown_ms=%s\n' "$server_status" "$shutdown_ms" > "$OUTPUT_DIR/lifecycle.txt"
        if [[ -n "$CURRENT_PHASE" ]]; then
            cp "$OUTPUT_DIR/lifecycle.txt" "$OUTPUT_DIR/$CURRENT_PHASE-lifecycle.txt"
        fi
        SERVER_PID=""
        if [[ "$server_status" -ne 0 ]]; then
            printf '[FAIL] Owned test server did not exit normally.\n' >&2
            return 1
        else
            printf '[PASS] Owned test server exited normally (%s ms).\n' "$shutdown_ms"
        fi
    fi
    return 0
}

cleanup() {
    local status=$?
    trap - EXIT INT TERM
    set +e
    if [[ -n "$RUNNER_PID" ]]; then
        kill -TERM "$RUNNER_PID" 2>/dev/null
        wait "$RUNNER_PID" 2>/dev/null
    fi
    stop_server || status=1
    if [[ -n "$PROXY_PID" ]]; then
        kill -TERM "$PROXY_PID" 2>/dev/null
        wait "$PROXY_PID" 2>/dev/null
    fi
    if [[ "$DB_GRANTED" -eq 1 ]]; then
        if printf "REVOKE ALL PRIVILEGES ON \`%s\`.* FROM 'a'@'%%';\n" "$TEST_DB" |
            admin_sql >> "$OUTPUT_DIR/admin-cleanup.log" 2>&1; then
            printf '[PASS] Revoked only the temporary database grant.\n'
        else
            printf '[FAIL] Could not revoke grant for %s; inspect admin-cleanup.log.\n' "$TEST_DB" >&2
            status=1
        fi
    fi
    if [[ "$DB_CREATED" -eq 1 ]]; then
        if printf 'DROP DATABASE `%s`;\n' "$TEST_DB" | admin_sql >> "$OUTPUT_DIR/admin-cleanup.log" 2>&1; then
            printf '[PASS] Dropped owned disposable database: %s\n' "$TEST_DB"
        else
            printf '[FAIL] Disposable database retained: %s; inspect admin-cleanup.log.\n' "$TEST_DB" >&2
            status=1
        fi
    fi
    if [[ "$BASELINE_SAVED" -eq 1 ]]; then
        if TRADE_MYSQL_DATABASE=arena timeout 10 "$RUNNER_BIN" --snapshot "$OUTPUT_DIR/arena-after.json" &&
            cmp -s "$OUTPUT_DIR/arena-before.json" "$OUTPUT_DIR/arena-after.json"; then
            printf '[PASS] All original arena rows are unchanged.\n'
        else
            printf '[FAIL] Original arena snapshot changed or could not be read; inspect before/after reports.\n' >&2
            status=1
        fi
    fi
    printf '[INFO] Results and logs: %s\n' "$OUTPUT_DIR"
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

if printf 'SELECT CURRENT_USER();\n' |
    timeout 10 "$RUNNER_BIN" --admin-sql > "$OUTPUT_DIR/admin-preflight.log" 2>&1; then
    ADMIN_MODE=tcp
elif command -v docker >/dev/null && timeout 5 docker info >/dev/null 2>&1; then
    ADMIN_MODE=docker
    DOCKER=(docker)
elif command -v sudo >/dev/null && timeout 5 sudo -n docker info >/dev/null 2>&1; then
    ADMIN_MODE=docker
    DOCKER=(sudo -n docker)
else
    printf '[ERROR] No MySQL admin TCP access or Docker permission. No database was created.\n' >&2
    printf '[NEXT] Run sudo -v in your terminal, then rerun this script as your normal user.\n' >&2
    exit 2
fi
printf '[INFO] Administrative route: %s\n' "$ADMIN_MODE"
app_account_count="$(printf "SELECT COUNT(*) FROM mysql.user WHERE User = 'a' AND Host = '%%';\n" |
    admin_sql 2>> "$OUTPUT_DIR/admin-preflight.log")" || exit 2
if [[ "$app_account_count" != "1" ]]; then
    printf "[ERROR] Expected existing MySQL application account 'a'@'%%'; no database created.\n" >&2
    exit 2
fi

TRADE_MYSQL_DATABASE=arena timeout 10 "$RUNNER_BIN" --snapshot "$OUTPUT_DIR/arena-before.json" || exit 2
BASELINE_SAVED=1
printf 'CREATE DATABASE `%s` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_bin;\n' "$TEST_DB" |
    admin_sql > "$OUTPUT_DIR/admin-setup.log" 2>&1 || exit 2
DB_CREATED=1
printf '{"database":"%s","admin_mode":"%s"}\n' "$TEST_DB" "$ADMIN_MODE" > "$OUTPUT_DIR/database.json"
for schema in accounts orders fund_ledger outbox_events; do
    admin_sql "$TEST_DB" < "$ROOT_DIR/sql/trade_${schema}_schema.sql" >> "$OUTPUT_DIR/admin-setup.log" 2>&1 || exit 2
done
printf "GRANT ALL PRIVILEGES ON \`%s\`.* TO 'a'@'%%';\n" "$TEST_DB" |
    admin_sql >> "$OUTPUT_DIR/admin-setup.log" 2>&1 || exit 2
DB_GRANTED=1
export TRADE_MYSQL_DATABASE="$TEST_DB"

run_acceptance() {
    local status=0
    timeout --kill-after=5s 90s "$RUNNER_BIN" "$@" &
    RUNNER_PID=$!
    wait "$RUNNER_PID" || status=$?
    RUNNER_PID=""
    return "$status"
}

if [[ "$REMAINING" -eq 1 ]]; then
    python3 "$ROOT_DIR/test/script/mysql_commit_fault_proxy.py" --events "$OUTPUT_DIR/proxy-events.jsonl" \
        > "$OUTPUT_DIR/proxy.log" 2>&1 &
    PROXY_PID=$!
    proxy_ready=0
    for ((i = 0; i < 100; ++i)); do
        if ! kill -0 "$PROXY_PID" 2>/dev/null; then break; fi
        if port_open 13308; then proxy_ready=1; break; fi
        sleep 0.05
    done
    [[ "$proxy_ready" -eq 1 ]] || exit 2

    start_test_server() {
        local pool_size="$1" mysql_port="$2"
        export TRADE_TEST_POOL_SIZE="$pool_size"
        if port_open 13401; then
            printf '[ERROR] Port 13401 became occupied; no existing process was touched.\n' >&2
            return 2
        fi
        (
            cd "$OUTPUT_DIR"
            exec "$BUILD_DIR/trade_fault_server" "$pool_size" "$mysql_port"
        ) > "$OUTPUT_DIR/$CURRENT_PHASE-server.log" 2>&1 &
        SERVER_PID=$!
        for ((i = 0; i < 100; ++i)); do
            if ! kill -0 "$SERVER_PID" 2>/dev/null; then break; fi
            if port_open 13401; then return 0; fi
            sleep 0.05
        done
        return 2
    }

    overall=0
    CURRENT_PHASE=boundaries
    start_test_server 1 13307 || exit 2
    run_acceptance --verify-database-target || exit 2
    run_acceptance --boundaries commit "$OUTPUT_DIR/commit-results.json" 1 || overall=1
    if [[ "$SERVER_BUDGET" -eq 1 ]]; then
        stop_server || overall=1
        CURRENT_PHASE=server-budget
        start_test_server 1 3306 || exit 2
        run_acceptance --verify-database-target || exit 2
        run_acceptance --boundaries server-budget "$OUTPUT_DIR/server-budget-results.json" 1 || overall=1
        stop_server || overall=1
        CURRENT_PHASE=commit-budget
        start_test_server 1 13307 || exit 2
        run_acceptance --verify-database-target || exit 2
        run_acceptance --boundaries server-budget-commit "$OUTPUT_DIR/server-budget-commit-results.json" 1 || overall=1
        stop_server || overall=1
        CURRENT_PHASE=shutdown
        start_test_server 1 13307 || exit 2
        run_acceptance --verify-database-target || exit 2
    else
        run_acceptance --boundaries cancel "$OUTPUT_DIR/cancel-results.json" 1 || overall=1
    fi
    run_acceptance --boundaries shutdown "$OUTPUT_DIR/shutdown-results.json" "$SERVER_PID" || overall=1
    stop_server || overall=1
    for pool_size in 2 4; do
        CURRENT_PHASE="pool$pool_size"
        start_test_server "$pool_size" 3306 || exit 2
        run_acceptance --verify-database-target || exit 2
        run_acceptance --boundaries concurrency "$OUTPUT_DIR/pool$pool_size-overlap.json" "$pool_size" || overall=1
        run_acceptance "$OUTPUT_DIR/pool$pool_size-regression.json" || overall=1
        stop_server || overall=1
    done
    exit "$overall"
fi

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
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then break; fi
    if port_open 13401; then ready=1; break; fi
    sleep 0.05
done
if [[ "$ready" -ne 1 ]]; then
    printf '[ERROR] Owned test server failed to start. See server.log.\n' >&2
    exit 2
fi

run_acceptance --verify-database-target
run_acceptance "$OUTPUT_DIR/regression-results.json"
run_acceptance --write-faults "$OUTPUT_DIR/write-fault-results.json"

check_error_count=0
for ((i = 0; i < 20; ++i)); do
    # Match the SQL error identity, not the application's exception-log prefix.
    check_error_count="$(grep -Ec 'er_check_constraint_violated.*mysql\.mysql-server:3819([^0-9]|$)' "$OUTPUT_DIR/server.log" || true)"
    check_error_count="${check_error_count:-0}"
    if [[ "$check_error_count" -eq 2 ]]; then break; fi
    sleep 0.05
done
printf 'server_check_constraint_errors=%s\n' "$check_error_count" > "$OUTPUT_DIR/fault-evidence.txt"
if [[ "$check_error_count" -ne 2 ]]; then
    printf '[FAIL] Expected two server-side MySQL CHECK errors; inspect server.log.\n' >&2
    exit 1
fi
printf '[PASS] Server logs confirm both RPCs reached a rejected MySQL INSERT.\n'
