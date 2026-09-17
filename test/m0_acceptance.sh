#!/usr/bin/env bash
#
# m0_acceptance.sh —— InterviewArena M0 验收脚本
#
# 覆盖场景：
#   1. 正常请求（mock delay=100ms）
#   2. 零延迟请求（delay=0，add_task 分支）
#   3. 非法参数（question_id=0 -> INVALID_ARGUMENT）
#   4. deadline 超时（DEADLINE_EXCEEDED）
#   5. 客户端主动取消（CANCELLED）
#   6. 连续取消 10 次后服务端仍存活（生命周期 / 唯一完成权冒烟）
#   7. 并发 30 个慢请求时线程数有界（不再一请求一线程）
#   8. 空闲 SIGINT：快速干净退出
#   9. 在途请求 + SIGINT：有界收口，客户端拿到明确结果
#
# 用法：
#   ./test/m0_acceptance.sh [build目录]
#
# 退出码：0 全部通过；1 有失败；2 环境/启动失败

set -u

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
BUILD_DIR="${1:-$ROOT_DIR/build}"
SERVER_BIN="$BUILD_DIR/arena_server"
CLIENT_BIN="$BUILD_DIR/arena_client"

TMP_DIR="$(mktemp -d /tmp/arena_m0.XXXXXX)"
SERVER_OUT="$TMP_DIR/server.out"
DRAIN_OUT="$TMP_DIR/drain_client.out"

PASS=0
FAIL=0
SERVER_PID=""

pass() { PASS=$((PASS + 1)); echo "[PASS] $1"; }
fail() { FAIL=$((FAIL + 1)); echo "[FAIL] $1"; }

cleanup() {
    if [[ -n "$SERVER_PID" ]] && kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -INT "$SERVER_PID" 2>/dev/null
        for _ in $(seq 1 30); do
            kill -0 "$SERVER_PID" 2>/dev/null || break
            sleep 0.1
        done
        kill -9 "$SERVER_PID" 2>/dev/null
    fi
    echo "[INFO] 临时输出目录: $TMP_DIR"
}
trap cleanup EXIT

start_server() {
    echo "[INFO] 启动服务端: $SERVER_BIN"
    "$SERVER_BIN" >"$SERVER_OUT" 2>&1 &
    SERVER_PID=$!
    sleep 1
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        echo "[FATAL] 服务端启动失败，输出如下："
        cat "$SERVER_OUT"
        exit 2
    fi
}

stop_server_wait() { # $1=最多等待秒数
    local max_wait="$1" i
    kill -INT "$SERVER_PID"
    for i in $(seq 1 $((max_wait * 10))); do
        kill -0 "$SERVER_PID" 2>/dev/null || break
        sleep 0.1
    done
    if kill -0 "$SERVER_PID" 2>/dev/null; then
        kill -9 "$SERVER_PID" 2>/dev/null
        SERVER_PID=""
        return 1
    fi
    SERVER_PID=""
    return 0
}

run_expect() { # $1=用例名 $2=期望子串，其后为客户端参数
    local name="$1" expect="$2"
    shift 2
    local out
    out="$(timeout 15 "$CLIENT_BIN" "$@" 2>&1)"
    if echo "$out" | grep -q "$expect"; then
        pass "$name"
    else
        fail "$name（期望包含: $expect）"
        echo "$out" | sed 's/^/       | /'
    fi
}

check_alive() { # $1=说明
    if kill -0 "$SERVER_PID" 2>/dev/null; then
        pass "$1"
    else
        fail "$1（服务端已退出）"
        echo "       | 服务端输出:"
        cat "$SERVER_OUT" | sed 's/^/       | /'
        exit 1
    fi
}

echo "=== M0 验收开始 ==="
echo

# 清理泄漏的旧进程（精确匹配进程名，避免误杀）
if pgrep -x arena_server >/dev/null 2>&1; then
    echo "[WARN] 发现残留 arena_server，先清理"
    pkill -x -9 arena_server
    sleep 0.3
fi

if [[ ! -x "$SERVER_BIN" || ! -x "$CLIENT_BIN" ]]; then
    echo "[FATAL] 找不到可执行文件：$SERVER_BIN / $CLIENT_BIN，请先构建"
    exit 2
fi

# ---------- 功能场景 ----------
start_server
run_expect "正常请求 delay=100"      "Question ID: 1" --question-id 1 --mock-delay-ms 100
run_expect "零延迟请求 delay=0"      "Question ID: 1" --question-id 1
run_expect "非法参数 id=0"           "status code: 3" --question-id 0
run_expect "deadline 超时"           "status code: 4" --question-id 1 --mock-delay-ms 500 --deadline-ms 100
run_expect "客户端主动取消"          "status code: 1" --question-id 1 --mock-delay-ms 500 --cancel-after-ms 50
check_alive "功能场景后服务端存活"

# ---------- 连续取消冒烟 ----------
cancel_ok=1
for _ in $(seq 1 10); do
    timeout 15 "$CLIENT_BIN" --question-id 1 --mock-delay-ms 500 --cancel-after-ms 20 >/dev/null 2>&1
    if ! kill -0 "$SERVER_PID" 2>/dev/null; then
        cancel_ok=0
        break
    fi
done
if [[ "$cancel_ok" -eq 1 ]]; then
    pass "连续取消 10 次后服务端存活"
else
    fail "连续取消 10 次后服务端崩溃"
fi

# ---------- 并发线程数有界 ----------
idle_threads="$(ls -1 "/proc/$SERVER_PID/task" | wc -l)"
client_pids=()
for _ in $(seq 1 30); do
    timeout 20 "$CLIENT_BIN" --question-id 1 --mock-delay-ms 3000 >/dev/null 2>&1 &
    client_pids+=("$!")
done
sleep 1
load_threads="$(ls -1 "/proc/$SERVER_PID/task" | wc -l)"
if [[ "$load_threads" -le $((idle_threads + 15)) ]]; then
    pass "并发 30 个慢请求线程数有界（空闲 $idle_threads -> 并发 $load_threads）"
else
    fail "并发 30 个慢请求线程数失控（空闲 $idle_threads -> 并发 $load_threads）"
fi
for pid in "${client_pids[@]}"; do
    wait "$pid" 2>/dev/null
done

# ---------- 空闲 SIGINT ----------
s0="$(date +%s%N)"
if stop_server_wait 3; then
    ms=$(( ($(date +%s%N) - s0) / 1000000 ))
    pass "空闲 SIGINT 快速退出（${ms}ms）"
else
    fail "空闲 SIGINT 未在 3 秒内退出"
fi

# ---------- 在途请求 + SIGINT（有界收口） ----------
start_server
timeout 20 "$CLIENT_BIN" --question-id 1 --mock-delay-ms 3000 >"$DRAIN_OUT" 2>&1 &
drain_pid=$!
sleep 0.3
if stop_server_wait 8; then
    pass "在途请求 SIGINT 有界收口（8 秒内退出）"
else
    fail "在途请求 SIGINT 未在 8 秒内退出"
fi
wait "$drain_pid" 2>/dev/null
if grep -qE "Question ID: 1|status code:" "$DRAIN_OUT"; then
    pass "在途客户端获得明确结果"
else
    fail "在途客户端结果异常"
    sed 's/^/       | /' "$DRAIN_OUT"
fi

# ---------- 汇总 ----------
echo
echo "summary: pass=$PASS fail=$FAIL"
if [[ "$FAIL" -eq 0 ]]; then
    echo "M0 验收通过"
    exit 0
fi
echo "M0 验收未通过"
exit 1
