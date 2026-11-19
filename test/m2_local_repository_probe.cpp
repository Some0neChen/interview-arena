#include "AsioRuntime.h"
#include "TradeAccountRepository.h"
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/mysql.hpp>
#include <nlohmann/json.hpp>
#include <spdlog/sinks/ostream_sink.h>
#include <algorithm>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <future>
#include <iostream>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <unistd.h>

using Json = nlohmann::json;
using U64 = std::uint64_t;
void require(bool ok, const char* message) { if (!ok) throw std::runtime_error(message); }
template<class Predicate> void wait_for(Predicate check, const char* message) {
    auto end = std::chrono::steady_clock::now() + std::chrono::milliseconds(1200);
    do {
        if (check()) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (std::chrono::steady_clock::now() < end);
    throw std::runtime_error(message);
}

int hold_gate(const std::string& database) {
    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGINT);
    sigaddset(&signals, SIGTERM);
    require(pthread_sigmask(SIG_BLOCK, &signals, nullptr) == 0, "Cannot block gate shutdown signals");
    boost::asio::io_context ioc;
    boost::mysql::any_connection monitor(ioc);
    boost::mysql::connect_params params;
    params.server_address.emplace_host_and_port("127.0.0.1", 3306);
    auto user = std::getenv("TRADE_TEST_MYSQL_ADMIN_USER");
    auto password = std::getenv("TRADE_TEST_MYSQL_ADMIN_PASSWORD");
    params.username = user ? user : "root";
    params.password = password ? password : "root_dev_only";
    params.database = database;
    monitor.connect(params);
    boost::mysql::results result;
    const auto gate = "settle_" + database;
    monitor.execute(boost::mysql::with_params("SELECT GET_LOCK({},0)", gate), result);
    require(result.rows()[0][0].as_int64() == 1, "Cannot acquire settlement gate");
    std::cout << Json({{"gate_ready", gate}}).dump() << std::endl;
    int received = 0;
    const int error = sigwait(&signals, &received);
    monitor.execute(boost::mysql::with_params("SELECT RELEASE_LOCK({})", gate), result);
    monitor.close();
    return error == 0 ? 0 : 1;
}

Json race(TradeAccountRepository& repository, AsioRuntime& runtime,
          const std::string& database, U64 account, bool cancel_first) {
    const auto executor = boost::asio::make_strand(runtime.get_io_context());
    const std::string key = "probe-" + std::to_string(account);
    auto [code, order] = boost::asio::co_spawn(executor,
        repository.submit_order(account, key, "SIM.FILL", 10, 300), boost::asio::use_future).get();
    require(code == grpc::StatusCode::OK && order.has_value(), "Cannot create race order");
    boost::asio::io_context ioc;
    boost::mysql::any_connection monitor(ioc);
    boost::mysql::connect_params admin;
    admin.server_address.emplace_host_and_port("127.0.0.1", 3306);
    auto user = std::getenv("TRADE_TEST_MYSQL_ADMIN_USER");
    auto password = std::getenv("TRADE_TEST_MYSQL_ADMIN_PASSWORD");
    admin.username = user ? user : "root";
    admin.password = password ? password : "root_dev_only";
    admin.database = database;
    monitor.connect(admin);
    boost::mysql::results result;
    auto execute = [&](const auto& sql) { monitor.execute(sql, result); };
    const auto gate = "m2_local_gate_" + std::to_string(getpid());
    bool held = false, trigger = false;
    std::exception_ptr error;
    std::future<std::optional<OrderSubmitBrief>> prepare;
    std::future<std::pair<grpc::StatusCode, std::optional<CancelOrderResult>>> cancel;
    Json evidence = {{"order_id", order->order_id}, {"cancel_first", cancel_first}, {"pool_size", 2}};
    try {
        wait_for([&] {
            execute(boost::mysql::with_params("SELECT COUNT(*) FROM information_schema.PROCESSLIST "
                "WHERE DB={} AND USER='a'", database));
            return result.rows()[0][0].as_int64() == 2;
        }, "Pool needs two independent connections for this race");
        execute("CREATE TRIGGER m2_local_gate BEFORE UPDATE ON orders FOR EACH ROW BEGIN "
            "DECLARE acquired INT DEFAULT 0; IF OLD.order_id=" + std::to_string(order->order_id) +
            " AND OLD.state=5 AND NEW.state=" + std::to_string(cancel_first ? 9 : 6) +
            " THEN SET acquired=GET_LOCK('" + gate + "',5); "
            "IF acquired IS NULL OR acquired<>1 THEN SIGNAL SQLSTATE '45000' "
            "SET MESSAGE_TEXT='local race gate timed out'; END IF; "
            "DO RELEASE_LOCK('" + gate + "'); END IF; END");
        trigger = true;
        execute(boost::mysql::with_params("SELECT GET_LOCK({},0)", gate));
        require(result.rows()[0][0].as_int64() == 1, "Gate not held");
        held = true;
        auto start_prepare = [&] { prepare = boost::asio::co_spawn(executor,
            repository.async_prepare_order_dispatch(order->order_id), boost::asio::use_future); };
        auto start_cancel = [&] { cancel = boost::asio::co_spawn(executor,
            repository.async_cancel_order(account, key), boost::asio::use_future); };
        if (cancel_first) start_cancel(); else start_prepare();
        U64 winner = 0;
        wait_for([&] {
            execute(boost::mysql::with_params("SELECT OWNER_THREAD_ID FROM performance_schema.metadata_locks "
                "WHERE OBJECT_TYPE='USER LEVEL LOCK' AND OBJECT_NAME={} AND LOCK_STATUS='PENDING'", gate));
            if (result.rows().empty()) return false;
            winner = result.rows()[0][0].as_uint64();
            return true;
        }, "Winner did not reach its UPDATE");
        if (cancel_first) start_prepare(); else start_cancel();
        wait_for([&] {
            execute(boost::mysql::with_params("SELECT w.REQUESTING_THREAD_ID,w.BLOCKING_THREAD_ID,l.LOCK_MODE "
                "FROM performance_schema.data_lock_waits w JOIN performance_schema.data_locks l "
                "ON l.ENGINE=w.ENGINE AND l.ENGINE_LOCK_ID=w.REQUESTING_ENGINE_LOCK_ID "
                "WHERE w.BLOCKING_THREAD_ID={} AND l.OBJECT_SCHEMA={} AND l.OBJECT_NAME='orders' "
                "AND l.LOCK_TYPE='RECORD'", winner, database));
            if (result.rows().empty()) return false;
            auto row = result.rows()[0];
            evidence["lock_wait"] = {{"requester", row[0].as_uint64()}, {"blocker", row[1].as_uint64()},
                {"mode", std::string(row[2].as_string())}};
            return row[0].as_uint64() != winner;
        }, "Loser did not wait on winner's row lock");
    } catch (...) { error = std::current_exception(); }
    if (held) {
        try { execute(boost::mysql::with_params("SELECT RELEASE_LOCK({})", gate)); }
        catch (...) { if (!error) error = std::current_exception(); }
    }
    if (prepare.valid()) {
        try { evidence["prepare_granted"] = prepare.get().has_value(); }
        catch (...) { if (!error) error = std::current_exception(); }
    }
    if (cancel.valid()) {
        try {
            auto [status, value] = cancel.get();
            require(status == grpc::StatusCode::OK && value.has_value(), "Cancel failed");
            evidence["cancel_state"] = static_cast<int>(value->order_state);
        } catch (...) { if (!error) error = std::current_exception(); }
    }
    if (trigger) {
        try { execute("DROP TRIGGER m2_local_gate"); }
        catch (...) { if (!error) error = std::current_exception(); }
    }
    monitor.close();
    if (error) std::rethrow_exception(error);
    return evidence;
}

int main(int argc, char** argv) {
    try {
        spdlog::set_level(spdlog::level::off);
        auto env = std::getenv("TRADE_MYSQL_DATABASE");
        const std::string database = env ? env : "";
        require(database.starts_with("tradewritefaultm2trade") && database.size() <= 64 &&
            std::all_of(database.begin(), database.end(), [](char c) {
                return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9');
            }), "Refusing non-test database");
        require(argc == 3 || argc == 4, "Usage: probe mode account_or_order_id [mysql_port]");
        const std::string mode = argv[1];
        if (mode == "gate") return hold_gate(database);
        std::ostringstream captured_log;
        if (mode == "snapshot") {
            auto sink = std::make_shared<spdlog::sinks::ostream_sink_mt>(captured_log);
            spdlog::set_default_logger(std::make_shared<spdlog::logger>("snapshot-probe", sink));
            spdlog::set_level(spdlog::level::warn);
        }
        const U64 id = std::stoull(argv[2]);
        const int port = argc == 4 ? std::stoi(argv[3]) : 3306;
        require(port == 3306 || port == 13307, "Unexpected MySQL port");
        AsioRuntime runtime(2);
        boost::mysql::pool_params params;
        params.server_address.emplace_host_and_port("127.0.0.1", port);
        params.username = "a";
        params.password = "111111";
        params.database = database;
        params.initial_size = params.max_size = 2;
        params.thread_safe = true;
        if (port == 13307) params.ssl = boost::mysql::ssl_mode::disable;
        TradeAccountRepository repository(std::move(params), runtime.get_io_context());
        runtime.start();
        const auto executor = boost::asio::make_strand(runtime.get_io_context());
        Json output;
        std::exception_ptr failure;
        try {
            if (mode == "cancel-first" || mode == "prepare-first") {
                output = race(repository, runtime, database, id, mode == "cancel-first");
            } else if (mode == "seed") {
                auto [code, order] = boost::asio::co_spawn(executor,
                    repository.submit_order(id, "probe-" + std::to_string(id), "SIM.FILL", 10, 300),
                    boost::asio::use_future).get();
                require(code == grpc::StatusCode::OK && order.has_value(), "Cannot seed order");
                auto brief = boost::asio::co_spawn(executor,
                    repository.async_prepare_order_dispatch(order->order_id), boost::asio::use_future).get();
                require(brief.has_value(), "Cannot prepare dispatch");
                output = {{"order_id", order->order_id}};
            } else if (mode == "snapshot" || mode == "settle" || mode == "accepted" || mode == "cancelled" || mode == "rejected") {
                Json incoming = Json::object();
                if (mode == "snapshot") std::cin >> incoming;
                const auto state = incoming.value("broker_state",
                    mode == "accepted" ? 1 : mode == "cancelled" ? 4 : mode == "rejected" ? 3 : 2);
                require(state >= 1 && state <= 4, "Probe expects a known broker state");
                boost::asio::io_context monitor_ioc;
                boost::mysql::any_connection monitor(monitor_ioc);
                boost::mysql::connect_params admin;
                admin.server_address.emplace_host_and_port("127.0.0.1", 3306);
                auto user = std::getenv("TRADE_TEST_MYSQL_ADMIN_USER");
                auto password = std::getenv("TRADE_TEST_MYSQL_ADMIN_PASSWORD");
                admin.username = user ? user : "root";
                admin.password = password ? password : "root_dev_only";
                admin.database = database;
                monitor.connect(admin);
                boost::mysql::results row_data;
                monitor.execute(boost::mysql::with_params("SELECT account_id,symbol,quantity,limit_price_cent,"
                    "broker_order_id FROM orders WHERE order_id={}", id), row_data);
                require(row_data.rows().size() == 1, "Snapshot fixture order missing");
                const auto row = row_data.rows()[0];
                OrderRefreshPkg pkg{};
                pkg.order_id = id;
                pkg.broker_terms = BrokerOrderTerms{
                    .account_id = incoming.value("account_id", row[0].as_uint64()),
                    .broker_order_id = incoming.value("broker_order_id", row[4].is_null() ? id + 10000 : row[4].as_uint64()),
                    .quantity = incoming.value("quantity", row[2].as_uint64()),
                    .limit_price_cent = incoming.value("limit_price_cent", row[3].as_uint64()),
                    .broker_version = incoming.value("broker_version", U64(state == 1 ? 1 : 2)),
                    .reject_reason = incoming.value("reject_reason", std::string(state == 3 ? "probe rejection" : "")),
                    .symbol = incoming.value("symbol", std::string(row[1].as_string())),
                    .broker_state = static_cast<std::uint8_t>(state)
                };
                monitor.close();
                pkg.target_state = state == 1 ? trade::ORDER_STATE_BROKER_ACCEPTED :
                    state == 4 ? trade::ORDER_STATE_CANCELLED :
                    state == 3 ? trade::ORDER_STATE_REJECTED : trade::ORDER_STATE_FILLED;
                pkg.ledger_type = state == 1 ? LEDGER_ENTRY_TYPE::ORDER_NONE :
                    state == 4 ? LEDGER_ENTRY_TYPE::ORDER_CANCELLED :
                    state == 3 ? LEDGER_ENTRY_TYPE::ORDER_REJECTED : LEDGER_ENTRY_TYPE::ORDER_FILLED;
                pkg.outbox_type = state == 1 ? OUTBOX_EVENT_TYPE::OUTBOX_EVENT_DISPATCHED :
                    state == 4 ? OUTBOX_EVENT_TYPE::OUTBOX_EVENT_CANCELLED :
                    state == 3 ? OUTBOX_EVENT_TYPE::OUTBOX_EVENT_REJECTED : OUTBOX_EVENT_TYPE::OUTBOX_EVENT_FILLED;
                pkg.fill_price_cent = incoming.value("fill_price_cent", U64(state == 2 ? 280 : 0));
                pkg.filled_quntity = incoming.value("filled_quantity", U64(state == 2 ? 10 : 0));
                if (state == 2) {
                    require(!__builtin_mul_overflow(pkg.fill_price_cent, pkg.filled_quntity, &pkg.spent_cent),
                        "Probe fixture amount overflows");
                }
                try {
                    boost::asio::co_spawn(executor,
                        repository.async_dispatcher_refresh_data(pkg), boost::asio::use_future).get();
                    output = {{"threw", false}};
                } catch (const std::exception& error) {
                    output = {{"threw", true}, {"error", error.what()}};
                }
                if (mode == "snapshot") output["logs"] = captured_log.str();
            } else throw std::invalid_argument("Unknown probe mode");
        } catch (...) { failure = std::current_exception(); }
        repository.stop();
        runtime.stop();
        if (failure) std::rethrow_exception(failure);
        std::cout << output.dump() << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
