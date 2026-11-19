#include "AsioRuntime.h"
#include "TradeBrokerRepository.h"
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/mysql.hpp>
#include <nlohmann/json.hpp>
#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdlib>
#include <future>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <thread>
#include <unistd.h>

using Json = nlohmann::json;
using U64 = std::uint64_t;

U64 number(std::string_view text)
{
    U64 value{};
    auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc{} || end != text.data() + text.size() || value == 0)
        throw std::invalid_argument("Expected a positive integer");
    return value;
}

void require(bool condition, const char* message)
{
    if (!condition) throw std::runtime_error(message);
}

template<class Predicate>
void wait_for(Predicate predicate, const char* description)
{
    const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(1200);
    do {
        if (predicate()) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (std::chrono::steady_clock::now() < until);
    throw std::runtime_error(description);
}

Json run(const std::string& database, bool cancel_first, U64 local_id, U64 broker_id)
{
    boost::asio::io_context monitor_ioc;
    boost::mysql::any_connection monitor(monitor_ioc);
    boost::mysql::connect_params admin;
    admin.server_address.emplace_host_and_port("127.0.0.1", 3306);
    const char* user = std::getenv("TRADE_TEST_MYSQL_ADMIN_USER");
    const char* password = std::getenv("TRADE_TEST_MYSQL_ADMIN_PASSWORD");
    admin.username = user ? user : "root";
    admin.password = password ? password : "root_dev_only";
    admin.database = database;
    monitor.connect(admin);
    boost::mysql::results result;
    const auto execute = [&](const auto& statement) { monitor.execute(statement, result); };

    AsioRuntime runtime(2);
    boost::mysql::pool_params params;
    params.server_address.emplace_host_and_port("127.0.0.1", 3306);
    params.username = "a";
    params.password = "111111";
    params.database = database;
    params.initial_size = params.max_size = 2;
    params.thread_safe = true;
    TradeBrokerRepository repository(std::move(params), runtime.get_io_context());
    runtime.start();
    const auto executor = boost::asio::make_strand(runtime.get_io_context());
    std::future<void> fill;
    std::future<std::pair<grpc::StatusCode, std::optional<BrokerOrderInfo>>> cancel;
    std::exception_ptr failure;
    bool trigger_created = false;
    bool gate_held = false;
    const std::string gate = "m2_cancel_fill_" + std::to_string(getpid());
    const int winning_state = cancel_first ? 4 : 2;
    Json evidence = {{"order", cancel_first ? "cancel-first" : "fill-first"},
                     {"pool_size", 2}, {"gateway_order_id", local_id}, {"broker_order_id", broker_id}};

    try {
        execute(boost::mysql::with_params(
            "SELECT gateway_order_id, state FROM broker_orders WHERE broker_order_id={} "
            "AND gateway_namespace='SolCelestial69'", broker_id));
        require(result.rows().size() == 1 && result.rows()[0][0].as_uint64() == local_id &&
                result.rows()[0][1].as_uint64() == 1, "Fixture must be the intended ACCEPTED order");
        wait_for([&] {
            execute(boost::mysql::with_params(
                "SELECT COUNT(*) FROM information_schema.PROCESSLIST WHERE DB={} AND USER='a'", database));
            return result.rows()[0][0].as_int64() == 2;
        }, "Two pool connections did not become available");

        // The trigger pauses the winner while its real UPDATE holds the order's row lock.
        execute("CREATE TRIGGER m2_cancel_fill_gate BEFORE UPDATE ON broker_orders FOR EACH ROW BEGIN "
                "DECLARE acquired INT DEFAULT 0; "
                "IF OLD.broker_order_id=" + std::to_string(broker_id) + " AND OLD.state=1 AND NEW.state=" +
                std::to_string(winning_state) + " THEN "
                "SET acquired=GET_LOCK('" + gate + "',5); "
                "IF acquired IS NULL OR acquired<>1 THEN SIGNAL SQLSTATE '45000' "
                "SET MESSAGE_TEXT='race test gate timed out'; END IF; "
                "DO RELEASE_LOCK('" + gate + "'); END IF; END");
        trigger_created = true;
        execute(boost::mysql::with_params("SELECT GET_LOCK({},0)", gate));
        require(result.rows()[0][0].as_int64() == 1, "Cannot acquire test gate");
        gate_held = true;

        const auto start_fill = [&] {
            fill = boost::asio::co_spawn(executor, repository.async_try_fill_order(
                broker_id, broker::BROKER_ORDER_STATE_ACCEPTED), boost::asio::use_future);
        };
        const auto start_cancel = [&] {
            cancel = boost::asio::co_spawn(executor, repository.async_cancel_order(
                BrokerOrderClintId{local_id, "SolCelestial69"}), boost::asio::use_future);
        };
        if (cancel_first) start_cancel(); else start_fill();

        U64 winner_thread = 0;
        wait_for([&] {
            execute(boost::mysql::with_params("SELECT OWNER_THREAD_ID FROM performance_schema.metadata_locks "
                "WHERE OBJECT_TYPE='USER LEVEL LOCK' AND OBJECT_NAME={} AND LOCK_STATUS='PENDING'", gate));
            if (result.rows().empty()) return false;
            winner_thread = result.rows()[0][0].as_uint64();
            return true;
        }, "Winner never reached the trigger while holding the order lock");
        evidence["winner_thread_id"] = winner_thread;
        if (cancel_first) start_fill(); else start_cancel();

        wait_for([&] {
            execute(boost::mysql::with_params(
                "SELECT w.REQUESTING_THREAD_ID,w.BLOCKING_THREAD_ID,l.LOCK_TYPE,l.LOCK_MODE,l.LOCK_DATA "
                "FROM performance_schema.data_lock_waits w JOIN performance_schema.data_locks l "
                "ON l.ENGINE=w.ENGINE AND l.ENGINE_LOCK_ID=w.REQUESTING_ENGINE_LOCK_ID "
                "WHERE w.BLOCKING_THREAD_ID={} AND l.OBJECT_SCHEMA={} AND l.OBJECT_NAME='broker_orders'",
                winner_thread, database));
            if (result.rows().empty()) return false;
            auto row = result.rows()[0];
            evidence["lock_wait"] = {{"requesting_thread_id", row[0].as_uint64()},
                {"blocking_thread_id", row[1].as_uint64()}, {"lock_type", std::string(row[2].as_string())},
                {"lock_mode", std::string(row[3].as_string())},
                {"lock_data", row[4].is_null() ? "" : std::string(row[4].as_string())}};
            return row[0].as_uint64() != winner_thread && row[2].as_string() == "RECORD";
        }, "The opposing real SQL operation did not wait for the winner's row lock");
        execute(boost::mysql::with_params("SELECT RELEASE_LOCK({})", gate));
        require(result.rows()[0][0].as_int64() == 1, "Test gate was not released");
        gate_held = false;
    } catch (...) {
        failure = std::current_exception();
    }

    // Release the barrier before draining either coroutine, including failed test paths.
    if (gate_held) {
        try { execute(boost::mysql::with_params("SELECT RELEASE_LOCK({})", gate)); }
        catch (...) { if (!failure) failure = std::current_exception(); }
    }
    if (fill.valid()) {
        try { fill.get(); evidence["fill_completed"] = true; }
        catch (...) { if (!failure) failure = std::current_exception(); }
    }
    if (cancel.valid()) {
        try {
            auto [code, info] = cancel.get();
            require(code == grpc::StatusCode::OK && info.has_value(), "Cancel did not return an order snapshot");
            evidence["cancel_result"] = {{"grpc_code", static_cast<int>(code)},
                {"state", static_cast<int>(info->state)}, {"broker_version", info->broker_version}};
            require(static_cast<int>(info->state) == winning_state && info->broker_version == 2,
                    "Cancel must return the winner's unique terminal state and version");
        } catch (...) { if (!failure) failure = std::current_exception(); }
    }
    if (trigger_created) {
        try { execute("DROP TRIGGER m2_cancel_fill_gate"); }
        catch (...) { if (!failure) failure = std::current_exception(); }
    }
    repository.stop();
    runtime.stop();
    monitor.close();
    if (failure) std::rethrow_exception(failure);
    return evidence;
}

int main(int argc, char** argv)
{
    try {
        spdlog::set_level(spdlog::level::off);
        const char* value = std::getenv("TRADE_MYSQL_DATABASE");
        const std::string database = value ? value : "";
        require(database.starts_with("tradewritefaultm2broker") && database.size() <= 64 &&
                std::all_of(database.begin(), database.end(), [](char c) {
                    return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9');
                }), "Refusing a non-test database");
        require(argc == 4, "Usage: m2_broker_cancel_fill_race cancel-first|fill-first local_id broker_id");
        const std::string mode = argv[1];
        require(mode == "cancel-first" || mode == "fill-first", "Unknown race order");
        std::cout << run(database, mode == "cancel-first", number(argv[2]), number(argv[3])).dump() << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Cancel/fill race failed: " << error.what() << '\n';
        return 1;
    }
}
