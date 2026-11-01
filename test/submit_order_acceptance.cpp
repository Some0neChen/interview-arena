#include "trade_account.grpc.pb.h"
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/read_until.hpp>
#include <boost/asio/write.hpp>
#include <boost/mysql/common_server_errc.hpp>
#include <boost/mysql/error_with_diagnostics.hpp>
#include <boost/mysql/error_categories.hpp>
#include <boost/mysql/handshake_params.hpp>
#include <boost/mysql/results.hpp>
#include <boost/mysql/mysql_server_errc.hpp>
#include <boost/mysql/tcp.hpp>
#include <grpcpp/grpcpp.h>
#include <nlohmann/json.hpp>
#include <chrono>
#include <algorithm>
#include <cstdlib>
#include <cstdint>
#include <csignal>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <iterator>
#include <latch>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>
#include <thread>
#include <unistd.h>

using Json = nlohmann::json;
using U64 = std::uint64_t;

std::string database_name() {
    const char* value = std::getenv("TRADE_MYSQL_DATABASE");
    return value && value[0] ? value : "arena";
}

bool is_write_fault_database(const std::string& name) {
    return name.starts_with("tradewritefault") && name.size() > 20 && name.size() <= 64 &&
           std::all_of(name.begin(), name.end(), [](char c) {
               return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9');
           });
}

// Only the test script uses this administrator connection for an owned disposable schema.
int admin_sql(const std::string& database) {
    if (!database.empty() && !is_write_fault_database(database)) {
        std::cerr << "Refusing administrator SQL in a non-test database\n";
        return 2;
    }
    try {
        const char* user = std::getenv("TRADE_TEST_MYSQL_ADMIN_USER");
        const char* password = std::getenv("TRADE_TEST_MYSQL_ADMIN_PASSWORD");
        boost::asio::io_context ioc;
        boost::mysql::tcp_connection conn(ioc);
        conn.connect(boost::asio::ip::tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), 3306),
                     boost::mysql::handshake_params(user ? user : "root",
                                                   password ? password : "root_dev_only", database));
        const std::string sql{std::istreambuf_iterator<char>(std::cin), std::istreambuf_iterator<char>()};
        if (sql.empty()) throw std::runtime_error("Expected one SQL statement on stdin");
        boost::mysql::results result;
        conn.execute(sql, result);
        for (auto row : result.rows()) {
            for (std::size_t i = 0; i < row.size(); ++i) {
                if (i) std::cout << '\t';
                std::cout << row[i];
            }
            std::cout << '\n';
        }
        conn.close();
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "Administrator preflight/SQL failed: " << ex.what() << '\n';
        return 2;
    }
}

struct Fixture {
    U64 id;
    std::string name;
};

class Database {
    boost::asio::io_context ioc_;
    boost::mysql::tcp_connection conn_{ioc_};
    std::string manifest_path_;
    bool has_ledger_table_ = false;
    bool has_outbox_table_ = false;
public:
    std::vector<Fixture> fixtures;

    explicit Database(std::string manifest_path = {}) : manifest_path_(std::move(manifest_path)) {
        conn_.connect(boost::asio::ip::tcp::endpoint(boost::asio::ip::make_address("127.0.0.1"), 3306),
                      boost::mysql::handshake_params("a", "111111", database_name()));
        boost::mysql::results result;
        conn_.execute("SET SESSION innodb_lock_wait_timeout = 3", result);
        conn_.execute("SELECT COUNT(*) FROM information_schema.tables "
                      "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'fund_ledger'", result);
        has_ledger_table_ = result.rows()[0][0].as_int64() > 0;
        conn_.execute("SELECT COUNT(*) FROM information_schema.tables "
                      "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'outbox_events'", result);
        has_outbox_table_ = result.rows()[0][0].as_int64() > 0;
    }

    ~Database() {
        boost::mysql::error_code ec;
        boost::mysql::diagnostics diagnostics;
        conn_.close(ec, diagnostics);
    }

    Json snapshot() {
        boost::mysql::results result;
        conn_.execute("SELECT account_id, account_name, initial_budget_cent, available_cent, "
                      "frozen_cent, spent_cent, CAST(created_at AS CHAR), CAST(updated_at AS CHAR) "
                      "FROM accounts ORDER BY account_id", result);
        Json rows = Json::array();
        for (auto row : result.rows()) {
            rows.push_back({{"account_id", row[0].as_uint64()},
                            {"account_name", std::string(row[1].as_string())},
                            {"initial", row[2].as_uint64()}, {"available", row[3].as_uint64()},
                            {"frozen", row[4].as_uint64()}, {"spent", row[5].as_uint64()},
                            {"created_at", std::string(row[6].as_string())},
                            {"updated_at", std::string(row[7].as_string())}});
        }
        return rows;
    }

    Json account(U64 id) {
        auto rows = snapshot();
        for (const auto& row : rows) {
            if (row.at("account_id").get<U64>() == id) return row;
        }
        return nullptr;
    }

    Json snapshot_orders() {
        boost::mysql::results result;
        conn_.execute("SELECT order_id, account_id, client_order_id, symbol, limit_price_cent, quantity, "
                      "reserved_cent, state, version, CAST(created_at AS CHAR), CAST(updated_at AS CHAR) "
                      "FROM orders ORDER BY order_id", result);
        Json rows = Json::array();
        for (auto row : result.rows()) {
            rows.push_back({{"order_id", row[0].as_uint64()}, {"account_id", row[1].as_uint64()},
                            {"client_order_id", std::string(row[2].as_string())},
                            {"symbol", std::string(row[3].as_string())},
                            {"limit_price_cent", row[4].as_uint64()}, {"quantity", row[5].as_uint64()},
                            {"reserved_cent", row[6].as_uint64()}, {"state", row[7].as_uint64()},
                            {"version", row[8].as_uint64()}, {"created_at", std::string(row[9].as_string())},
                            {"updated_at", std::string(row[10].as_string())}});
        }
        return rows;
    }

    Json orders_for(U64 account_id) {
        Json rows = Json::array();
        for (const auto& row : snapshot_orders()) {
            if (row.at("account_id").get<U64>() == account_id) rows.push_back(row);
        }
        return rows;
    }

    Json snapshot_ledger() {
        if (!has_ledger_table_) {
            throw std::runtime_error("fund_ledger is missing; import sql/trade_fund_ledger_schema.sql first");
        }
        boost::mysql::results result;
        conn_.execute("SELECT ledger_id, account_id, order_id, entry_type, amount_cent, "
                      "CAST(created_at AS CHAR) FROM fund_ledger ORDER BY ledger_id", result);
        Json rows = Json::array();
        for (auto row : result.rows()) {
            rows.push_back({{"ledger_id", row[0].as_uint64()}, {"account_id", row[1].as_uint64()},
                            {"order_id", row[2].as_uint64()}, {"entry_type", row[3].as_uint64()},
                            {"amount_cent", row[4].as_uint64()},
                            {"created_at", std::string(row[5].as_string())}});
        }
        return rows;
    }

    Json ledger_for(U64 account_id) {
        Json rows = Json::array();
        for (const auto& row : snapshot_ledger()) {
            if (row.at("account_id").get<U64>() == account_id) rows.push_back(row);
        }
        return rows;
    }

    Json snapshot_outbox() {
        if (!has_outbox_table_) {
            throw std::runtime_error("outbox_events is missing; import sql/trade_outbox_events_schema.sql first");
        }
        boost::mysql::results result;
        conn_.execute("SELECT event_id, order_id, order_version, event_type, CAST(payload AS CHAR), "
                      "publish_state, CAST(created_at AS CHAR), CAST(published_at AS CHAR) "
                      "FROM outbox_events ORDER BY event_id", result);
        Json rows = Json::array();
        for (auto row : result.rows()) {
            Json published_at = nullptr;
            if (!row[7].is_null()) published_at = std::string(row[7].as_string());
            rows.push_back({{"event_id", row[0].as_uint64()}, {"order_id", row[1].as_uint64()},
                            {"order_version", row[2].as_uint64()}, {"event_type", row[3].as_uint64()},
                            {"payload", Json::parse(std::string(row[4].as_string()))},
                            {"publish_state", row[5].as_uint64()},
                            {"created_at", std::string(row[6].as_string())},
                            {"published_at", std::move(published_at)}});
        }
        return rows;
    }

    Json outbox_for(U64 account_id) {
        const auto orders = orders_for(account_id);
        Json rows = Json::array();
        for (const auto& event : snapshot_outbox()) {
            const auto& payload = event.at("payload");
            // Payload ownership also finds orphan events if transaction rollback regresses.
            bool belongs = payload.is_object() && payload.value("account_id", Json()) == Json(account_id);
            for (const auto& order : orders) {
                belongs = belongs || event.at("order_id") == order.at("order_id");
            }
            if (belongs) rows.push_back(event);
        }
        return rows;
    }

    bool duplicate_freeze_is_rejected(const Fixture& fixture, U64 order_id, U64 amount) {
        auto statement = conn_.prepare_statement(
            "INSERT INTO fund_ledger (account_id, order_id, entry_type, amount_cent) VALUES (?, ?, 1, ?)");
        boost::mysql::results result;
        boost::mysql::error_code ec;
        boost::mysql::diagnostics diagnostics;
        conn_.execute(statement.bind(fixture.id, order_id, amount), result, ec, diagnostics);
        conn_.close_statement(statement);
        return ec == boost::mysql::common_server_errc::er_dup_entry;
    }

    bool duplicate_event_is_rejected(const Json& event) {
        auto statement = conn_.prepare_statement(
            "INSERT INTO outbox_events (order_id, order_version, event_type, payload) VALUES (?, ?, ?, ?)");
        boost::mysql::results result;
        boost::mysql::error_code ec;
        boost::mysql::diagnostics diagnostics;
        const auto payload = event.at("payload").dump();
        conn_.execute(statement.bind(event.at("order_id").get<U64>(), event.at("order_version").get<U64>(),
                                     event.at("event_type").get<U64>(), payload), result, ec, diagnostics);
        conn_.close_statement(statement);
        return ec == boost::mysql::common_server_errc::er_dup_entry;
    }

    Json order(U64 account_id, const std::string& client_id) {
        for (const auto& row : orders_for(account_id)) {
            if (row.at("client_order_id").get<std::string>() == client_id) return row;
        }
        return nullptr;
    }

    Fixture create(const std::string& name, U64 budget = 10000) {
        auto statement = conn_.prepare_statement(
            "INSERT INTO accounts (account_name, initial_budget_cent, available_cent) VALUES (?, ?, ?)");
        boost::mysql::results result;
        conn_.execute(statement.bind(name, budget, budget), result);
        Fixture fixture{result.last_insert_id(), name};
        fixtures.push_back(fixture);
        if (!manifest_path_.empty()) {
            Json manifest = Json::array();
            for (const auto& row : fixtures) manifest.push_back({{"id", row.id}, {"name", row.name}});
            const auto temporary = manifest_path_ + ".tmp";
            std::ofstream file(temporary);
            file << manifest.dump(2) << '\n';
            file.close();
            if (!file) throw std::runtime_error("Cannot write fixture cleanup manifest");
            std::filesystem::rename(temporary, manifest_path_);
        }
        conn_.close_statement(statement);
        return fixture;
    }

    U64 erase(const Fixture& fixture) {
        const auto existing = account(fixture.id);
        if (!existing.is_null() && existing.at("account_name").get<std::string>() != fixture.name) {
            throw std::runtime_error("Refusing to clean a fixture whose account name changed");
        }
        boost::mysql::results result;
        if (has_outbox_table_) {
            const auto events = outbox_for(fixture.id);
            auto delete_event = conn_.prepare_statement("DELETE FROM outbox_events WHERE event_id = ?");
            for (const auto& event : events) {
                conn_.execute(delete_event.bind(event.at("event_id").get<U64>()), result);
            }
            conn_.close_statement(delete_event);
        }
        if (has_ledger_table_) {
            auto delete_ledger = conn_.prepare_statement("DELETE FROM fund_ledger WHERE account_id = ?");
            conn_.execute(delete_ledger.bind(fixture.id), result);
            conn_.close_statement(delete_ledger);
        }
        auto delete_orders = conn_.prepare_statement("DELETE FROM orders WHERE account_id = ?");
        conn_.execute(delete_orders.bind(fixture.id), result);
        conn_.close_statement(delete_orders);
        auto statement = conn_.prepare_statement("DELETE FROM accounts WHERE account_id = ? AND account_name = ?");
        conn_.execute(statement.bind(fixture.id, fixture.name), result);
        auto changed = result.affected_rows();
        conn_.close_statement(statement);
        return changed;
    }

    void set_frozen(const Fixture& fixture, U64 frozen) {
        auto statement = conn_.prepare_statement(
            "UPDATE accounts SET frozen_cent = ? WHERE account_id = ? AND account_name = ?");
        boost::mysql::results result;
        conn_.execute(statement.bind(frozen, fixture.id, fixture.name), result);
        if (result.affected_rows() != 1) throw std::runtime_error("Fixture update did not affect one row");
        conn_.close_statement(statement);
    }

    void set_order_state(const Fixture& fixture, U64 order_id, U64 state) {
        auto statement = conn_.prepare_statement(
            "UPDATE orders SET state = ?, version = version + 1 WHERE order_id = ? AND account_id = ?");
        boost::mysql::results result;
        conn_.execute(statement.bind(state, order_id, fixture.id), result);
        if (result.affected_rows() != 1) throw std::runtime_error("Order fixture update did not affect one row");
        conn_.close_statement(statement);
    }

    void seed_release_conflict(const Fixture& fixture, const Json& order) {
        auto statement = conn_.prepare_statement(
            "INSERT INTO fund_ledger (account_id, order_id, entry_type, amount_cent) VALUES (?, ?, 2, ?)");
        boost::mysql::results result;
        conn_.execute(statement.bind(fixture.id, order.at("order_id").get<U64>(),
                                     order.at("reserved_cent").get<U64>()), result);
        conn_.close_statement(statement);
    }

    void seed_cancel_event_conflict(const Fixture& fixture, const Json& order) {
        const Json payload = {{"account_id", fixture.id}, {"client_order_id", order.at("client_order_id")},
                              {"state", static_cast<int>(trade::ORDER_STATE_CANCELLED)}};
        const auto text = payload.dump();
        auto statement = conn_.prepare_statement(
            "INSERT INTO outbox_events (order_id, order_version, event_type, payload) VALUES (?, ?, 2, ?)");
        boost::mysql::results result;
        conn_.execute(statement.bind(order.at("order_id").get<U64>(), order.at("version").get<U64>() + 1,
                                     text), result);
        conn_.close_statement(statement);
    }

    void wait_for_transaction_end(const Fixture& fixture) {
        auto statement = conn_.prepare_statement(
            "UPDATE accounts SET available_cent = available_cent WHERE account_id = ? AND account_name = ?");
        boost::mysql::results result;
        // Even a no-op UPDATE needs the row lock; it cannot pass an uncommitted writer.
        conn_.execute(statement.bind(fixture.id, fixture.name), result);
        conn_.close_statement(statement);
    }

    Json snapshot_all() {
        return {{"accounts", snapshot()}, {"orders", snapshot_orders()},
                {"fund_ledger", snapshot_ledger()}, {"outbox_events", snapshot_outbox()}};
    }

    void require_write_fault_database() {
        boost::mysql::results result;
        conn_.execute("SELECT DATABASE()", result);
        if (!is_write_fault_database(std::string(result.rows()[0][0].as_string()))) {
            throw std::runtime_error("Write faults require a script-owned tradewritefault... database, never arena");
        }
    }

    void set_write_fault(bool ledger, bool enabled) {
        require_write_fault_database();
        boost::mysql::results result;
        const char* sql = ledger
            ? (enabled ? "ALTER TABLE fund_ledger ADD CONSTRAINT fault_reject_freeze CHECK (entry_type <> 1) ENFORCED"
                       : "ALTER TABLE fund_ledger DROP CHECK fault_reject_freeze")
            : (enabled ? "ALTER TABLE outbox_events ADD CONSTRAINT fault_reject_acceptance CHECK (event_type <> 1) ENFORCED"
                       : "ALTER TABLE outbox_events DROP CHECK fault_reject_acceptance");
        conn_.execute(sql, result);
    }

    bool write_fault_is_enforced(bool ledger, U64 account_id) {
        require_write_fault_database();
        boost::mysql::results result;
        boost::mysql::error_code ec;
        boost::mysql::diagnostics diagnostics;
        auto statement = conn_.prepare_statement(ledger
            ? "INSERT INTO fund_ledger (account_id, order_id, entry_type, amount_cent) VALUES (?, 0, 1, 3000)"
            : "INSERT INTO outbox_events (order_id, order_version, event_type, payload) VALUES (?, 1, 1, '{}')");
        conn_.execute(statement.bind(ledger ? account_id : U64{0}), result, ec, diagnostics);
        conn_.close_statement(statement);
        return ec.category() == boost::mysql::get_mysql_server_category() &&
               ec.value() == boost::mysql::mysql_server_errc::er_check_constraint_violated;
    }

    void wait_for_order_transaction_end(U64 account_id, const std::string& client_id) {
        boost::mysql::results result;
        conn_.execute("START TRANSACTION", result);
        try {
            auto statement = conn_.prepare_statement(
                "SELECT order_id FROM orders WHERE account_id = ? AND client_order_id = ? FOR UPDATE");
            // Unlike a snapshot read, this waits for the RPC's inserted unique key to commit or roll back.
            conn_.execute(statement.bind(account_id, client_id), result);
            conn_.close_statement(statement);
            conn_.execute("ROLLBACK", result);
        } catch (...) {
            boost::mysql::error_code ec;
            boost::mysql::diagnostics diagnostics;
            conn_.execute("ROLLBACK", result, ec, diagnostics);
            throw;
        }
    }

    void lock_accounts(const std::vector<Fixture>& accounts) {
        require_write_fault_database();
        boost::mysql::results result;
        conn_.execute("START TRANSACTION", result);
        auto statement = conn_.prepare_statement("SELECT account_id FROM accounts WHERE account_id = ? FOR UPDATE");
        for (const auto& fixture : accounts) conn_.execute(statement.bind(fixture.id), result);
        conn_.close_statement(statement);
    }

    void release_locks() {
        boost::mysql::results result;
        conn_.execute("ROLLBACK", result);
    }

    Json active_account_updates() {
        boost::mysql::results result;
        conn_.execute("SELECT ID, INFO FROM information_schema.PROCESSLIST WHERE USER = 'a' "
                      "AND DB = DATABASE() AND INFO LIKE 'update accounts set available_cent = available_cent -%'", result);
        Json rows = Json::array();
        for (auto row : result.rows()) {
            rows.push_back({{"connection_id", row[0].as_uint64()}, {"sql", std::string(row[1].as_string())}});
        }
        return rows;
    }
};

struct RpcResult {
    int code;
    std::string message;
    trade::SubmitOrderResponse response;
};

struct CancelRpcResult {
    int code;
    std::string message;
    trade::CancelOrderResponse response;
};

CancelRpcResult cancel_order(U64 id, const std::string& client_id) {
    auto channel = grpc::CreateChannel("127.0.0.1:13401", grpc::InsecureChannelCredentials());
    auto stub = trade::TradeAccount::NewStub(channel);
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(6));
    trade::CancelOrderRequest request;
    request.set_account_id(id);
    request.set_client_order_id(client_id);
    trade::CancelOrderResponse response;
    auto status = stub->CancelOrder(&context, request, &response);
    return {static_cast<int>(status.error_code()), status.error_message(), std::move(response)};
}

RpcResult submit_with_context(U64 id, const std::string& client_id, U64 quantity, U64 price,
                              const std::string& symbol, grpc::ClientContext& context) {
    auto channel = grpc::CreateChannel("127.0.0.1:13401", grpc::InsecureChannelCredentials());
    auto stub = trade::TradeAccount::NewStub(channel);
    trade::SubmitOrderRequest request;
    request.set_account_id(id);
    request.set_client_order_id(client_id);
    request.set_symbol(symbol);
    request.set_quantity(quantity);
    request.set_limit_price_cent(price);
    trade::SubmitOrderResponse response;
    auto status = stub->SubmitOrder(&context, request, &response);
    return {static_cast<int>(status.error_code()), status.error_message(), std::move(response)};
}

RpcResult submit(U64 id, const std::string& client_id, U64 quantity, U64 price,
                 const std::string& symbol = "DEMO_A", int deadline_ms = 6000) {
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_ms));
    return submit_with_context(id, client_id, quantity, price, symbol, context);
}

Json rpc_json(const RpcResult& result) {
    return {{"grpc_code", result.code}, {"message", result.message},
            {"state", trade::OrderState_Name(result.response.state())},
            {"order_id", result.response.order_id()}};
}

Json rpc_json(const CancelRpcResult& result) {
    return {{"grpc_code", result.code}, {"message", result.message},
            {"state", trade::OrderState_Name(result.response.state())},
            {"order_id", result.response.order_id()}};
}

bool state_is(const RpcResult& result, trade::OrderState state) {
    return result.code == 0 && result.response.state() == state;
}

bool money_is(const Json& account, U64 available, U64 frozen, U64 initial = 10000) {
    if (account.is_null()) return false;
    return account.at("initial").get<U64>() == initial &&
           account.at("available").get<U64>() == available &&
           account.at("frozen").get<U64>() == frozen &&
           account.at("spent").get<U64>() == 0 && available + frozen == initial;
}

bool new_order_is(const Json& order, const RpcResult& rpc, U64 account_id, const std::string& client_id,
                  U64 quantity, U64 price) {
    return !order.is_null() && rpc.response.order_id() != 0 &&
           order.at("order_id").get<U64>() == rpc.response.order_id() &&
           order.at("account_id").get<U64>() == account_id &&
           order.at("client_order_id").get<std::string>() == client_id &&
           order.at("symbol").get<std::string>() == "DEMO_A" &&
           order.at("quantity").get<U64>() == quantity &&
           order.at("limit_price_cent").get<U64>() == price &&
           order.at("reserved_cent").get<U64>() == quantity * price &&
           order.at("state").get<U64>() == trade::ORDER_STATE_LOCAL_ACCEPTED &&
           order.at("version").get<U64>() == 1;
}

bool freeze_entry_is(const Json& entry, U64 account_id, U64 order_id, U64 amount) {
    return entry.at("ledger_id").get<U64>() != 0 &&
           entry.at("account_id").get<U64>() == account_id &&
           entry.at("order_id").get<U64>() == order_id &&
           entry.at("entry_type").get<U64>() == 1 &&
           entry.at("amount_cent").get<U64>() == amount &&
           !entry.at("created_at").get<std::string>().empty();
}

bool ledger_matches_orders(const Json& entries, const Json& orders) {
    if (entries.size() != orders.size()) return false;
    for (const auto& order : orders) {
        int matches = 0;
        for (const auto& entry : entries) {
            matches += freeze_entry_is(entry, order.at("account_id").get<U64>(),
                                       order.at("order_id").get<U64>(), order.at("reserved_cent").get<U64>());
        }
        if (matches != 1) return false;
    }
    return true;
}

bool acceptance_event_is(const Json& event, const Json& order) {
    if (order.is_null() || !event.at("payload").is_object()) return false;
    const Json expected_payload = {
        {"account_id", order.at("account_id")}, {"client_order_id", order.at("client_order_id")},
        {"symbol", order.at("symbol")}, {"quantity", order.at("quantity")},
        {"limit_price_cent", order.at("limit_price_cent")}, {"reserved_cent", order.at("reserved_cent")},
        {"state", static_cast<int>(trade::ORDER_STATE_LOCAL_ACCEPTED)}
    };
    const auto& payload = event.at("payload");
    for (const auto& item : expected_payload.items()) {
        if (!payload.contains(item.key()) || payload.at(item.key()) != item.value()) return false;
    }
    // Later fixture state changes must not rewrite the original version-1 snapshot.
    return event.at("event_id").get<U64>() != 0 && event.at("order_id") == order.at("order_id") &&
           event.at("order_version").get<U64>() == 1 && event.at("event_type").get<U64>() == 1 &&
           event.at("publish_state").get<U64>() == 0 && event.at("published_at").is_null() &&
           !event.at("created_at").get<std::string>().empty();
}

bool outbox_matches_orders(const Json& events, const Json& orders) {
    if (events.size() != orders.size()) return false;
    for (const auto& order : orders) {
        int matches = 0;
        for (const auto& event : events) matches += acceptance_event_is(event, order);
        if (matches != 1) return false;
    }
    return true;
}

bool cancellation_event_is(const Json& event, const Json& original_order) {
    if (!event.at("payload").is_object()) return false;
    const Json expected_payload = {
        {"account_id", original_order.at("account_id")}, {"client_order_id", original_order.at("client_order_id")},
        {"symbol", original_order.at("symbol")}, {"quantity", original_order.at("quantity")},
        {"limit_price_cent", original_order.at("limit_price_cent")},
        {"reserved_cent", original_order.at("reserved_cent")},
        {"state", static_cast<int>(trade::ORDER_STATE_CANCELLED)}
    };
    const auto& payload = event.at("payload");
    for (const auto& item : expected_payload.items()) {
        if (!payload.contains(item.key()) || payload.at(item.key()) != item.value()) return false;
    }
    return event.at("event_id").get<U64>() != 0 && event.at("order_id") == original_order.at("order_id") &&
           event.at("order_version").get<U64>() == original_order.at("version").get<U64>() + 1 &&
           event.at("event_type").get<U64>() == 2 && event.at("publish_state").get<U64>() == 0 &&
           event.at("published_at").is_null() && !event.at("created_at").get<std::string>().empty();
}

int write_fault_acceptance(const std::string& report_path) {
    Json report{{"database", database_name()}, {"checks", Json::array()},
                {"scope", "M1.5 direct SubmitOrder ledger/outbox INSERT failure and recovery; pool size 1"},
                {"notes", Json::array({
                    "CHECK constraints are installed only in the disposable test schema.",
                    "A direct SQL probe verifies MySQL error 3819 before each RPC fault.",
                    "A locking read waits for the failed order transaction to end before snapshots.",
                    "These non-duplicate INSERT failures do not cover error 1062 or COMMIT response loss.",
                    "An unchanged account is not evidence of reversing an UPDATE: account UPDATE follows both INSERTs.",
                    "AUTO_INCREMENT gaps are expected and are not reset."})}};
    auto check = [&](const std::string& name, bool pass, Json details = Json::object()) {
        report["checks"].push_back({{"name", name}, {"pass", pass}, {"details", std::move(details)}});
        std::cerr << (pass ? "PASS " : "FAIL ") << name << '\n';
    };
    try {
        Database db;
        db.require_write_fault_database();
        const auto baseline = db.snapshot_all();
        if (!std::all_of(baseline.begin(), baseline.end(), [](const Json& rows) { return rows.empty(); })) {
            throw std::runtime_error("Write-fault mode requires four empty disposable tables");
        }
        const std::string prefix = "accept_fault_" + std::to_string(
            std::chrono::system_clock::now().time_since_epoch().count());
        for (bool ledger : {true, false}) {
            const std::string scenario = ledger ? "ledger_insert" : "outbox_insert";
            const auto fixture = db.create(prefix + "_" + scenario);
            const auto key = prefix + "_" + scenario;
            const auto before = db.snapshot_all();
            bool fault_enabled = false;
            try {
                db.set_write_fault(ledger, true);
                fault_enabled = true;
                const bool enforced = db.write_fault_is_enforced(ledger, fixture.id);
                check(scenario + "_constraint_rejects_direct_insert_with_3819", enforced);
                if (!enforced) throw std::runtime_error("Fault constraint did not reject the probe");

                const auto failed_rpc = submit(fixture.id, key, 2, 1500);
                check(scenario + "_rpc_returns_internal", failed_rpc.code == grpc::StatusCode::INTERNAL,
                      rpc_json(failed_rpc));
                db.wait_for_order_transaction_end(fixture.id, key);
                check(scenario + "_failed_transaction_ended", true);
                const auto after = db.snapshot_all();
                check(scenario + "_account_unchanged", after.at("accounts") == before.at("accounts"));
                check(scenario + "_orders_rolled_back", after.at("orders") == before.at("orders"));
                check(scenario + "_ledger_rolled_back", after.at("fund_ledger") == before.at("fund_ledger"));
                check(scenario + "_outbox_rolled_back", after.at("outbox_events") == before.at("outbox_events"));
                report[scenario] = {{"failed_rpc", rpc_json(failed_rpc)}, {"before", before}, {"after", after}};

                db.set_write_fault(ledger, false);
                fault_enabled = false;
                const auto retry = submit(fixture.id, key, 2, 1500);
                const auto stored = db.order(fixture.id, key);
                const auto entries = db.ledger_for(fixture.id);
                const auto events = db.outbox_for(fixture.id);
                check(scenario + "_same_key_retry_succeeds_and_pool_is_reusable",
                      state_is(retry, trade::ORDER_STATE_LOCAL_ACCEPTED) &&
                      new_order_is(stored, retry, fixture.id, key, 2, 1500), rpc_json(retry));
                check(scenario + "_retry_freezes_once", money_is(db.account(fixture.id), 7000, 3000));
                check(scenario + "_retry_has_exactly_one_ledger", entries.size() == 1 &&
                      freeze_entry_is(entries[0], fixture.id, retry.response.order_id(), 3000), entries);
                check(scenario + "_retry_has_exactly_one_outbox", events.size() == 1 &&
                      acceptance_event_is(events[0], stored), events);
                const auto committed = db.snapshot_all();
                const auto duplicate = submit(fixture.id, key, 2, 1500);
                check(scenario + "_further_retry_is_idempotent", state_is(duplicate, trade::ORDER_STATE_LOCAL_ACCEPTED) &&
                      duplicate.response.order_id() == retry.response.order_id() && db.snapshot_all() == committed,
                      rpc_json(duplicate));
                report[scenario]["recovered_rpc"] = rpc_json(retry);
            } catch (...) {
                if (fault_enabled) db.set_write_fault(ledger, false);
                throw;
            }
            db.erase(fixture);
            check(scenario + "_fixture_cleaned", db.snapshot_all() == baseline);
        }
        check("all_disposable_data_removed", db.snapshot_all() == baseline);
    } catch (const std::exception& ex) {
        check("write_fault_setup_or_execution", false, {{"error", ex.what()}});
    }
    int failed = 0;
    for (const auto& item : report["checks"]) if (!item["pass"].get<bool>()) ++failed;
    report["failed"] = failed;
    report["total"] = report["checks"].size();
    std::ofstream file(report_path);
    file << report.dump(2) << '\n';
    file.close();
    if (!file) return 2;
    std::cout << "SUMMARY M1.5 " << report["total"] << " checks, " << failed << " failed\n";
    std::cout << "REPORT " << report_path << '\n';
    return failed == 0 ? 0 : 1;
}

int verify_database_target() {
    try {
        Database db;
        db.require_write_fault_database();
        const auto fixture = db.create("accept_target_" + std::to_string(
            std::chrono::system_clock::now().time_since_epoch().count()), 17);
        auto stub = trade::TradeAccount::NewStub(
            grpc::CreateChannel("127.0.0.1:13401", grpc::InsecureChannelCredentials()));
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(6));
        trade::GetTradeAccountRequest request;
        request.set_account_id(fixture.id);
        trade::GetTradeAccountResponse response;
        const auto status = stub->GetAccount(&context, request, &response);
        const bool matched = status.ok() && response.account_id() == fixture.id &&
                             response.account_name() == fixture.name && response.available_cent() == 17;
        db.erase(fixture);
        if (!matched) throw std::runtime_error("Server did not read the isolated sentinel; no write RPCs were sent");
        std::cout << "PASS server_and_runner_use_same_disposable_database\n";
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << "Database target verification failed: " << ex.what() << '\n';
        return 2;
    }
}

Json proxy_command(Json request) {
    boost::asio::io_context ioc;
    boost::asio::ip::tcp::socket socket(ioc);
    socket.connect({boost::asio::ip::make_address("127.0.0.1"), 13308});
    const auto message = request.dump() + "\n";
    boost::asio::write(socket, boost::asio::buffer(message));
    std::string reply;
    boost::asio::read_until(socket, boost::asio::dynamic_buffer(reply, 1024 * 1024), '\n');
    auto result = Json::parse(reply);
    if (!result.value("ok", false)) throw std::runtime_error(result.dump());
    return result;
}

Json wait_for_proxy_event(const std::string& event) {
    const auto until = std::chrono::steady_clock::now() + std::chrono::seconds(4);
    do {
        auto status = proxy_command({{"command", "status"}});
        for (const auto& item : status.at("events")) if (item.at("event") == event) return status;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    } while (std::chrono::steady_clock::now() < until);
    throw std::runtime_error("Missing proxy event: " + event);
}

Json get_order_rpc(U64 account_id, const std::string& client_id) {
    auto stub = trade::TradeAccount::NewStub(
        grpc::CreateChannel("127.0.0.1:13401", grpc::InsecureChannelCredentials()));
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(6));
    trade::GetOrderRequest request;
    request.set_account_id(account_id);
    request.set_client_order_id(client_id);
    trade::GetOrderResponse response;
    const auto status = stub->GetOrder(&context, request, &response);
    return {{"grpc_code", static_cast<int>(status.error_code())}, {"order_id", response.order_id()},
            {"state", static_cast<int>(response.state())}};
}

int boundary_acceptance(const std::string& mode, const std::string& report_path, int parameter) {
    Json report{{"database", database_name()}, {"mode", mode}, {"checks", Json::array()},
                {"scope", "Actual Service/Reactor/Repository reused by a test-only startup harness"}};
    auto check = [&](const std::string& name, bool pass, Json details = Json::object()) {
        report["checks"].push_back({{"name", name}, {"pass", pass}, {"details", std::move(details)}});
        std::cerr << (pass ? "PASS " : "FAIL ") << name << '\n';
    };
    std::future<RpcResult> pending;
    try {
        Database db, observer;
        db.require_write_fault_database();
        const auto baseline = db.snapshot_all();
        const std::string prefix = "accept_boundary_" + std::to_string(
            std::chrono::system_clock::now().time_since_epoch().count());
        auto one_order_is_atomic = [&](const Fixture& fixture, const std::string& key) {
            const auto order = db.order(fixture.id, key);
            if (order.is_null()) return money_is(db.account(fixture.id), 10000, 0) &&
                                       db.ledger_for(fixture.id).empty() && db.outbox_for(fixture.id).empty();
            return money_is(db.account(fixture.id), 7000, 3000) &&
                   db.orders_for(fixture.id).size() == 1 &&
                   ledger_matches_orders(db.ledger_for(fixture.id), db.orders_for(fixture.id)) &&
                   outbox_matches_orders(db.outbox_for(fixture.id), db.orders_for(fixture.id));
        };
        if (mode == "commit") {
            for (const std::string fault : {"before_commit", "drop_commit_ack"}) {
                const auto fixture = db.create(prefix + "_" + fault);
                const auto key = prefix + "_" + fault;
                const auto before = db.snapshot_all();
                proxy_command({{"command", "arm"}, {"mode", fault}, {"case", fault}});
                const auto rpc = submit(fixture.id, key, 2, 1500);
                const auto evidence = wait_for_proxy_event(fault == "before_commit"
                    ? "commit_not_forwarded" : "commit_ok_dropped");
                check(fault + "_exact_fault_point_observed", true, evidence);
                check(fault + "_rpc_reports_error_not_false_success", rpc.code == grpc::StatusCode::INTERNAL,
                      rpc_json(rpc));
                db.wait_for_order_transaction_end(fixture.id, key);
                const auto order = db.order(fixture.id, key);
                if (fault == "before_commit") {
                    check("disconnect_before_commit_rolls_back_all_four_tables", db.snapshot_all() == before);
                } else {
                    check("lost_commit_ok_still_has_one_durable_order", !order.is_null() && one_order_is_atomic(fixture, key),
                          {{"order", order}, {"account", db.account(fixture.id)}});
                    const auto queried = get_order_rpc(fixture.id, key);
                    check("lost_commit_ok_can_be_confirmed_by_business_key", queried.at("grpc_code") == 0 &&
                          queried.at("order_id") == order.at("order_id"), queried);
                }
                const auto recovered = submit(fixture.id, key, 2, 1500);
                check(fault + "_same_key_retry_is_successful", state_is(recovered, trade::ORDER_STATE_LOCAL_ACCEPTED) &&
                      one_order_is_atomic(fixture, key) && (order.is_null() ||
                      recovered.response.order_id() == order.at("order_id").get<U64>()), rpc_json(recovered));
                const auto committed = db.snapshot_all();
                const auto duplicate = submit(fixture.id, key, 2, 1500);
                check(fault + "_further_retry_does_not_repeat_side_effects", state_is(duplicate, trade::ORDER_STATE_LOCAL_ACCEPTED) &&
                      duplicate.response.order_id() == recovered.response.order_id() && db.snapshot_all() == committed);
                db.erase(fixture);
            }
        } else if (mode == "concurrency") {
            if (parameter != 2 && parameter != 4) throw std::runtime_error("Expected pool size 2 or 4");
            std::vector<Fixture> fixtures;
            for (int i = 0; i < parameter; ++i) fixtures.push_back(db.create(prefix + "_" + std::to_string(i)));
            db.lock_accounts(fixtures);
            std::vector<std::future<RpcResult>> calls;
            Json active;
            try {
                for (const auto& fixture : fixtures) {
                    calls.push_back(std::async(std::launch::async, [fixture, prefix] {
                        return submit(fixture.id, prefix, 2, 1500);
                    }));
                }
                const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(1500);
                do {
                    active = observer.active_account_updates();
                    if (active.size() == static_cast<std::size_t>(parameter)) break;
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                } while (std::chrono::steady_clock::now() < until);
            } catch (...) {
                // Release SQL locks before future destructors wait for their RPCs.
                db.release_locks();
                throw;
            }
            db.release_locks();
            check("pool_connections_really_execute_concurrent_transactions",
                  active.size() == static_cast<std::size_t>(parameter), {{"pool_size", parameter}, {"active_updates", active}});
            bool all_ok = true;
            for (std::size_t i = 0; i < fixtures.size(); ++i) {
                const auto rpc = calls[i].get();
                all_ok = all_ok && state_is(rpc, trade::ORDER_STATE_LOCAL_ACCEPTED) &&
                         one_order_is_atomic(fixtures[i], prefix);
            }
            check("all_overlapping_transactions_commit_atomically", all_ok);
            for (const auto& fixture : fixtures) db.erase(fixture);
        } else if (mode == "server-budget") {
            report["policy"] = "Started writes use a server-owned 3s budget; RPC cancellation is not CancelOrder";
            auto wait_for_blocked_update = [&] {
                Json active;
                const auto until = std::chrono::steady_clock::now() + std::chrono::milliseconds(1500);
                do {
                    active = observer.active_account_updates();
                    if (!active.empty()) break;
                    std::this_thread::sleep_for(std::chrono::milliseconds(10));
                } while (std::chrono::steady_clock::now() < until);
                return active;
            };
            auto fixture_snapshot = [&](const Fixture& fixture) {
                return Json{{"account", db.account(fixture.id)}, {"orders", db.orders_for(fixture.id)},
                            {"ledger", db.ledger_for(fixture.id)}, {"outbox", db.outbox_for(fixture.id)}};
            };

            for (const bool explicit_cancel : {false, true}) {
                const std::string kind = explicit_cancel ? "trycancel" : "client_deadline";
                const auto fixture = db.create(prefix + "_" + kind);
                const auto key = prefix + "_" + kind;
                db.lock_accounts({fixture});
                auto context = std::make_shared<grpc::ClientContext>();
                context->set_deadline(std::chrono::system_clock::now() +
                                     std::chrono::milliseconds(explicit_cancel ? 6000 : 300));
                const auto start = std::chrono::steady_clock::now();
                pending = std::async(std::launch::async, [=] {
                    return submit_with_context(fixture.id, key, 2, 1500, "DEMO_A", *context);
                });
                const auto blocked = wait_for_blocked_update();
                check(kind + "_reaches_account_update_before_cancellation", !blocked.empty(), blocked);
                if (blocked.empty()) throw std::runtime_error("Cancellation fixture did not reach the SQL barrier");
                if (explicit_cancel) context->TryCancel();
                const auto rpc = pending.get();
                check(kind + "_client_observes_transport_cancellation",
                      rpc.code == (explicit_cancel ? grpc::StatusCode::CANCELLED : grpc::StatusCode::DEADLINE_EXCEEDED),
                      rpc_json(rpc));
                std::this_thread::sleep_until(start + std::chrono::milliseconds(700));
                const auto active = observer.active_account_updates();
                check(kind + "_does_not_abort_started_write_at_client_deadline", !active.empty(), active);
                db.release_locks();
                db.wait_for_order_transaction_end(fixture.id, key);
                const auto order = db.order(fixture.id, key);
                check(kind + "_completion_inside_server_budget_commits_without_refund",
                      !order.is_null() && order.at("state") == static_cast<int>(trade::ORDER_STATE_LOCAL_ACCEPTED) &&
                      one_order_is_atomic(fixture, key), fixture_snapshot(fixture));
                const auto queried = get_order_rpc(fixture.id, key);
                check(kind + "_can_confirm_order_by_original_business_key", !order.is_null() &&
                      queried.at("grpc_code") == 0 && queried.at("order_id") == order.at("order_id"), queried);
                const auto before_retry = fixture_snapshot(fixture);
                const auto retry = submit(fixture.id, key, 2, 1500);
                check(kind + "_same_key_retry_has_no_additional_side_effects", !order.is_null() &&
                      state_is(retry, trade::ORDER_STATE_LOCAL_ACCEPTED) &&
                      retry.response.order_id() == order.at("order_id").get<U64>() &&
                      fixture_snapshot(fixture) == before_retry, rpc_json(retry));
                db.erase(fixture);
            }

            const auto fixture = db.create(prefix + "_server_timeout");
            const auto key = prefix + "_server_timeout";
            const auto before = fixture_snapshot(fixture);
            db.lock_accounts({fixture});
            auto start = std::chrono::steady_clock::now();
            pending = std::async(std::launch::async, [=] { return submit(fixture.id, key, 2, 1500); });
            const auto blocked = wait_for_blocked_update();
            check("server_budget_timeout_reaches_blocked_update", !blocked.empty(), blocked);
            const bool completed = pending.wait_until(start + std::chrono::milliseconds(3500)) == std::future_status::ready;
            check("server_budget_ends_local_sql_wait_before_lock_is_released", completed,
                  {{"observed_ms", std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - start).count()},
                   {"remote_updates", observer.active_account_updates()}});
            // Socket cancellation need not kill the remote query; release our lock before pool reset is awaited.
            db.release_locks();
            const auto expired = pending.get();
            check("server_budget_timeout_is_not_reported_as_success", expired.code == grpc::StatusCode::INTERNAL,
                  rpc_json(expired));
            db.wait_for_order_transaction_end(fixture.id, key);
            check("uncommitted_timed_out_write_eventually_rolls_back_all_four_tables",
                  fixture_snapshot(fixture) == before, fixture_snapshot(fixture));
            const auto recovered = submit(fixture.id, key, 2, 1500);
            check("server_budget_timeout_same_key_can_retry_on_recovered_pool",
                  state_is(recovered, trade::ORDER_STATE_LOCAL_ACCEPTED) && one_order_is_atomic(fixture, key),
                  rpc_json(recovered));
            db.erase(fixture);
        } else if (mode == "server-budget-commit") {
            report["policy"] = "A server SQL timeout does not imply that COMMIT failed";
            const auto committed_fixture = db.create(prefix + "_commit_timeout");
            const auto committed_key = prefix + "_commit_timeout";
            proxy_command({{"command", "arm"}, {"mode", "hold_commit_ack"}, {"case", "server_budget_commit"}});
            const auto start = std::chrono::steady_clock::now();
            pending = std::async(std::launch::async, [=] { return submit(committed_fixture.id, committed_key, 2, 1500); });
            const auto evidence = wait_for_proxy_event("commit_ok_held");
            check("server_budget_commit_timeout_has_confirmed_mysql_ok", true, evidence);
            const bool commit_completed = pending.wait_until(start + std::chrono::milliseconds(3500)) == std::future_status::ready;
            check("server_budget_ends_local_commit_wait_before_ack_is_released", commit_completed,
                  {{"observed_ms", std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - start).count()}, {"proxy", proxy_command({{"command", "status"}})}});
            proxy_command({{"command", "release"}});
            const auto commit_timeout = pending.get();
            check("server_budget_unknown_commit_is_not_reported_as_success",
                  commit_timeout.code == grpc::StatusCode::INTERNAL, rpc_json(commit_timeout));
            const auto order = db.order(committed_fixture.id, committed_key);
            check("server_budget_does_not_reverse_an_already_committed_order", !order.is_null() &&
                  one_order_is_atomic(committed_fixture, committed_key), db.snapshot_all());
            const auto queried = get_order_rpc(committed_fixture.id, committed_key);
            check("server_budget_unknown_commit_can_be_confirmed_by_business_key", !order.is_null() &&
                  queried.at("grpc_code") == 0 && queried.at("order_id") == order.at("order_id"), queried);
            const auto before_retry = db.snapshot_all();
            const auto retry = submit(committed_fixture.id, committed_key, 2, 1500);
            check("server_budget_unknown_commit_retry_does_not_refreeze", !order.is_null() &&
                  state_is(retry, trade::ORDER_STATE_LOCAL_ACCEPTED) &&
                  retry.response.order_id() == order.at("order_id").get<U64>() &&
                  db.snapshot_all() == before_retry, rpc_json(retry));
            db.erase(committed_fixture);
        } else if (mode == "cancel") {
            const auto fixture = db.create(prefix + "_precommit_cancel");
            const auto key = prefix + "_precommit_cancel";
            db.lock_accounts({fixture});
            pending = std::async(std::launch::async, [=] { return submit(fixture.id, key, 2, 1500, "DEMO_A", 300); });
            Json active;
            for (int i = 0; i < 20; ++i) {
                active = observer.active_account_updates();
                if (!active.empty()) break;
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            check("precommit_cancel_reaches_blocked_account_update", !active.empty(), active);
            const auto rpc = pending.get();
            check("precommit_cancel_client_observes_deadline", rpc.code == grpc::StatusCode::DEADLINE_EXCEEDED, rpc_json(rpc));
            std::this_thread::sleep_for(std::chrono::milliseconds(400));
            active = observer.active_account_updates();
            check("expired_write_stops_waiting_on_sql_within_remaining_budget", active.empty(), active);
            db.release_locks();
            db.wait_for_order_transaction_end(fixture.id, key);
            check("precommit_cancel_eventual_data_is_atomic_not_assumed_rollback", one_order_is_atomic(fixture, key));
            const auto recovered = submit(fixture.id, key, 2, 1500);
            check("precommit_cancel_same_key_recovers_without_double_freeze", state_is(recovered, trade::ORDER_STATE_LOCAL_ACCEPTED) &&
                  one_order_is_atomic(fixture, key), rpc_json(recovered));
            db.erase(fixture);

            const auto committed_fixture = db.create(prefix + "_postcommit_cancel");
            const auto committed_key = prefix + "_postcommit_cancel";
            proxy_command({{"command", "arm"}, {"mode", "hold_commit_ack"}, {"case", "postcommit_cancel"}});
            pending = std::async(std::launch::async, [=] {
                return submit(committed_fixture.id, committed_key, 2, 1500, "DEMO_A", 300);
            });
            const auto evidence = wait_for_proxy_event("commit_ok_held");
            const auto cancelled = pending.get();
            check("postcommit_cancel_client_observes_deadline", cancelled.code == grpc::StatusCode::DEADLINE_EXCEEDED,
                  {{"rpc", rpc_json(cancelled)}, {"proxy", evidence}});
            check("postcommit_deadline_does_not_undo_committed_order", one_order_is_atomic(committed_fixture, committed_key) &&
                  !db.order(committed_fixture.id, committed_key).is_null());
            std::this_thread::sleep_for(std::chrono::milliseconds(400));
            const auto waiting = proxy_command({{"command", "status"}});
            check("expired_call_releases_commit_ack_wait_within_remaining_budget", waiting.at("held") == 0, waiting);
            proxy_command({{"command", "release"}});
            const auto queried = get_order_rpc(committed_fixture.id, committed_key);
            check("late_completion_keeps_service_queryable", queried.at("grpc_code") == 0, queried);
            const auto retry = submit(committed_fixture.id, committed_key, 2, 1500);
            check("postcommit_cancel_same_key_retry_preserves_one_order", state_is(retry, trade::ORDER_STATE_LOCAL_ACCEPTED) &&
                  one_order_is_atomic(committed_fixture, committed_key), rpc_json(retry));
            db.erase(committed_fixture);
        } else if (mode == "shutdown") {
            const auto executable = std::filesystem::read_symlink("/proc/" + std::to_string(parameter) + "/exe");
            if (parameter <= 1 || executable.filename() != "trade_fault_server") {
                throw std::runtime_error("Refusing to signal a non-test server");
            }
            const auto fixture = db.create(prefix + "_shutdown");
            const auto key = prefix + "_shutdown";
            proxy_command({{"command", "arm"}, {"mode", "hold_commit_ack"}, {"case", "shutdown"}});
            pending = std::async(std::launch::async, [=] { return submit(fixture.id, key, 2, 1500, "DEMO_A", 10000); });
            const auto evidence = wait_for_proxy_event("commit_ok_held");
            const auto start = std::chrono::steady_clock::now();
            if (::kill(parameter, SIGTERM) != 0) throw std::runtime_error("Could not signal owned test server");
            while (::kill(parameter, 0) == 0 && std::chrono::steady_clock::now() - start < std::chrono::milliseconds(3500)) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            check("shutdown_drains_pending_sql_within_3s_grace_plus_500ms", ::kill(parameter, 0) != 0,
                  {{"proxy", evidence}, {"observed_ms", std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::steady_clock::now() - start).count()}});
            proxy_command({{"command", "release"}});
            const auto rpc = pending.get();
            check("shutdown_client_has_non_ok_status", rpc.code != grpc::StatusCode::OK, rpc_json(rpc));
            for (int i = 0; i < 200 && ::kill(parameter, 0) == 0; ++i) std::this_thread::sleep_for(std::chrono::milliseconds(10));
            check("server_can_exit_after_dependency_is_released", ::kill(parameter, 0) != 0);
            check("shutdown_does_not_reverse_a_committed_order", one_order_is_atomic(fixture, key) &&
                  !db.order(fixture.id, key).is_null());
            db.erase(fixture);
        } else {
            throw std::runtime_error("Unknown boundary mode");
        }
        check("boundary_fixtures_cleaned", db.snapshot_all() == baseline);
    } catch (const std::exception& ex) {
        if (mode != "concurrency") {
            try { proxy_command({{"command", "release"}}); } catch (...) {}
        }
        if (pending.valid()) pending.wait();
        check("boundary_setup_or_execution", false, {{"error", ex.what()}});
    }
    int failed = 0;
    for (const auto& item : report["checks"]) if (!item["pass"].get<bool>()) ++failed;
    report["failed"] = failed;
    report["total"] = report["checks"].size();
    std::ofstream file(report_path);
    file << report.dump(2) << '\n';
    file.close();
    if (!file) return 2;
    std::cout << "SUMMARY " << mode << " " << report["total"] << " checks, " << failed << " failed\n";
    return failed == 0 ? 0 : 1;
}

int main(int argc, char** argv) {
    if (argc == 5 && std::string(argv[1]) == "--boundaries") {
        try { return boundary_acceptance(argv[2], argv[3], std::stoi(argv[4])); }
        catch (const std::exception& ex) { std::cerr << ex.what() << '\n'; return 2; }
    }
    if ((argc == 2 || argc == 3) && std::string(argv[1]) == "--admin-sql") {
        return admin_sql(argc == 3 ? argv[2] : "");
    }
    if (argc == 3 && std::string(argv[1]) == "--snapshot") {
        try {
            Database db;
            std::ofstream file(argv[2]);
            file << Json({{"database", database_name()}, {"tables", db.snapshot_all()}}).dump(2) << '\n';
            file.close();
            return file ? 0 : 2;
        } catch (const std::exception& ex) {
            std::cerr << "Read-only snapshot failed: " << ex.what() << '\n';
            return 2;
        }
    }
    if (argc == 3 && std::string(argv[1]) == "--write-faults") {
        return write_fault_acceptance(argv[2]);
    }
    if (argc == 2 && std::string(argv[1]) == "--verify-database-target") {
        return verify_database_target();
    }
    if (argc == 3 && std::string(argv[1]) == "--cleanup") {
        try {
            std::ifstream file(argv[2]);
            const auto manifest = Json::parse(file);
            Database db;
            for (const auto& row : manifest) {
                Fixture fixture{row.at("id").get<U64>(), row.at("name").get<std::string>()};
                if (!fixture.name.starts_with("accept_")) throw std::runtime_error("Invalid fixture name");
                db.erase(fixture);
            }
            return 0;
        } catch (const std::exception& ex) {
            std::cerr << "Fixture cleanup failed: " << ex.what() << '\n';
            return 1;
        }
    }
    if (argc != 2) return 2;
    const std::string report_path = argv[1];
    const std::string prefix = "accept_" + std::to_string(
        std::chrono::system_clock::now().time_since_epoch().count());
    const char* launcher_pool_size = std::getenv("TRADE_TEST_POOL_SIZE");
    Json report{{"database", database_name()},
                {"launcher_pool_size", launcher_pool_size ? launcher_pool_size : "unspecified; production default is 1"},
                {"checks", Json::array()}, {"run_prefix", prefix},
                {"scope", "Local SubmitOrder and pre-dispatch CancelOrder four-table correctness and idempotency"},
                {"notes", Json::array({
                    "Business rejections use gRPC status; successful replies carry a persisted order state.",
                    "Fault and state injection modify disposable fixtures only, not schema or original data.",
                    "Pool size is chosen by the launcher; overlapping SQL execution must be proven separately.",
                    "The deliberate choice to accept UNSPECIFIED is not overridden by these tests.",
                    "Account UPDATE failure tests rollback of the preceding order, ledger, and outbox INSERTs.",
                    "The duplicate-ledger check tests the database constraint, not the RPC ledger-INSERT error branch.",
                    "The duplicate-event check tests the database constraint, not the RPC outbox-INSERT error branch.",
                    "Direct SubmitOrder ledger/outbox INSERT failure injection remains outside this test run.",
                    "CancelOrder conflict injection preinserts release/event rows on disposable orders only.",
                    "CancelOrder forbidden states are injected fixtures, not an implemented broker dispatch flow.",
                    "Outbox tests verify persistence and immutable acceptance snapshots, not Kafka delivery or consumer processing.",
                    "Temporary rows are deleted, but AUTO_INCREMENT is not rewound."})}};
    auto check = [&](const std::string& name, bool pass, Json details = Json::object()) {
        report["checks"].push_back({{"name", name}, {"pass", pass}, {"details", std::move(details)}});
        std::cerr << (pass ? "PASS " : "FAIL ") << name << '\n';
    };
    try {
        const auto manifest = std::filesystem::path(report_path).parent_path() / "fixtures.json";
        Database db(manifest.string());
        auto baseline = db.snapshot();
        auto baseline_orders = db.snapshot_orders();
        auto baseline_ledger = db.snapshot_ledger();
        auto baseline_outbox = db.snapshot_outbox();
        report["existing_accounts_before"] = baseline;
        report["existing_orders_before"] = baseline_orders;
        report["existing_ledger_before"] = baseline_ledger;
        report["existing_outbox_before"] = baseline_outbox;
        try {
            auto happy = db.create(prefix + "_happy");
            auto concurrent = db.create(prefix + "_concurrent");
            auto fault = db.create(prefix + "_fault");
            auto missing = db.create(prefix + "_missing");
            auto exact = db.create(prefix + "_exact");
            auto replay = db.create(prefix + "_replay");
            auto states = db.create(prefix + "_states");
            auto cross_a = db.create(prefix + "_cross_a");
            auto cross_b = db.create(prefix + "_cross_b");
            auto case_account = db.create(prefix + "_case");
            auto empty_id = db.create(prefix + "_empty_id");
            auto escaping = db.create(prefix + "_escaping");
            report["fixtures"] = Json::array();
            for (const auto& f : db.fixtures) report["fixtures"].push_back({{"id", f.id}, {"name", f.name}});
            check("fixture_missing_account_removed", db.erase(missing) == 1);

            auto result = submit(happy.id, prefix + "_normal", 2, 1500);
            auto after = db.account(happy.id);
            auto stored = db.order(happy.id, prefix + "_normal");
            const auto original_id = result.response.order_id();
            check("normal_order_id_state_version_and_freeze_commit", state_is(result, trade::ORDER_STATE_LOCAL_ACCEPTED) &&
                  money_is(after, 7000, 3000) && new_order_is(stored, result, happy.id, prefix + "_normal", 2, 1500),
                  {{"rpc", rpc_json(result)}, {"account", after}, {"order", stored}});
            auto entries = db.ledger_for(happy.id);
            check("normal_order_has_one_matching_freeze_entry", entries.size() == 1 &&
                  freeze_entry_is(entries[0], happy.id, original_id, 3000), {{"ledger", entries}});
            const auto original_entry = entries;
            auto events = db.outbox_for(happy.id);
            check("normal_order_has_one_matching_acceptance_event", events.size() == 1 &&
                  acceptance_event_is(events[0], stored), {{"outbox", events}});
            const auto original_event = events;
            const bool duplicate_event_rejected = events.size() == 1 && db.duplicate_event_is_rejected(events[0]);
            check("duplicate_order_and_version_is_rejected_by_database", duplicate_event_rejected &&
                  db.outbox_for(happy.id) == original_event);
            const bool duplicate_rejected = db.duplicate_freeze_is_rejected(happy, original_id, 3000);
            check("duplicate_order_and_entry_type_is_rejected_by_database", duplicate_rejected &&
                  db.ledger_for(happy.id) == original_entry);

            auto before = after;
            auto orders_before = db.orders_for(happy.id);
            result = submit(happy.id, prefix + "_normal", 2, 1500);
            after = db.account(happy.id);
            check("identical_retry_returns_same_order_without_freezing", state_is(result, trade::ORDER_STATE_LOCAL_ACCEPTED) &&
                  result.response.order_id() == original_id && after == before && db.orders_for(happy.id) == orders_before,
                  {{"rpc", rpc_json(result)}, {"account", after}});
            check("identical_retry_does_not_append_ledger", db.ledger_for(happy.id) == original_entry);
            check("identical_retry_does_not_append_outbox", db.outbox_for(happy.id) == original_event);

            struct ConflictCase { const char* name; U64 quantity, price; const char* symbol; };
            const ConflictCase conflicts[] = {
                {"same_key_different_price", 2, 1600, "DEMO_A"},
                {"same_key_different_quantity", 3, 1500, "DEMO_A"},
                {"same_key_different_symbol", 2, 1500, "DEMO_B"}
            };
            for (auto input : conflicts) {
                before = db.account(happy.id);
                orders_before = db.orders_for(happy.id);
                const auto ledger_before = db.ledger_for(happy.id);
                result = submit(happy.id, prefix + "_normal", input.quantity, input.price, input.symbol);
                after = db.account(happy.id);
                check(input.name, result.code == grpc::StatusCode::ALREADY_EXISTS && after == before &&
                      db.orders_for(happy.id) == orders_before && db.ledger_for(happy.id) == ledger_before &&
                      db.outbox_for(happy.id) == original_event,
                      {{"rpc", rpc_json(result)}, {"account", after}});
            }

            before = after;
            orders_before = db.orders_for(happy.id);
            result = submit(happy.id, prefix + "_insufficient", 1, 7001);
            after = db.account(happy.id);
            check("insufficient_balance_rolls_back_inserted_order", result.code == grpc::StatusCode::FAILED_PRECONDITION &&
                  after == before && db.orders_for(happy.id) == orders_before && db.ledger_for(happy.id) == original_entry &&
                  db.outbox_for(happy.id) == original_event,
                  {{"rpc", rpc_json(result)}, {"account", after}});

            result = submit(missing.id, prefix + "_missing_order", 1, 1);
            check("missing_account_does_not_leave_an_order", result.code == grpc::StatusCode::NOT_FOUND &&
                  db.account(missing.id).is_null() && db.orders_for(missing.id).empty() &&
                  db.ledger_for(missing.id).empty() && db.outbox_for(missing.id).empty(), rpc_json(result));

            result = submit(happy.id, prefix + "_reuse_after_rejection", 1, 1000);
            after = db.account(happy.id);
            check("pool_connection_reused_after_early_returns", state_is(result, trade::ORDER_STATE_LOCAL_ACCEPTED) &&
                  money_is(after, 6000, 4000) &&
                  new_order_is(db.order(happy.id, prefix + "_reuse_after_rejection"), result,
                               happy.id, prefix + "_reuse_after_rejection", 1, 1000),
                  {{"rpc", rpc_json(result)}, {"account", after}});

            struct InvalidCase { const char* name; U64 quantity, price; };
            const InvalidCase invalid[] = {
                {"zero_quantity", 0, 1000}, {"zero_price", 1, 0},
                {"both_zero", 0, 0},
                {"overflow_wraps_to_4", 4611686018427387905ULL, 4},
                {"overflow_wraps_to_2", 6148914691236517206ULL, 3},
                {"max_uint64_times_2", std::numeric_limits<U64>::max(), 2}
            };
            for (auto input : invalid) {
                before = db.account(happy.id);
                orders_before = db.orders_for(happy.id);
                const auto events_before = db.outbox_for(happy.id);
                result = submit(happy.id, prefix + input.name, input.quantity, input.price);
                after = db.account(happy.id);
                check(input.name, result.code == grpc::StatusCode::INVALID_ARGUMENT && after == before &&
                      db.orders_for(happy.id) == orders_before && db.outbox_for(happy.id) == events_before,
                      {{"rpc", rpc_json(result)}, {"account", after}});
            }

            result = submit(0, prefix + "_invalid_account", 1, 1);
            check("zero_account_id_grpc_invalid_argument", result.code == 3, rpc_json(result));

            result = submit(exact.id, prefix + "_exact_balance", 4, 2500);
            after = db.account(exact.id);
            const auto exact_id = result.response.order_id();
            check("exact_balance_can_be_fully_frozen", state_is(result, trade::ORDER_STATE_LOCAL_ACCEPTED) &&
                  money_is(after, 0, 10000) &&
                  new_order_is(db.order(exact.id, prefix + "_exact_balance"), result, exact.id,
                               prefix + "_exact_balance", 4, 2500), {{"rpc", rpc_json(result)}, {"account", after}});
            before = after;
            orders_before = db.orders_for(exact.id);
            result = submit(exact.id, prefix + "_exact_balance", 4, 2500);
            after = db.account(exact.id);
            check("retry_succeeds_even_when_available_balance_is_zero", state_is(result, trade::ORDER_STATE_LOCAL_ACCEPTED) &&
                  result.response.order_id() == exact_id && after == before && db.orders_for(exact.id) == orders_before,
                  {{"rpc", rpc_json(result)}, {"account", after}});
            result = submit(exact.id, prefix + "_empty_balance", 1, 1);
            after = db.account(exact.id);
            check("empty_balance_rejection_changes_nothing", result.code == grpc::StatusCode::FAILED_PRECONDITION &&
                  after == before && db.orders_for(exact.id) == orders_before,
                  {{"rpc", rpc_json(result)}, {"account", after}});

            std::vector<std::future<RpcResult>> pending;
            std::latch ready(8);
            for (int i = 0; i < 8; ++i) pending.push_back(std::async(std::launch::async, [&, i] {
                ready.arrive_and_wait();
                return submit(concurrent.id, prefix + "_concurrent_" + std::to_string(i), 2, 1500);
            }));
            int accepted = 0, rejected = 0;
            Json calls = Json::array();
            for (auto& future : pending) {
                auto rpc = future.get();
                accepted += state_is(rpc, trade::ORDER_STATE_LOCAL_ACCEPTED);
                rejected += rpc.code == grpc::StatusCode::FAILED_PRECONDITION;
                calls.push_back(rpc_json(rpc));
            }
            after = db.account(concurrent.id);
            check("eight_concurrent_requests_do_not_overfreeze", accepted == 3 && rejected == 5 &&
                  money_is(after, 1000, 9000) && db.orders_for(concurrent.id).size() == 3,
                  {{"accepted", accepted}, {"rejected", rejected},
                  {"rpc_results", calls}, {"account", after}, {"orders", db.orders_for(concurrent.id)}});
            entries = db.ledger_for(concurrent.id);
            check("eight_distinct_requests_leave_only_three_matching_ledger_entries", entries.size() == 3 &&
                  ledger_matches_orders(entries, db.orders_for(concurrent.id)), {{"ledger", entries}});
            events = db.outbox_for(concurrent.id);
            check("eight_distinct_requests_leave_three_matching_outbox_events", events.size() == 3 &&
                  outbox_matches_orders(events, db.orders_for(concurrent.id)), {{"outbox", events}});

            std::vector<std::future<RpcResult>> replay_pending;
            std::latch replay_ready(8);
            for (int i = 0; i < 8; ++i) replay_pending.push_back(std::async(std::launch::async, [&] {
                replay_ready.arrive_and_wait();
                return submit(replay.id, prefix + "_same_business_key", 2, 1500);
            }));
            calls = Json::array();
            bool same_id = true;
            U64 shared_id = 0;
            for (auto& future : replay_pending) {
                auto rpc = future.get();
                if (shared_id == 0) shared_id = rpc.response.order_id();
                same_id = same_id && state_is(rpc, trade::ORDER_STATE_LOCAL_ACCEPTED) &&
                          rpc.response.order_id() != 0 && rpc.response.order_id() == shared_id;
                calls.push_back(rpc_json(rpc));
            }
            after = db.account(replay.id);
            check("eight_identical_concurrent_requests_freeze_once", same_id &&
                  money_is(after, 7000, 3000) && db.orders_for(replay.id).size() == 1,
                  {{"rpc_results", calls}, {"account", after}, {"orders", db.orders_for(replay.id)}});
            entries = db.ledger_for(replay.id);
            check("eight_identical_requests_append_one_ledger_entry", entries.size() == 1 &&
                  freeze_entry_is(entries[0], replay.id, shared_id, 3000), {{"ledger", entries}});
            events = db.outbox_for(replay.id);
            check("eight_identical_requests_append_one_outbox_event", events.size() == 1 &&
                  outbox_matches_orders(events, db.orders_for(replay.id)), {{"outbox", events}});

            result = submit(states.id, prefix + "_state_replay", 2, 1500);
            const auto state_order_id = result.response.order_id();
            check("state_fixture_order_created", state_is(result, trade::ORDER_STATE_LOCAL_ACCEPTED) &&
                  new_order_is(db.order(states.id, prefix + "_state_replay"), result, states.id,
                               prefix + "_state_replay", 2, 1500), rpc_json(result));
            const auto original_state_event = db.outbox_for(states.id);
            db.set_order_state(states, state_order_id, trade::ORDER_STATE_BROKER_ACCEPTED);
            before = db.account(states.id);
            orders_before = db.orders_for(states.id);
            result = submit(states.id, prefix + "_state_replay", 2, 1500);
            check("replay_returns_persisted_state_not_a_constant", state_is(result, trade::ORDER_STATE_BROKER_ACCEPTED) &&
                  result.response.order_id() == state_order_id && db.account(states.id) == before &&
                  db.orders_for(states.id) == orders_before, {{"rpc", rpc_json(result)}, {"orders", orders_before}});

            db.set_order_state(states, state_order_id, 99);
            orders_before = db.orders_for(states.id);
            result = submit(states.id, prefix + "_state_replay", 2, 1500);
            check("unknown_persisted_state_is_not_treated_as_success", result.code == grpc::StatusCode::INTERNAL &&
                  db.account(states.id) == before && db.orders_for(states.id) == orders_before,
                  {{"rpc", rpc_json(result)}, {"orders", orders_before}});
            db.set_order_state(states, state_order_id, trade::ORDER_STATE_BROKER_ACCEPTED);
            result = submit(states.id, prefix + "_state_replay", 2, 1500);
            check("connection_reused_after_unknown_state", state_is(result, trade::ORDER_STATE_BROKER_ACCEPTED) &&
                  result.response.order_id() == state_order_id && db.account(states.id) == before, rpc_json(result));
            check("state_retries_preserve_original_acceptance_snapshot", original_state_event.size() == 1 &&
                  db.outbox_for(states.id) == original_state_event &&
                  outbox_matches_orders(original_state_event, db.orders_for(states.id)),
                  {{"outbox", db.outbox_for(states.id)}});

            // The account UPDATE fails after all three INSERTs; none may survive.
            db.set_frozen(fault, std::numeric_limits<U64>::max());
            before = db.account(fault.id);
            orders_before = db.orders_for(fault.id);
            const auto ledger_before_fault = db.ledger_for(fault.id);
            const auto outbox_before_fault = db.outbox_for(fault.id);
            result = submit(fault.id, prefix + "_injected_second_update_error", 2, 1500);
            db.wait_for_transaction_end(fault);
            after = db.account(fault.id);
            check("account_sql_error_rolls_back_pending_order", result.code == grpc::StatusCode::INTERNAL &&
                  after == before && db.orders_for(fault.id) == orders_before,
                  {{"rpc", rpc_json(result)}, {"before", before}, {"after", after},
                  {"orders", db.orders_for(fault.id)}});
            check("account_sql_error_rolls_back_pending_ledger", result.code == grpc::StatusCode::INTERNAL &&
                  db.ledger_for(fault.id) == ledger_before_fault, {{"ledger", db.ledger_for(fault.id)}});
            check("account_sql_error_rolls_back_pending_outbox", result.code == grpc::StatusCode::INTERNAL &&
                  db.outbox_for(fault.id) == outbox_before_fault, {{"outbox", db.outbox_for(fault.id)}});

            db.set_frozen(fault, 0);
            result = submit(fault.id, prefix + "_injected_second_update_error", 2, 1500);
            after = db.account(fault.id);
            check("same_key_can_retry_after_failed_transaction", state_is(result, trade::ORDER_STATE_LOCAL_ACCEPTED) &&
                  money_is(after, 7000, 3000) && new_order_is(db.order(fault.id,
                      prefix + "_injected_second_update_error"), result, fault.id,
                      prefix + "_injected_second_update_error", 2, 1500),
                  {{"rpc", rpc_json(result)}, {"account", after}});
            entries = db.ledger_for(fault.id);
            check("retry_after_account_error_has_one_matching_freeze_entry", entries.size() == 1 &&
                  freeze_entry_is(entries[0], fault.id, result.response.order_id(), 3000), {{"ledger", entries}});
            events = db.outbox_for(fault.id);
            check("retry_after_account_error_has_one_acceptance_event", events.size() == 1 &&
                  outbox_matches_orders(events, db.orders_for(fault.id)), {{"outbox", events}});

            const std::string escaped_symbol = "DEMO_\"'\\\n";
            const auto escaped_result = submit(escaping.id, prefix + "_escaped_payload", 2, 1500, escaped_symbol);
            const auto escaped_order = db.order(escaping.id, prefix + "_escaped_payload");
            events = db.outbox_for(escaping.id);
            check("payload_escaping_round_trips_through_mysql_json", state_is(escaped_result, trade::ORDER_STATE_LOCAL_ACCEPTED) &&
                  !escaped_order.is_null() && escaped_order.at("order_id") == escaped_result.response.order_id() &&
                  escaped_order.at("symbol") == escaped_symbol && money_is(db.account(escaping.id), 7000, 3000) &&
                  events.size() == 1 && acceptance_event_is(events[0], escaped_order),
                  {{"rpc", rpc_json(escaped_result)}, {"order", escaped_order}, {"outbox", events}});

            auto cross_result_a = submit(cross_a.id, prefix + "_cross_account_key", 1, 1000);
            auto cross_result_b = submit(cross_b.id, prefix + "_cross_account_key", 1, 1000);
            check("different_accounts_may_use_the_same_client_order_id",
                  state_is(cross_result_a, trade::ORDER_STATE_LOCAL_ACCEPTED) &&
                  state_is(cross_result_b, trade::ORDER_STATE_LOCAL_ACCEPTED) &&
                  cross_result_a.response.order_id() != cross_result_b.response.order_id() &&
                  new_order_is(db.order(cross_a.id, prefix + "_cross_account_key"), cross_result_a,
                               cross_a.id, prefix + "_cross_account_key", 1, 1000) &&
                  new_order_is(db.order(cross_b.id, prefix + "_cross_account_key"), cross_result_b,
                               cross_b.id, prefix + "_cross_account_key", 1, 1000) &&
                  money_is(db.account(cross_a.id), 9000, 1000) && money_is(db.account(cross_b.id), 9000, 1000),
                  {{"account_a_rpc", rpc_json(cross_result_a)}, {"account_b_rpc", rpc_json(cross_result_b)}});

            auto case_result_a = submit(case_account.id, prefix + "_CaseKey", 2, 1500);
            auto case_result_b = submit(case_account.id, prefix + "_casekey", 2, 1500);
            after = db.account(case_account.id);
            auto case_orders = db.orders_for(case_account.id);
            check("business_ids_are_case_sensitive", state_is(case_result_a, trade::ORDER_STATE_LOCAL_ACCEPTED) &&
                  state_is(case_result_b, trade::ORDER_STATE_LOCAL_ACCEPTED) &&
                  case_result_a.response.order_id() != case_result_b.response.order_id() &&
                  case_orders.size() == 2 && money_is(after, 4000, 6000),
                  {{"upper_case_rpc", rpc_json(case_result_a)}, {"lower_case_rpc", rpc_json(case_result_b)},
                  {"account", after}, {"orders", case_orders}});

            before = db.account(empty_id.id);
            result = submit(empty_id.id, "", 2, 1500);
            after = db.account(empty_id.id);
            check("empty_client_order_id_is_rejected_before_writing", result.code == grpc::StatusCode::INVALID_ARGUMENT &&
                  after == before && db.orders_for(empty_id.id).empty() && db.ledger_for(empty_id.id).empty() &&
                  db.outbox_for(empty_id.id).empty(),
                  {{"rpc", rpc_json(result)}, {"account", after}, {"orders", db.orders_for(empty_id.id)}});
            bool all_ledger_matches = true;
            bool all_outbox_matches = true;
            Json ledger_summary = Json::array();
            Json outbox_summary = Json::array();
            for (const auto& fixture : db.fixtures) {
                const auto fixture_orders = db.orders_for(fixture.id);
                const auto fixture_ledger = db.ledger_for(fixture.id);
                const auto fixture_events = db.outbox_for(fixture.id);
                const bool matches = ledger_matches_orders(fixture_ledger, fixture_orders);
                all_ledger_matches = all_ledger_matches && matches;
                ledger_summary.push_back({{"account_id", fixture.id}, {"orders", fixture_orders.size()},
                                          {"ledger_entries", fixture_ledger.size()}, {"match", matches}});
                const bool events_match = outbox_matches_orders(fixture_events, fixture_orders);
                all_outbox_matches = all_outbox_matches && events_match;
                outbox_summary.push_back({{"account_id", fixture.id}, {"orders", fixture_orders.size()},
                                          {"events", fixture_events.size()}, {"match", events_match}});
            }
            check("all_fixture_orders_have_exactly_one_matching_freeze_entry", all_ledger_matches,
                  {{"accounts", ledger_summary}});
            check("all_fixture_orders_have_one_acceptance_event", all_outbox_matches,
                  {{"accounts", outbox_summary}});

            auto fixture_snapshot = [&](const Fixture& fixture) {
                return Json{{"account", db.account(fixture.id)}, {"orders", db.orders_for(fixture.id)},
                            {"ledger", db.ledger_for(fixture.id)}, {"outbox", db.outbox_for(fixture.id)}};
            };
            auto create_cancel_fixture = [&](const std::string& suffix) {
                auto fixture = db.create(prefix + "_" + suffix);
                const auto client_id = prefix + "_" + suffix;
                const auto rpc = submit(fixture.id, client_id, 2, 1500);
                auto order = db.order(fixture.id, client_id);
                if (!state_is(rpc, trade::ORDER_STATE_LOCAL_ACCEPTED) ||
                    !new_order_is(order, rpc, fixture.id, client_id, 2, 1500)) {
                    throw std::runtime_error("Could not create cancellation fixture: " + suffix);
                }
                return std::make_pair(fixture, order);
            };

            auto [cancel_fixture, cancel_original] = create_cancel_fixture("cancel_normal");
            const auto cancel_key = cancel_original.at("client_order_id").get<std::string>();
            const auto cancel_id = cancel_original.at("order_id").get<U64>();
            const auto freeze_before_cancel = db.ledger_for(cancel_fixture.id);
            const auto acceptance_before_cancel = db.outbox_for(cancel_fixture.id);
            auto cancel_result = cancel_order(cancel_fixture.id, cancel_key);
            auto cancelled_order = db.order(cancel_fixture.id, cancel_key);
            check("cancel_returns_original_id_and_cancelled", cancel_result.code == grpc::StatusCode::OK &&
                  cancel_result.response.order_id() == cancel_id &&
                  cancel_result.response.state() == trade::ORDER_STATE_CANCELLED, rpc_json(cancel_result));
            check("cancel_refunds_budget_and_advances_order_version", money_is(db.account(cancel_fixture.id), 10000, 0) &&
                  !cancelled_order.is_null() && cancelled_order.at("state") == static_cast<int>(trade::ORDER_STATE_CANCELLED) &&
                  cancelled_order.at("version").get<U64>() == cancel_original.at("version").get<U64>() + 1,
                  fixture_snapshot(cancel_fixture));
            const auto cancel_ledger = db.ledger_for(cancel_fixture.id);
            check("cancel_appends_one_release_and_preserves_freeze", cancel_ledger.size() == 2 &&
                  freeze_before_cancel.size() == 1 && cancel_ledger[0] == freeze_before_cancel[0] &&
                  cancel_ledger[1].at("order_id") == cancel_id && cancel_ledger[1].at("entry_type") == 2 &&
                  cancel_ledger[1].at("amount_cent") == 3000, {{"ledger", cancel_ledger}});
            const auto cancel_events = db.outbox_for(cancel_fixture.id);
            check("cancel_appends_snapshot_and_preserves_acceptance_event", cancel_events.size() == 2 &&
                  acceptance_before_cancel.size() == 1 && cancel_events[0] == acceptance_before_cancel[0] &&
                  cancellation_event_is(cancel_events[1], cancel_original), {{"outbox", cancel_events}});

            auto cancel_before = fixture_snapshot(cancel_fixture);
            cancel_result = cancel_order(cancel_fixture.id, cancel_key);
            check("cancel_retry_returns_same_id_without_new_side_effects", cancel_result.code == grpc::StatusCode::OK &&
                  cancel_result.response.order_id() == cancel_id &&
                  cancel_result.response.state() == trade::ORDER_STATE_CANCELLED &&
                  fixture_snapshot(cancel_fixture) == cancel_before,
                  {{"rpc", rpc_json(cancel_result)}, {"data", fixture_snapshot(cancel_fixture)}});
            result = submit(cancel_fixture.id, cancel_key, 2, 1500);
            check("cancelled_order_submit_retry_does_not_refreeze", state_is(result, trade::ORDER_STATE_CANCELLED) &&
                  result.response.order_id() == cancel_id && fixture_snapshot(cancel_fixture) == cancel_before,
                  rpc_json(result));

            cancel_result = cancel_order(0, cancel_key);
            check("cancel_zero_account_is_invalid", cancel_result.code == grpc::StatusCode::INVALID_ARGUMENT, rpc_json(cancel_result));
            cancel_result = cancel_order(cancel_fixture.id, "");
            check("cancel_empty_business_key_is_invalid", cancel_result.code == grpc::StatusCode::INVALID_ARGUMENT, rpc_json(cancel_result));
            cancel_result = cancel_order(cancel_fixture.id, prefix + "_missing_cancel");
            check("cancel_missing_order_is_not_found", cancel_result.code == grpc::StatusCode::NOT_FOUND &&
                  fixture_snapshot(cancel_fixture) == cancel_before, rpc_json(cancel_result));
            cancel_result = cancel_order(happy.id, cancel_key);
            check("cancel_wrong_account_is_not_found", cancel_result.code == grpc::StatusCode::NOT_FOUND &&
                  fixture_snapshot(cancel_fixture) == cancel_before, rpc_json(cancel_result));

            auto [forbidden_fixture, forbidden_order] = create_cancel_fixture("cancel_forbidden");
            const trade::OrderState forbidden_states[] = {
                trade::ORDER_STATE_DISPATCHING, trade::ORDER_STATE_BROKER_ACCEPTED, trade::ORDER_STATE_FILLED
            };
            for (auto forbidden_state : forbidden_states) {
                db.set_order_state(forbidden_fixture, forbidden_order.at("order_id").get<U64>(), forbidden_state);
                const auto before_rejection = fixture_snapshot(forbidden_fixture);
                cancel_result = cancel_order(forbidden_fixture.id, forbidden_order.at("client_order_id").get<std::string>());
                check("cancel_rejects_" + trade::OrderState_Name(forbidden_state),
                      cancel_result.code == grpc::StatusCode::FAILED_PRECONDITION &&
                      fixture_snapshot(forbidden_fixture) == before_rejection,
                      {{"rpc", rpc_json(cancel_result)}, {"data", fixture_snapshot(forbidden_fixture)}});
            }
            db.set_order_state(forbidden_fixture, forbidden_order.at("order_id").get<U64>(), 99);
            cancel_before = fixture_snapshot(forbidden_fixture);
            cancel_result = cancel_order(forbidden_fixture.id, forbidden_order.at("client_order_id").get<std::string>());
            check("cancel_unknown_state_is_internal_without_changes", cancel_result.code == grpc::StatusCode::INTERNAL &&
                  fixture_snapshot(forbidden_fixture) == cancel_before, rpc_json(cancel_result));

            auto [account_fault, account_fault_order] = create_cancel_fixture("cancel_account_fault");
            db.set_frozen(account_fault, 0);
            cancel_before = fixture_snapshot(account_fault);
            cancel_result = cancel_order(account_fault.id, account_fault_order.at("client_order_id").get<std::string>());
            check("cancel_account_mismatch_rolls_back_order_change", cancel_result.code == grpc::StatusCode::INTERNAL &&
                  fixture_snapshot(account_fault) == cancel_before,
                  {{"rpc", rpc_json(cancel_result)}, {"before", cancel_before}, {"after", fixture_snapshot(account_fault)}});

            auto [ledger_fault, ledger_fault_order] = create_cancel_fixture("cancel_ledger_fault");
            db.seed_release_conflict(ledger_fault, ledger_fault_order);
            cancel_before = fixture_snapshot(ledger_fault);
            cancel_result = cancel_order(ledger_fault.id, ledger_fault_order.at("client_order_id").get<std::string>());
            check("cancel_ledger_conflict_rolls_back_all_changes", cancel_result.code == grpc::StatusCode::INTERNAL &&
                  fixture_snapshot(ledger_fault) == cancel_before,
                  {{"rpc", rpc_json(cancel_result)}, {"before", cancel_before}, {"after", fixture_snapshot(ledger_fault)}});

            auto [event_fault, event_fault_order] = create_cancel_fixture("cancel_event_fault");
            db.seed_cancel_event_conflict(event_fault, event_fault_order);
            cancel_before = fixture_snapshot(event_fault);
            cancel_result = cancel_order(event_fault.id, event_fault_order.at("client_order_id").get<std::string>());
            check("cancel_outbox_conflict_rolls_back_all_changes", cancel_result.code == grpc::StatusCode::INTERNAL &&
                  fixture_snapshot(event_fault) == cancel_before,
                  {{"rpc", rpc_json(cancel_result)}, {"before", cancel_before}, {"after", fixture_snapshot(event_fault)}});

            auto [cancel_concurrent, concurrent_order] = create_cancel_fixture("cancel_concurrent");
            std::vector<std::future<CancelRpcResult>> cancel_pending;
            std::latch cancel_ready(8);
            for (int i = 0; i < 8; ++i) cancel_pending.push_back(std::async(std::launch::async, [&] {
                cancel_ready.arrive_and_wait();
                return cancel_order(cancel_concurrent.id, concurrent_order.at("client_order_id").get<std::string>());
            }));
            Json cancel_calls = Json::array();
            bool all_cancelled = true;
            for (auto& future : cancel_pending) {
                const auto rpc = future.get();
                all_cancelled = all_cancelled && rpc.code == grpc::StatusCode::OK &&
                                rpc.response.state() == trade::ORDER_STATE_CANCELLED &&
                                rpc.response.order_id() == concurrent_order.at("order_id").get<U64>();
                cancel_calls.push_back(rpc_json(rpc));
            }
            const auto concurrent_after = db.order(cancel_concurrent.id, concurrent_order.at("client_order_id").get<std::string>());
            check("cancel_eight_requests_return_one_id_and_refund_once", all_cancelled &&
                  money_is(db.account(cancel_concurrent.id), 10000, 0) &&
                  db.ledger_for(cancel_concurrent.id).size() == 2 && db.outbox_for(cancel_concurrent.id).size() == 2 &&
                  concurrent_after.at("version").get<U64>() == 2,
                  {{"rpc_results", cancel_calls}, {"data", fixture_snapshot(cancel_concurrent)}});
        } catch (const std::exception& ex) {
            check("test_execution", false, {{"error", ex.what()}});
        }

        report["fixtures"] = Json::array();
        for (const auto& fixture : db.fixtures) report["fixtures"].push_back({{"id", fixture.id}, {"name", fixture.name}});
        bool cleanup_ok = true;
        for (const auto& fixture : db.fixtures) {
            try { db.erase(fixture); }
            catch (const std::exception& ex) {
                cleanup_ok = false;
                check("fixture_cleanup_error", false, {{"id", fixture.id}, {"name", fixture.name}, {"error", ex.what()}});
            }
        }
        auto final_rows = db.snapshot();
        auto final_orders = db.snapshot_orders();
        auto final_ledger = db.snapshot_ledger();
        auto final_outbox = db.snapshot_outbox();
        report["existing_accounts_after_cleanup"] = final_rows;
        report["existing_orders_after_cleanup"] = final_orders;
        report["existing_ledger_after_cleanup"] = final_ledger;
        report["existing_outbox_after_cleanup"] = final_outbox;
        check("all_fixture_rows_removed", cleanup_ok && final_rows.size() == baseline.size() &&
              final_orders.size() == baseline_orders.size() && final_ledger.size() == baseline_ledger.size() &&
              final_outbox.size() == baseline_outbox.size());
        check("all_preexisting_account_rows_unchanged", final_rows == baseline);
        check("all_preexisting_order_rows_unchanged", final_orders == baseline_orders);
        check("all_preexisting_ledger_rows_unchanged", final_ledger == baseline_ledger);
        check("all_preexisting_outbox_rows_unchanged", final_outbox == baseline_outbox);
    } catch (const std::exception& ex) {
        check("database_setup", false, {{"error", ex.what()}});
    }
    int failed = 0;
    for (const auto& item : report["checks"]) if (!item["pass"].get<bool>()) ++failed;
    report["failed"] = failed;
    report["total"] = report["checks"].size();
    std::ofstream file(report_path);
    file << report.dump(2) << '\n';
    file.close();
    if (!file) {
        std::cerr << "Cannot write report: " << report_path << '\n';
        return 2;
    }
    std::cout << "SUMMARY " << report["total"] << " checks, " << failed << " failed\n";
    std::cout << "REPORT " << report_path << '\n';
    return failed == 0 ? 0 : 1;
}
