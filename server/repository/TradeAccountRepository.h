#pragma once

#include "trade_account.pb.h"
#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancel_after.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detail/chrono.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/mysql/common_server_errc.hpp>
#include <boost/mysql/connection.hpp>
#include <boost/mysql/connection_pool.hpp>
#include <boost/mysql/error_code.hpp>
#include <boost/mysql/error_with_diagnostics.hpp>
#include <boost/mysql/pool_params.hpp>
#include <boost/mysql/results.hpp>
#include <boost/mysql/with_params.hpp>
#include <boost/system/system_error.hpp>
#include <cstdint>
#include <grpcpp/support/status.h>
#include <optional>
#include <spdlog/spdlog.h>
#include <string>
#include <utility>
#include <vector>

struct Account {
    uint64_t account_id;
    std::string account_name;
    uint64_t initial_budget_cent;
    uint64_t available_cent;
    uint64_t frozen_cent;
    uint64_t spent_cent;
};

struct SubmitResult {
    uint64_t order_id;
    trade::OrderState execute_state;
};

struct Order {
    uint64_t order_id;
    std::string symbol;
    uint64_t account_id;
    uint64_t limit_price_cent;
    uint64_t quantity;
    uint64_t reserved_cent;
    trade::OrderState state;
    uint64_t version;
};

struct CancelOrderResult {
    uint64_t order_id;
    trade::OrderState order_state;;
};

namespace LEDGER_ENTRY_TYPE {
    constexpr static uint8_t ORDER_FREEZE       = 1;
    constexpr static uint8_t ORDER_CANCELLED    = 2;
};

namespace OUTBOX_EVENT_TYPE {
    constexpr static uint8_t OUTBOX_EVENT_RECEIVED  = 1;
    constexpr static uint8_t OUTBOX_EVENT_CANCELLED = 2;
};

class TradeAccountRepository {
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    boost::mysql::connection_pool pool_;
public:
    TradeAccountRepository(boost::mysql::pool_params params, boost::asio::io_context& ioc);
    ~TradeAccountRepository();

    void stop();

    boost::asio::awaitable<bool> check_account_exist_by_id(uint64_t id, boost::mysql::pooled_connection& conn,
        boost::asio::chrono::time_point<std::chrono::steady_clock>& deadline);
    boost::asio::awaitable<std::optional<Account>> async_get_account_by_id(uint64_t id);
    boost::asio::awaitable<std::pair<grpc::StatusCode, std::optional<SubmitResult>>> submit_order(uint64_t account_id,
        std::string client_order_id, std::string symbol, uint64_t quantity, uint64_t price);
    boost::asio::awaitable<std::pair<grpc::StatusCode, std::optional<Order>>> async_get_order(
        uint64_t account_id, const std::string& client_order_id);
    boost::asio::awaitable<std::pair<grpc::StatusCode, std::optional<CancelOrderResult>>> async_cancel_order(
        uint64_t account_id, const std::string& client_order_id);
    boost::asio::awaitable<std::vector<std::pair<uint64_t, trade::OrderState>>> async_list_handle_order(uint16_t limit);
};