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
#include <memory>
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

enum class LEDGER_ENTRY_TYPE : uint8_t {
    ORDER_NONE          = 0,
    ORDER_FREEZE        = 1,
    ORDER_CANCELLED     = 2,
    ORDER_FILLED        = 3,
    ORDER_REJECTED      = 4
};

enum class OUTBOX_EVENT_TYPE : uint8_t {
    OUTBOX_EVENT_NONE           = 0, 
    OUTBOX_EVENT_RECEIVED       = 1,
    OUTBOX_EVENT_CANCELLED      = 2,
    OUTBOX_EVENT_DISPATCHED     = 3,
    OUTBOX_EVENT_REJECTED       = 4,
    OUTBOX_EVENT_FILLED         = 5
};

template<typename T>
constexpr inline uint8_t cast_type(T Type) {
    return static_cast<uint8_t>(Type);
}

// OrderDispatch 通知修改本地表
struct OrderRefreshPkg {
    uint64_t order_id;          // 刷新订单ID
    uint64_t unfreeze_cent = 0; // 解除冻结金额
    uint64_t spent_cent    = 0; // 已扣除的花费金额
    trade::OrderState target_state;
    LEDGER_ENTRY_TYPE ledger_type;
    OUTBOX_EVENT_TYPE outbox_type;
};

struct OrderSubmitBrief {
    uint64_t account_id;
    uint64_t limit_price_cent;
    uint64_t quantity;
    std::string symbol;
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
    boost::asio::awaitable<std::optional<OrderSubmitBrief>> async_get_order_submit_brief(uint64_t order_id);
    boost::asio::awaitable<std::vector<std::pair<uint64_t, trade::OrderState>>> async_list_handle_order(uint16_t limit);
    boost::asio::awaitable<void> async_dispatcher_refresh_data(std::unique_ptr<OrderRefreshPkg> refresh_pkg);
};