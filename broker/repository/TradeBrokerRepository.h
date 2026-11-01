#pragma once

#include "trade_broker.pb.h"
#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <boost/mysql/connection_pool.hpp>
#include <cstdint>
#include <grpcpp/support/status.h>
#include <optional>
#include <string>
#include <utility>

struct SubmitOrderRequest {
    std::string gateway_namespace;
    uint64_t gateway_order_id ;
    uint64_t account_id;
    std::string symbol;
    uint64_t limit_price_cent;
    uint64_t quantity;
};

struct SubmitOrderResponse {
    uint64_t broker_order_id;
    broker::BrokerOrderState state;
};

struct BrokerOrderClintId {
    uint64_t gateway_order_id;
    std::string gateway_namespace;
};

struct BrokerOrderInfo {
    uint64_t broker_order_id;
    uint64_t gateway_order_id;
    uint64_t account_id;
    uint64_t quantity;
    uint64_t limit_price_cent;
    uint64_t filled_quantity;
    uint64_t fill_price_cent;
    std::string gateway_namespace;
    std::string symbol;
    std::string reject_reason;
    broker::BrokerOrderState state;
    uint16_t broker_version;
};

namespace BROKER_ORDER_REJECT_REASON {
    constexpr static std::string_view NORMAL            = "";
    constexpr static std::string_view UNSUPPORT_SYMBOL  = "unsupported symbol";
    constexpr static std::string_view SYMBOL_DISABLED   = "symbol disabled";
}

class TradeBrokerRepository
{
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    boost::mysql::connection_pool pool_;

public:
    TradeBrokerRepository(boost::mysql::pool_params params, boost::asio::io_context& ioc);
    ~TradeBrokerRepository();

    void stop();

    boost::asio::awaitable<std::pair<grpc::StatusCode, std::optional<SubmitOrderResponse>>> async_submit_order(SubmitOrderRequest request);
    boost::asio::awaitable<std::pair<grpc::StatusCode, std::optional<BrokerOrderInfo>>> async_get_order(BrokerOrderClintId request);
    boost::asio::awaitable<std::pair<grpc::StatusCode, std::optional<BrokerOrderInfo>>> async_cancel_order(BrokerOrderClintId request);
};