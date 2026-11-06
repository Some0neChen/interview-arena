#pragma once

#include "trade_broker.grpc.pb.h"
#include "trade_broker.pb.h"
#include <boost/asio/awaitable.hpp>
#include <grpcpp/support/status.h>

struct BrokerSubmitOrderResult {
    grpc::StatusCode ret_code;
    broker::SubmitOrderResponse response;
};

struct BrokerOrderResult {
    grpc::StatusCode ret_code;
    broker::GetBrokerOrderResponse response;
};

class BrokerClient {
    std::unique_ptr<broker::TradeBroker::Stub> stub_;

    struct AsyncSubmitCall {
        grpc::ClientContext context;
        std::shared_ptr<const broker::SubmitOrderRequest> request;
        broker::SubmitOrderResponse response;
    };

    struct AsyncGetOrderCall {
        grpc::ClientContext context;
        std::shared_ptr<const broker::GetBrokerOrderRequest> request;
        broker::GetBrokerOrderResponse response;
    };

public:
    BrokerClient(const std::string& broker_address);

    boost::asio::awaitable<BrokerSubmitOrderResult>
    async_submit_order(std::shared_ptr<const broker::SubmitOrderRequest> request);

    boost::asio::awaitable<BrokerOrderResult>
    async_get_order(std::shared_ptr<const broker::GetBrokerOrderRequest> request);

    boost::asio::awaitable<BrokerOrderResult>
    async_cancel_order(std::shared_ptr<const broker::GetBrokerOrderRequest> request);
};