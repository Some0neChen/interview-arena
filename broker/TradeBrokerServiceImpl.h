#pragma once

#include "CancelBrokerOrderReactor.h"
#include "SubmitOrderReactor.h"
#include "TradeBrokerRepository.h"
#include "trade_broker.grpc.pb.h"
#include "trade_broker.pb.h"
#include <boost/asio/io_context.hpp>
#include <grpcpp/impl/service_type.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/server_callback.h>
#include "GetBrokerOrderReactor.h"

class TradeBrokerServiceImpl final : public broker::TradeBroker::CallbackService {
    boost::asio::io_context* io_;
    TradeBrokerRepository* rp_;
public:
    TradeBrokerServiceImpl(boost::asio::io_context* io_context, TradeBrokerRepository* repository) :
        io_(io_context), rp_(repository) {}
    
    grpc::ServerUnaryReactor* SubmitOrder(grpc::CallbackServerContext* context,
        const broker::SubmitOrderRequest* request, broker::SubmitOrderResponse* response) override
    {
        return SubmitOrderReactor::CreateSubmitOrderTask(*request, response, context, *io_, rp_);
    }

    grpc::ServerUnaryReactor* GetOrder(grpc::CallbackServerContext* context,
        const broker::GetBrokerOrderRequest* request, broker::GetBrokerOrderResponse* response) override
    {
        return GetBrokerOrderReactor::CreateGetOrderInfoTask(*request, response, context, *io_, rp_);
    }

    grpc::ServerUnaryReactor* CancelOrder(grpc::CallbackServerContext* context,
        const broker::GetBrokerOrderRequest* request, broker::GetBrokerOrderResponse* response) override
    {
        return CancelBrokerOrderReactor::CreateCancelOrderTask(*request, response, context, *io_, rp_);
    }
};