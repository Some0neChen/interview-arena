#pragma once

#include "GetAccountReactor.h"
#include "TradeAccountRepository.h"
#include "trade_account.grpc.pb.h"
#include "trade_account.pb.h"
#include <boost/asio/io_context.hpp>
#include <grpcpp/server_context.h>
#include <grpcpp/support/server_callback.h>
#include "SubmitOrderReactor.h"
#include "GetOrderReactor.h"
#include "CancelOrderReactor.h"

class TradeAccountServiceImpl final : public trade::TradeAccount::CallbackService {
    boost::asio::io_context* ioc_;
    TradeAccountRepository* pr_;
public:
    TradeAccountServiceImpl(boost::asio::io_context* io_context, TradeAccountRepository* pr) : ioc_(io_context), pr_(pr) {}

    grpc::ServerUnaryReactor* GetAccount(grpc::CallbackServerContext* context,
        const trade::GetTradeAccountRequest* request, trade::GetTradeAccountResponse* response)
    {
        return GetAccountReacotr::CreateGetAccountTask(*request, response, context, *ioc_, pr_);
    }

    grpc::ServerUnaryReactor* SubmitOrder(grpc::CallbackServerContext* context, const trade::SubmitOrderRequest* request, trade::SubmitOrderResponse* response)
    {
        return SubmitOrderReactor::CreateSubmitOrderTask(*request, response, context, *ioc_, pr_);
    }

    grpc::ServerUnaryReactor* GetOrder(grpc::CallbackServerContext* context, const trade::GetOrderRequest* request, trade::GetOrderResponse* response)
    {
        return GetOrderReactor::CreateGetOrderTask(*request, response, context, *ioc_, pr_);
    }

    grpc::ServerUnaryReactor* CancelOrder(grpc::CallbackServerContext* context, const trade::CancelOrderRequest* request, trade::CancelOrderResponse* response)
    {
        return CancelOrderReactor::CreateCancelOrderTask(*request, response, context, *ioc_, pr_);
    }
};