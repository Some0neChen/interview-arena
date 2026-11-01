#pragma once

#include "TradeBrokerRepository.h"
#include "trade_broker.pb.h"
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <grpcpp/server_context.h>
#include <grpcpp/support/server_callback.h>
#include <spdlog/spdlog.h>

class CancelBrokerOrderReactor final : public grpc::ServerUnaryReactor {
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;

    CancelBrokerOrderReactor(boost::asio::io_context& io_context) :
        strand_(boost::asio::make_strand(io_context.get_executor())) {}
    
    boost::asio::awaitable<void> async_cancel_order(const broker::GetBrokerOrderRequest& request,
        broker::GetBrokerOrderResponse* response, grpc::CallbackServerContext* context, TradeBrokerRepository* rp)
    {
        if (context->IsCancelled()) {
            SPDLOG_WARN("Client already cancelled the request.");
            Finish(grpc::Status(grpc::StatusCode::CANCELLED, "Client cancelled the request."));
            co_return;
        }

        BrokerOrderClintId req = {
            .gateway_order_id = request.gateway_order_id(),
            .gateway_namespace = std::move(request.gateway_namespace()),
        };
        auto [ret_code, res] = co_await rp->async_cancel_order(std::move(req));
        if (ret_code != grpc::StatusCode::OK || !res.has_value()) {
            Finish(grpc::Status(ret_code, "Mysql table err."));
            co_return;
        }

        response->set_broker_order_id(res->broker_order_id);
        response->set_gateway_namespace(res->gateway_namespace);
        response->set_gateway_order_id(res->gateway_order_id);
        response->set_account_id(res->account_id);
        response->set_symbol(res->symbol);
        response->set_quantity(res->quantity);
        response->set_limit_price_cent(res->limit_price_cent);
        response->set_state(res->state);
        response->set_filled_quantity(res->filled_quantity);
        response->set_fill_price_cent(res->fill_price_cent);
        response->set_broker_version(res->broker_version);
        response->set_reject_reason(res->reject_reason);
        if (context->IsCancelled()) {
            SPDLOG_INFO("Client already cancelled the request.");
            Finish(grpc::Status(grpc::StatusCode::CANCELLED, "Client cancelled the request."));
            co_return;
        }
        Finish(grpc::Status::OK);
        co_return;
    }
public:
    static CancelBrokerOrderReactor* CreateCancelOrderTask(const broker::GetBrokerOrderRequest& request, broker::GetBrokerOrderResponse* response,
        grpc::CallbackServerContext* context, boost::asio::io_context& ioc, TradeBrokerRepository* rp)
    {
        auto reactor = new CancelBrokerOrderReactor(ioc);
        boost::asio::co_spawn(reactor->strand_,
            reactor->async_cancel_order(request, response, context, rp),
                [reactor](std::exception_ptr e) {
                    if (!e) {
                        return;
                    }
                    SPDLOG_ERROR("mysql cancel broker order coroutine failed\n");
                    reactor->Finish(grpc::Status(grpc::StatusCode::INTERNAL, "mysql cancel broker order coroutine failed"));
                    return;
                }
        );
        return reactor;
    }

    void OnDone() override {
        delete this;
    }
};