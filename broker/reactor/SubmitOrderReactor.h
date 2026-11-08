#pragma once

#include "TradeBrokerRepository.h"
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <exception>
#include <grpcpp/support/server_callback.h>
#include <grpcpp/support/status.h>
#include <grpcpp/server_context.h>
#include <spdlog/spdlog.h>
#include <utility>

class SubmitOrderReactor final : public grpc::ServerUnaryReactor {
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;

    SubmitOrderReactor(boost::asio::io_context& io_context) :
        strand_(boost::asio::make_strand(io_context.get_executor())) {}
    
    boost::asio::awaitable<void> async_submit_order(const broker::SubmitOrderRequest& request,
        broker::SubmitOrderResponse* response, grpc::CallbackServerContext* context, TradeBrokerRepository* rp)
    {
        if (context->IsCancelled()) {
            SPDLOG_WARN("Client already cancelled the request.");
            Finish(grpc::Status(grpc::StatusCode::CANCELLED, "Client cancelled the request."));
            co_return;
        }

        SubmitOrderRequest req = {
            .gateway_namespace = std::move(request.gateway_namespace()),
            .gateway_order_id = request.gateway_order_id(),
            .account_id = request.account_id(),
            .symbol = std::move(request.symbol()),
            .limit_price_cent = request.limit_price_cent(),
            .quantity = request.quantity()
        };
        auto [ret_code, res] = co_await rp->async_submit_order(std::move(req));
        if (ret_code != grpc::StatusCode::OK || !res.has_value()) {
            Finish(grpc::Status(ret_code, "Mysql table err."));
            co_return;
        }

        response->set_broker_order_id(res->broker_order_id);
        response->set_state(res->state);
        if (context->IsCancelled()) {
            SPDLOG_INFO("Client already cancelled the request.");
            Finish(grpc::Status(grpc::StatusCode::CANCELLED, "Client cancelled the request."));
            co_return;
        }
        Finish(grpc::Status::OK);
        co_return;
    }
public:
    static SubmitOrderReactor* CreateSubmitOrderTask(const broker::SubmitOrderRequest& request, broker::SubmitOrderResponse* response,
        grpc::CallbackServerContext* context, boost::asio::io_context& ioc, TradeBrokerRepository* rp)
    {
        auto reactor = new SubmitOrderReactor(ioc);
        boost::asio::co_spawn(reactor->strand_,
            reactor->async_submit_order(request, response, context, rp),
                [reactor](std::exception_ptr e) {
                    if (!e) {
                        return;
                    }
                    SPDLOG_ERROR("mysql submit broker order coroutine failed\n");
                    reactor->Finish(grpc::Status(grpc::StatusCode::INTERNAL, "mysql submit broker order coroutine failed"));
                    return;
                }
        );
        return reactor;
    }

    void OnDone() override {
        delete this;
    }
};