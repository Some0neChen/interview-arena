#pragma once

#include "TradeAccountRepository.h"
#include "trade_account.pb.h"
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detail/chrono.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/system/detail/error_code.hpp>
#include <exception>
#include <grpcpp/server_context.h>
#include <grpcpp/support/server_callback.h>
#include <grpcpp/support/status.h>
#include <grpcpp/server_context.h>

class GetOrderReactor final : public grpc::ServerUnaryReactor {
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;

    GetOrderReactor(boost::asio::io_context& ioc) :
        strand_(boost::asio::make_strand(ioc.get_executor())) {}

    boost::asio::awaitable<void> async_get_order(const trade::GetOrderRequest& request,
        trade::GetOrderResponse* response, grpc::CallbackServerContext* context, TradeAccountRepository* rp)
    {
        if (request.account_id() == 0 ||
            request.client_order_id().empty()) {
            Finish(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid get order argument."));
            co_return;
        }
        if (context->IsCancelled()) {
            SPDLOG_INFO("Client already cancelled the request.");
            Finish(grpc::Status(grpc::StatusCode::CANCELLED, "Client cancelled the request."));
            co_return;
        }

        auto [code, res] = co_await rp->async_get_order(request.account_id(),
            request.client_order_id());
        if (!res.has_value()) {
            Finish(grpc::Status(grpc::StatusCode::NOT_FOUND, "Account has not createn."));
            co_return;
        }
        if (code != grpc::StatusCode::OK) {
            Finish(grpc::Status(code, ""));
            co_return;
        }

        response->set_order_id(res->order_id);
        response->set_symbol(res->symbol);
        response->set_account_id(res->account_id);
        response->set_limit_price_cent(res->limit_price_cent);
        response->set_quantity(res->quantity);
        response->set_reserved_cent(res->reserved_cent);
        response->set_state(res->state);
        response->set_version(res->version);

        if (context->IsCancelled()) {
            SPDLOG_INFO("Client already cancelled the request.");
            Finish(grpc::Status(grpc::StatusCode::CANCELLED, "Client cancelled the request."));
            co_return;
        }
        Finish(grpc::Status::OK);
        co_return;
    }
public:
    static GetOrderReactor* CreateGetOrderTask(const trade::GetOrderRequest& request, trade::GetOrderResponse* response,
        grpc::CallbackServerContext* context, boost::asio::io_context& ioc, TradeAccountRepository* rp)
    {
        auto reactor = new GetOrderReactor(ioc);
        boost::asio::co_spawn(reactor->strand_,
            reactor->async_get_order(request, response, context, rp),
                [reactor](std::exception_ptr e) {
                    if (!e) {
                        return;
                    }
                    SPDLOG_ERROR("mysql get order coroutine failed\n");
                    reactor->Finish(grpc::Status(grpc::StatusCode::INTERNAL, "mysql get order coroutine failed"));
                    return;
                }
        );
        return reactor;
    }

    void OnDone() override {
        delete this;
    }
};