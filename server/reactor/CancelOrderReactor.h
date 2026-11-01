
#include "TradeAccountRepository.h"
#include "trade_account.pb.h"
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/strand.hpp>
#include <exception>
#include <grpcpp/support/server_callback.h>
#include <grpcpp/support/status.h>
#include <grpcpp/server_context.h>

class CancelOrderReactor final : public grpc::ServerUnaryReactor {
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;

    CancelOrderReactor(boost::asio::io_context& io_context) :
        strand_(boost::asio::make_strand(io_context.get_executor())) {}
    
    boost::asio::awaitable<void> async_cancel_order(const trade::CancelOrderRequest& request,
        trade::CancelOrderResponse* response, grpc::CallbackServerContext* context, TradeAccountRepository* rp)
    {
        if (request.account_id() == 0 ||
            request.client_order_id().empty()) {
            Finish(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid account ID."));
            co_return;
        }

        if (context->IsCancelled()) {
            SPDLOG_INFO("Client already cancelled the request.");
            Finish(grpc::Status(grpc::StatusCode::CANCELLED, "Client cancelled the request."));
            co_return;
        }

        auto [ret_code, res] = co_await rp->async_cancel_order(request.account_id(), request.client_order_id());
        if (ret_code != grpc::StatusCode::OK) {
            Finish(grpc::Status(ret_code, ""));
            co_return;
        }

        response->set_order_id(res->order_id);
        response->set_state(static_cast<trade::OrderState>(res->order_state));
        if (context->IsCancelled()) {
            SPDLOG_INFO("Client already cancelled the request.");
            Finish(grpc::Status(grpc::StatusCode::CANCELLED, "Client cancelled the request."));
            co_return;
        }
        Finish(grpc::Status::OK);
        co_return;
    }
public:
    static CancelOrderReactor* CreateCancelOrderTask(const trade::CancelOrderRequest& request, trade::CancelOrderResponse* response,
        grpc::CallbackServerContext* context, boost::asio::io_context& ioc, TradeAccountRepository* rp)
    {
        auto reactor = new CancelOrderReactor(ioc);
        boost::asio::co_spawn(reactor->strand_,
            reactor->async_cancel_order(request, response, context, rp),
                [reactor](std::exception_ptr e) {
                    if (!e) {
                        return;
                    }
                    SPDLOG_ERROR("mysql cancel order coroutine failed\n");
                    reactor->Finish(grpc::Status(grpc::StatusCode::INTERNAL, "mysql cancel order coroutine failed"));
                    return;
                }
        );
        return reactor;
    }

    void OnDone() override {
        delete this;
    }
};