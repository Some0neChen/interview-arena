
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

class SubmitOrderReactor final : public grpc::ServerUnaryReactor {
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;

    SubmitOrderReactor(boost::asio::io_context& io_context) :
        strand_(boost::asio::make_strand(io_context.get_executor())) {}
    
    boost::asio::awaitable<void> async_submit_order(const trade::SubmitOrderRequest& request,
        grpc::CallbackServerContext* context, trade::SubmitOrderResponse* response, TradeAccountRepository* rp)
    {
        if (request.account_id() == 0 ||
            request.client_order_id().empty() ||
            request.symbol().empty() ||
            request.quantity() == 0||
            request.limit_price_cent() == 0) {
            Finish(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid account ID."));
            co_return;
        }
        if (context->IsCancelled()) {
            SPDLOG_INFO("Client already cancelled the request.");
            Finish(grpc::Status(grpc::StatusCode::CANCELLED, "Client cancelled the request."));
            co_return;
        }

        auto [ret_code, res] = co_await rp->submit_order(request.account_id(),
            request.client_order_id(), request.symbol(),
            request.quantity(), request.limit_price_cent());
        if (!res.has_value()) {
            Finish(grpc::Status(grpc::StatusCode::NOT_FOUND, "Account has not createn."));
            co_return;
        }

        response->set_order_id(res->order_id);
        response->set_state(static_cast<trade::OrderState>(res->execute_state));
        if (context->IsCancelled()) {
            SPDLOG_INFO("Client already cancelled the request.");
            Finish(grpc::Status(grpc::StatusCode::CANCELLED, "Client cancelled the request."));
            co_return;
        }
        Finish(ret_code == grpc::StatusCode::OK ? grpc::Status::OK : grpc::Status(ret_code, ""));
        co_return;
    }
public:
    static SubmitOrderReactor* CreateSubmitOrderTask(const trade::SubmitOrderRequest& request, trade::SubmitOrderResponse* response,
        grpc::CallbackServerContext* context, boost::asio::io_context& ioc, TradeAccountRepository* rp)
    {
        auto reactor = new SubmitOrderReactor(ioc);
        boost::asio::co_spawn(reactor->strand_,
            reactor->async_submit_order(request, context, response, rp),
                [reactor](std::exception_ptr e) {
                    if (!e) {
                        return;
                    }
                    SPDLOG_ERROR("mysql submit order coroutine failed\n");
                    reactor->Finish(grpc::Status(grpc::StatusCode::INTERNAL, "mysql submit order coroutine failed"));
                    return;
                }
        );
        return reactor;
    }

    void OnDone() override {
        delete this;
    }
};