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
#include <cstdint>
#include <exception>
#include <grpcpp/server_context.h>
#include <grpcpp/support/server_callback.h>
#include <grpcpp/support/status.h>
#include <memory>
#include <grpcpp/server_context.h>

class GetAccountReacotr final : public grpc::ServerUnaryReactor {
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    std::shared_ptr<boost::asio::steady_timer> timer_;

    GetAccountReacotr(boost::asio::io_context& ioc) :
        strand_(boost::asio::make_strand(ioc.get_executor())), timer_(std::make_shared<boost::asio::steady_timer>(strand_)) {}

    boost::asio::awaitable<void> async_get_account(uint64_t id, uint64_t delay_ms,
        grpc::CallbackServerContext* context,trade::GetTradeAccountResponse* response, TradeAccountRepository* rp)
    {
        if (id == 0) {
            Finish(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid account ID."));
            co_return;
        }

        if (delay_ms > 0) {
            if (context->IsCancelled()) {
                SPDLOG_INFO("Client cancelled the request.");
                Finish(grpc::Status(grpc::StatusCode::CANCELLED, "Client cancelled the request."));
                co_return;
            }

            timer_->expires_after(boost::asio::chrono::milliseconds(delay_ms));
            boost::system::error_code err_code;
            co_await timer_->async_wait(boost::asio::redirect_error(err_code));

            if (context->IsCancelled()) {
                SPDLOG_INFO("Client already cancelled the request.");
                Finish(grpc::Status(grpc::StatusCode::CANCELLED, "Client cancelled the request."));
                co_return;
            }

            if (err_code) {
                SPDLOG_ERROR("Timer wait error: {}", err_code.message());
                Finish(grpc::Status(grpc::StatusCode::INTERNAL, "Timer wait error."));
                co_return;
            }
        }

        auto res = co_await rp->async_get_account_by_id(id);
        if (!res.has_value()) {
            Finish(grpc::Status(grpc::StatusCode::NOT_FOUND, "Account has not createn."));
            co_return;
        }

        response->set_account_id(res->account_id);
        response->set_account_name(res->account_name);
        response->set_initial_budget_cent(res->initial_budget_cent);
        response->set_available_cent(res->available_cent);
        response->set_frozen_cent(res->frozen_cent);
        response->set_spent_cent(res->spent_cent);

        Finish(grpc::Status(grpc::Status::OK));
        co_return;
    }
public:
    static GetAccountReacotr* CreateGetAccountTask(const trade::GetTradeAccountRequest& request, trade::GetTradeAccountResponse* response,
        grpc::CallbackServerContext* context, boost::asio::io_context& ioc, TradeAccountRepository* rp)
    {
        auto reactor = new GetAccountReacotr(ioc);
        boost::asio::co_spawn(reactor->strand_, reactor->async_get_account(request.account_id(),
            request.mock_delay_ms(), context, response, rp),
            [reactor](std::exception_ptr e) {
                if (e) {
                    SPDLOG_ERROR("mysql get question by qid coroutine failed\n");
                    reactor->Finish(grpc::Status(grpc::StatusCode::INTERNAL, "mysql get question by qid coroutine failed"));
                    return;
                }
            });
        return reactor;
    }

    ~GetAccountReacotr() = default;

    void OnDone() override {
        delete this;
    }

    void OnCancel() override {
        // 取消定时器等待
        if (timer_) {
            boost::asio::post(strand_, [weak_timer = std::weak_ptr<boost::asio::steady_timer>(timer_)]() {
                if (auto timer = weak_timer.lock()) {
                    timer->cancel();
                }
            });
        }
    }
};