#pragma once
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
#include <grpcpp/support/server_callback.h>
#include <grpcpp/support/status.h>
#include <memory>
#include <spdlog/spdlog.h>
#include "interview_arena.pb.h"
#include "AsioRuntime.h"
#include "QuestionRepository.h"

class GetQuestionReactor final : public grpc::ServerUnaryReactor {
private:
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    std::shared_ptr<boost::asio::steady_timer> timer_;

    boost::asio::awaitable<void> async_get_question(QuestionRepository* qr, const uint64_t qid, const uint64_t delay_ms, grpc::CallbackServerContext* ctx
        , arena::GetQuestionResponse* response) {
        if (qid == 0) {
            Finish(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid question ID."));
            co_return;
        }

        if (delay_ms > 0) {
            // 如果strand里已有其它串行任务提前取消，这里直接返回
            if (ctx->IsCancelled()) {
                SPDLOG_INFO("Client cancelled the request.");
                Finish(grpc::Status(grpc::StatusCode::CANCELLED, "Client cancelled the request."));
                co_return;
            }

            // 模拟延时：挂起协程等待，不阻塞工作线程。
            timer_->expires_after(boost::asio::chrono::milliseconds(delay_ms));

            boost::system::error_code wait_ret_errcode;
            co_await timer_->async_wait(boost::asio::redirect_error(boost::asio::use_awaitable, wait_ret_errcode));

            if (ctx->IsCancelled()) {
                SPDLOG_INFO("Client already cancelled the request.");
                Finish(grpc::Status(grpc::StatusCode::CANCELLED, "Client cancelled the request."));
                co_return;
            }

            if (wait_ret_errcode) {
                SPDLOG_ERROR("Timer wait error: {}", wait_ret_errcode.message());
                Finish(grpc::Status(grpc::StatusCode::INTERNAL, "Timer wait error."));
                co_return;
            }
        }

        auto res = co_await qr->async_get_question_by_id(qid);
        if (!res.has_value()) {
            Finish(grpc::Status(grpc::StatusCode::NOT_FOUND, "Question not found."));
            co_return;
        }

        response->set_question_id(res->question_id);
        response->set_title(res->title);
        response->set_difficulty(arena::QuestionDifficulty(res->difficulty));
        response->set_torture_score(res->torture_score);
        response->set_source(res->source);

        Finish(grpc::Status(grpc::Status::OK));
        co_return;
    }

    GetQuestionReactor(uint64_t q_id, uint32_t delay_ms, arena::GetQuestionResponse* resp, grpc::CallbackServerContext* ctx, AsioRuntime* runtime) :
        strand_(runtime->get_io_context().get_executor()), timer_(std::make_shared<boost::asio::steady_timer>(runtime->get_io_context())) {}

    ~GetQuestionReactor() override = default;
public:
    static GetQuestionReactor* CreateGetTask (grpc::CallbackServerContext* context, const arena::GetQuestionRequest* request,
        arena::GetQuestionResponse* response, AsioRuntime* runtime, QuestionRepository* qr) {
        auto* reactor = new GetQuestionReactor(request->question_id(),
            request->mock_delay_ms(),
            response,
            context,
            runtime);

        boost::asio::co_spawn(reactor->strand_,
            reactor->async_get_question(qr, request->question_id(), request->mock_delay_ms(), context, response),
                [reactor](std::exception_ptr e) {
                if (e) {
                    SPDLOG_ERROR("mysql get question by qid coroutine failed\n");
                    reactor->Finish(grpc::Status(grpc::StatusCode::INTERNAL, "mysql get question by qid coroutine failed"));
                    return;
                }
            });
        return reactor;
    }

    void OnDone() override {
        delete this;
    }

    void OnCancel() override {
        // TODO: 将取消通知关联到当前协程的等待操作。
        if (timer_) {
            boost::asio::post(strand_, [this, weak_timer = std::weak_ptr<boost::asio::steady_timer>(timer_)] {
                if (auto timer = weak_timer.lock()) {
                    timer->cancel();
                }
            });
        }
    }
};
