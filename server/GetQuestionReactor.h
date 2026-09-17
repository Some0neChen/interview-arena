
#include <chrono>
#include <cstdint>
#include <grpcpp/support/server_callback.h>
#include <memory>
#include <optional>
#include <spdlog/spdlog.h>
#include "interview_arena.grpc.pb.h"
#include "interview_arena.pb.h"
#include "AsioRuntime.h"

class GetQuestionReactor final : public grpc::ServerUnaryReactor {
private:
    uint64_t question_id;
    uint32_t mock_delay_ms;
    arena::GetQuestionResponse* response;
    grpc::CallbackServerContext* context;
    AsioRuntime* runtime;
    std::optional<std::shared_ptr<AsioRuntime::DelayTask>> delay_task_;

    void GetQuestion() {
        if (question_id == 0) {
            Finish(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid question ID."));
            return;
        }
        if (context->IsCancelled()) {
            SPDLOG_INFO("Client cancelled the request.");
            Finish(grpc::Status(grpc::StatusCode::CANCELLED, "Client cancelled the request."));
            return;
        }
        response->set_question_id(question_id);
        response->set_title("Epoll ET模式为什么要读到EGAIN? 读到0是什么意思? EPOLLHUP和EpollRDHUP的区别是什么?");
        response->set_difficulty(arena::QuestionDifficulty::MEDIUM);
        response->set_torture_score(726);
        response->set_source("https://someonechen.interview.invincible.com/arena/question/0");
        Finish(grpc::Status(grpc::Status::OK));
    }

    void async_get_question() {
        if (mock_delay_ms > 0) {
            delay_task_ = runtime->get_delay_task();
            if (delay_task_.has_value()) {
                (*delay_task_)->run_timer(std::chrono::milliseconds(mock_delay_ms), &GetQuestionReactor::GetQuestion, this);
                return;
            }
        } 
        else {
            runtime->add_task(&GetQuestionReactor::GetQuestion, this);
        }
    }

    GetQuestionReactor(uint64_t q_id, uint32_t delay_ms, arena::GetQuestionResponse* resp, grpc::CallbackServerContext* ctx, AsioRuntime* runtime) :
        question_id(q_id), mock_delay_ms(delay_ms), response(resp), context(ctx), runtime(runtime) {}

    ~GetQuestionReactor() override = default;
public:
    static GetQuestionReactor* CreateGetTask (grpc::CallbackServerContext* context, const arena::GetQuestionRequest* request,
        arena::GetQuestionResponse* response, AsioRuntime* runtime) {
        auto* reactor = new GetQuestionReactor(request->question_id(),
            request->mock_delay_ms(),
            response,
            context,
            runtime);
        reactor->async_get_question();
        return reactor;
    }

    void OnDone() override {
        delete this;
    }

    void OnCancel() override {
        if (delay_task_.has_value()) {
            (*delay_task_)->cancel();
        }
    }
};