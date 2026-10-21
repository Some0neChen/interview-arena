#pragma once
#include "AsioRuntime.h"
#include "interview_arena.grpc.pb.h"
#include "interview_arena.pb.h"
#include "GetQuestionReactor.h"
#include <memory>

class InterviewArenaServiceImpl final : public arena::InterviewArena::CallbackService {
    std::shared_ptr<AsioRuntime> runtime;
    QuestionRepository* qr_;
public:
    InterviewArenaServiceImpl(std::shared_ptr<AsioRuntime> runtime, QuestionRepository* qr) : runtime(runtime), qr_(qr) {}

    grpc::ServerUnaryReactor* GetQuestion(grpc::CallbackServerContext* context, const arena::GetQuestionRequest* request, arena::GetQuestionResponse* response) override {
        return GetQuestionReactor::CreateGetTask(context, request, response, runtime.get(), qr_);
    }
};