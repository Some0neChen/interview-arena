#include "interview_arena.grpc.pb.h"
#include "interview_arena.pb.h"
#include "log_init.h"
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/stub_options.h>
#include <iostream>
#include <memory>
#include <spdlog/common.h>
#include <string>
#include <thread>
#include <utility>

void HandleSignal(int signal) {

}

// 传入参数为question-id，deadline定时，mock模拟对端延迟耗时
int main(int argc, char** argv)
{
    InitLogging("./diaglogs/clientLog", spdlog::level::info, spdlog::level::debug, spdlog::level::info);

    uint64_t question_id = 0;
    uint32_t deadline_ms = 0;
    uint32_t mock_delay_ms = 0;
    uint64_t cancel_after_ms = 0;
    std::string server_addr = "127.0.0.1:13401";

    for (int i = 1; i < argc; ++i) {
        std::string_view arg(argv[i]);
        if (arg == "--question-id" && i + 1 < argc) {
            question_id = std::stoll(argv[++i]);
            continue;
        } else if (arg == "--deadline-ms" && i + 1 < argc) {
            deadline_ms = std::stol(argv[++i]);
            continue;
        } else if (arg == "--mock-delay-ms" && i + 1 < argc) {
            mock_delay_ms = std::stol(argv[++i]);
            continue;
        } else if (arg == "--server" && i + 1 < argc) {
            server_addr = argv[++i];
            continue;
        } else if (arg == "--cancel-after-ms") {
            cancel_after_ms = std::stoll(argv[++i]);
            continue;
        }
        else {
            std::cerr << "Unknown argument: " << arg << std::endl;
            exit(EXIT_FAILURE);
        }
    }
    // 建立连接，并为连接注册grpc服务
    auto channel = grpc::CreateChannel(server_addr, grpc::InsecureChannelCredentials());
    auto stub = arena::InterviewArena::NewStub(channel);

    arena::GetQuestionRequest request;
    request.set_question_id(question_id);
    request.set_mock_delay_ms(mock_delay_ms);

    std::shared_ptr<grpc::ClientContext> context = std::make_shared<grpc::ClientContext>();
    arena::GetQuestionResponse response;
    std::thread cancel_thread;

    if (deadline_ms > 0) {
        auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(deadline_ms);
        context->set_deadline(deadline);
    }
    if (cancel_after_ms > 0) {
        auto cancel_time = std::chrono::system_clock::now() + std::chrono::milliseconds(cancel_after_ms);
        cancel_thread = std::thread([clock = std::move(cancel_time), ct = std::weak_ptr<grpc::ClientContext>(context)]() {
            std::this_thread::sleep_until(clock);
            auto ct_shared = ct.lock();
            if (ct_shared) {
                ct_shared->TryCancel();
            }
        });
        cancel_thread.detach();
    }

    auto status = stub->GetQuestion(context.get(), request, &response);
    if (status.ok()) {
        std::cout << "Question ID: " << response.question_id() << std::endl;
        std::cout << "Title: " << response.title() << std::endl;
        std::cout << "Difficulty: " << arena::QuestionDifficulty_Name(response.difficulty()) << std::endl;
        std::cout << "Torture Score: " << response.torture_score() << std::endl;
        std::cout << "Source: " << response.source() << std::endl;
    } else {
        std::cerr << "gRPC failed with status code: " << status.error_code()
                  << ", message: " << status.error_message() << std::endl;
    }

    return 0;
}