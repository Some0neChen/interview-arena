#include <bits/types/sigset_t.h>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <grpc/grpc.h>
#include <grpcpp/grpcpp.h>
#include <grpcpp/resource_quota.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server.h>
#include <grpcpp/server_builder.h>
#include <grpcpp/server_context.h>
#include <grpcpp/support/server_callback.h>
#include <grpcpp/support/status.h>
#include <memory>
#include <spdlog/common.h>
#include <spdlog/spdlog.h>
#include <thread>
#include "interview_arena.grpc.pb.h"
#include "interview_arena.pb.h"
#include "log_init.h"

class GetQuestionReactor final : public grpc::ServerUnaryReactor {
private:
    uint64_t question_id;
    uint32_t mock_delay_ms;
    arena::GetQuestionResponse* response;
    grpc::CallbackServerContext* context;

    void GetQuestion() {
        if (question_id == 0) {
            Finish(grpc::Status(grpc::StatusCode::INVALID_ARGUMENT, "Invalid question ID."));
            return;
        }
        response->set_question_id(question_id);
        response->set_title("Epoll ET模式为什么要读到EGAIN? 读到0是什么意思? EPOLLHUP和EpollRDHUP的区别是什么?");
        response->set_difficulty(arena::QuestionDifficulty::MEDIUM);
        response->set_torture_score(726);
        response->set_source("https://someonechen.interview.invincible.com/arena/question/0");
        uint32_t delay = 0; // 模拟延迟的毫秒数
        while (delay < mock_delay_ms) {
            if (context->IsCancelled()) {
                SPDLOG_INFO("Client cancelled the request.");
                Finish(grpc::Status(grpc::StatusCode::CANCELLED, "Client cancelled the request."));
                return;
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            delay += 20;
        }
        Finish(grpc::Status(grpc::Status::OK));
    }

    void async_get_question() {
        auto async_thread = std::thread([this]() {
            this->GetQuestion();
        });
        async_thread.detach();
    }

    GetQuestionReactor(uint64_t q_id, uint32_t delay_ms, arena::GetQuestionResponse* resp, grpc::CallbackServerContext* ctx) :
        question_id(q_id), mock_delay_ms(delay_ms), response(resp), context(ctx) {}

    ~GetQuestionReactor() override = default;
public:
    static GetQuestionReactor* CreateGetTask (grpc::CallbackServerContext* context, const arena::GetQuestionRequest* request, arena::GetQuestionResponse* response) {
        auto* reactor = new GetQuestionReactor(request->question_id(),
            request->mock_delay_ms(),
            response,
            context);
        reactor->async_get_question();
        return reactor;
    }

    void OnDone() override {
        delete this;
    }
};

class InterviewArenaServiceImpl final : public arena::InterviewArena::CallbackService {
public:
    grpc::ServerUnaryReactor* GetQuestion(grpc::CallbackServerContext* context, const arena::GetQuestionRequest* request, arena::GetQuestionResponse* response) override {
        return GetQuestionReactor::CreateGetTask(context, request, response);
    }
};

std::unique_ptr<grpc::Server> g_server;

void HandleSignal(int signal) {
    
    if (g_server) {
        
    }
}

int main() {
    // 注册退出信号，先选择无视，另起线程跟踪，不插入原有主线程打断执行
    sigset_t stop_signals;
    sigemptyset(&stop_signals);
    sigaddset(&stop_signals, SIGINT);
    sigaddset(&stop_signals, SIGRTMIN);
    sigaddset(&stop_signals, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &stop_signals, nullptr);

    // 忽略SIGPIPE信号，防止写管道时对方关闭导致进程被杀死
    signal(SIGPIPE, SIG_IGN);

    // 日志库初始化
    InitLogging("diaglogs/serverLog", spdlog::level::info, spdlog::level::debug, spdlog::level::debug);
    SPDLOG_INFO("InterviewArena Server Excute.");

    std::string server_address("127.0.0.1:13401");
    auto service_handle = std::make_unique<InterviewArenaServiceImpl>();
    grpc::ServerBuilder net_server_builder;

    // 绑定监听端口
    net_server_builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());

    // RPC服务注册
    net_server_builder.RegisterService(server_address, service_handle.get());

    // 设置线程池规模
    grpc::ResourceQuota quota;
    quota.SetMaxThreads(16);
    net_server_builder.SetResourceQuota(quota);

    // 设置底层 I/O CQ 轮询器数量与 Poller 线程数
    net_server_builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::NUM_CQS, 4);
    net_server_builder.SetSyncServerOption(grpc::ServerBuilder::SyncServerOption::MAX_POLLERS, 4);

    // 开启 Linux 内核级 SO_REUSEPORT 多全连接队列负载均衡
    net_server_builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, true);

    // 限制单次接收最大消息为4MB
    net_server_builder.SetMaxMessageSize(4 * 1024 * 1024);

    g_server = net_server_builder.BuildAndStart();
    SPDLOG_INFO("[Server] Server listening on {}", server_address);
    // 另起线程监听退出信号
    std::thread signal_handle_thread([&stop_signals]() {
        int recived_signal = 0;
        // 同步等待信号触发
        if (sigwait(&stop_signals, &recived_signal) == 0) {
            g_server->Shutdown();
        }
    });

    g_server->Wait();

    SPDLOG_INFO("[Server] Server stopped.");
    spdlog::shutdown();

    if (signal_handle_thread.joinable()) {
        signal_handle_thread.join();
    }
    return 0;
}