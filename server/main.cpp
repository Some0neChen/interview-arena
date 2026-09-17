#include <bits/types/sigset_t.h>
#include <chrono>
#include <csignal>
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
#include "AsioRuntime.h"
#include "log_init.h"
#include "InterviewArenaServiceImpl.h"

std::unique_ptr<grpc::Server> g_server;

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

    auto runtime = std::make_shared<AsioRuntime>(8);
    runtime->start();

    std::string server_address("127.0.0.1:13401");
    auto service_handle = std::make_unique<InterviewArenaServiceImpl>(runtime);
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
            // 关闭服务端
            // 给予三秒延迟，三秒一到直接关闭服务，防止里面还有定时任务长时间拖着等待
            g_server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(3));
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