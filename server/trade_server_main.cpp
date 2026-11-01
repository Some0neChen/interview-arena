
#include "AsioRuntime.h"
#include "TradeAccountRepository.h"
#include <bits/types/sigset_t.h>
#include <boost/asio/detail/chrono.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/mysql/pool_params.hpp>
#include <csignal>
#include <cstdlib>
#include <grpc/grpc.h>
#include <grpcpp/resource_quota.h>
#include <grpcpp/security/server_credentials.h>
#include <grpcpp/server_builder.h>
#include <memory>
#include <thread>
#include "TradeAccountServiceImpl.h"
#include "log_init.h"

std::unique_ptr<grpc::Server> g_server;

std::unique_ptr<TradeAccountRepository> create_mysql_account_mgr(boost::asio::io_context& ioc)
{
    boost::mysql::pool_params params;
    const char* db_name = std::getenv("TRADE_MYSQL_DATABASE");
    if (db_name == nullptr || db_name[0] == '\0') {
        db_name = "arena";
    }
    params.server_address.emplace_host_and_port("127.0.0.1", 3306);
    params.initial_size = 1;
    params.max_size = 1;
    params.thread_safe = true;
    params.username = "a";
    params.password = "111111";
    params.database = db_name;
    SPDLOG_DEBUG("Create and init mysql QuestionRepository");
    return std::make_unique<TradeAccountRepository>(std::move(params), ioc);
}

int main(int argc, char** argv)
{
    // 注册退出信号，另起线程进行跟踪，在后续触发时在子线程进行阻塞，不插入原有主线程打断执行
    sigset_t stop_signals;
    sigemptyset(&stop_signals);
    sigaddset(&stop_signals, SIGINT);
    sigaddset(&stop_signals, SIGTERM);
    sigaddset(&stop_signals, SIGRTMIN);
    pthread_sigmask(SIG_BLOCK, &stop_signals, nullptr);
    
    // 忽略SIGPIPE信号，防止写管道时对方关闭导致进程被杀死
    signal(SIGPIPE, SIG_IGN);

    // 日志库初始化
    InitLogging("diaglogs/serverLog", spdlog::level::info, spdlog::level::debug, spdlog::level::debug);
    SPDLOG_INFO("InterviewArena Server Excute.");

    // 创建任务处理线程池
    auto runtime = std::make_unique<AsioRuntime>(8);
    runtime->start();

    // 创建 account 数据库管理类
    auto account_repository = create_mysql_account_mgr(runtime->get_io_context());

    // 创建 account 表管理服务
    auto account_service_handle = std::make_unique<TradeAccountServiceImpl>(&runtime->get_io_context(), account_repository.get());

    // 初始化grpc服务
    grpc::ServerBuilder builder;
    // 绑定grpc服务端监听端口
    std::string server_address{"127.0.0.1:13401"};
    builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
    // 注册account获取服务
    builder.RegisterService(server_address, account_service_handle.get());
    // 设置grpc处理线程池规模
    grpc::ResourceQuota quota;
    quota.SetMaxThreads(16);
    builder.SetResourceQuota(quota);
    // 设置底层 I/O CQ 轮询器数量与 Poller 线程数
    builder.SetSyncServerOption(grpc::ServerBuilder::NUM_CQS, 4);
    builder.SetSyncServerOption(grpc::ServerBuilder::MAX_POLLERS, 4);
    // 开启 SO_REUSEPORT 对全连接队列执行负载均衡
    builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, true);
    // 限制grpc单次消息规模
    builder.SetMaxMessageSize(4 * 1024 * 1024);

    g_server = builder.BuildAndStart();
    SPDLOG_INFO("[Server] Server listening on {}", server_address);
    // 用线程处理进程退出信号接收与处理
    std::thread signal_handle_thread([&stop_signals, &account_repository, &runtime]() {
        int signal = 0;
        // 同步等待信号触发后执行
        if (sigwait(&stop_signals, &signal) == 0) {
            // 关闭服务端
            // 给予三秒延三秒到期后取消剩余gRPC任务，对有定时等待的任务超过3s后也执行cancel
            g_server->Shutdown(boost::asio::chrono::system_clock::now() +  boost::asio::chrono::seconds(3));
            account_repository->stop();
            runtime->stop();
        }
    });

    g_server->Wait();
    SPDLOG_INFO("[Server] Server stopped.");
    if (signal_handle_thread.joinable()) {
        signal_handle_thread.join();
    }

    g_server.reset();
    account_service_handle.reset();
    spdlog::shutdown();
    return 0;
}