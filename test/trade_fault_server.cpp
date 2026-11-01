#include "AsioRuntime.h"
#include "TradeAccountRepository.h"
#include "TradeAccountServiceImpl.h"
#include "log_init.h"
#include <boost/mysql/ssl_mode.hpp>
#include <charconv>
#include <csignal>
#include <cstdlib>
#include <grpcpp/grpcpp.h>
#include <iostream>
#include <thread>

int main(int argc, char** argv) {
    if (argc != 3) return 2;
    unsigned int pool_size = 0, mysql_port = 0;
    const auto parse = [](const char* text, unsigned int& value) {
        const std::string input(text);
        const auto [end, ec] = std::from_chars(input.data(), input.data() + input.size(), value);
        return ec == std::errc{} && end == input.data() + input.size();
    };
    if (!parse(argv[1], pool_size) || !parse(argv[2], mysql_port) ||
        pool_size < 1 || pool_size > 4 || (mysql_port != 3306 && mysql_port != 13307)) return 2;
    const char* database = std::getenv("TRADE_MYSQL_DATABASE");
    if (!database || !std::string(database).starts_with("tradewritefault")) return 2;

    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGINT);
    sigaddset(&signals, SIGTERM);
    pthread_sigmask(SIG_BLOCK, &signals, nullptr);
    signal(SIGPIPE, SIG_IGN);
    InitLogging("diaglogs/serverLog", spdlog::level::info, spdlog::level::debug, spdlog::level::debug);

    AsioRuntime runtime(8);
    runtime.start();
    boost::mysql::pool_params params;
    params.server_address.emplace_host_and_port("127.0.0.1", mysql_port);
    params.username = "a";
    params.password = "111111";
    params.database = database;
    params.initial_size = pool_size;
    params.max_size = pool_size;
    params.thread_safe = true;
    // Only the loopback fault proxy needs readable MySQL packets; production TLS is unchanged.
    if (mysql_port == 13307) params.ssl = boost::mysql::ssl_mode::disable;
    params.retry_interval = std::chrono::milliseconds(100);
    params.connect_timeout = std::chrono::seconds(2);
    TradeAccountRepository repository(std::move(params), runtime.get_io_context());
    TradeAccountServiceImpl service(&runtime.get_io_context(), &repository);
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:13401", grpc::InsecureServerCredentials());
    builder.RegisterService(&service);
    auto server = builder.BuildAndStart();
    if (!server) {
        repository.stop();
        runtime.stop();
        return 2;
    }
    SPDLOG_INFO("Test server: pool={}, mysql_port={}, database={}", pool_size, mysql_port, database);
    std::thread signal_thread([&] {
        int received = 0;
        if (sigwait(&signals, &received) == 0) {
            server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(3));
            repository.stop();
            runtime.stop();
        }
    });
    server->Wait();
    signal_thread.join();
    server.reset();
    spdlog::shutdown();
}
