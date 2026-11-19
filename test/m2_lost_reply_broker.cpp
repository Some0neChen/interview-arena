#include "AsioRuntime.h"
#include "TradeBrokerRepository.h"
#include "trade_broker.grpc.pb.h"
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/mysql/pool_params.hpp>
#include <grpcpp/grpcpp.h>
#include <nlohmann/json.hpp>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <fstream>
#include <iostream>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>

// Test-only synchronous facade: real Repository/COMMIT, deliberately no successful reply.
class LostReplyBroker final : public broker::TradeBroker::Service {
    boost::asio::io_context& ioc_;
    TradeBrokerRepository& repository_;
    std::ofstream evidence_;
    std::mutex evidence_mutex_;
    std::atomic<bool> stopping_{false};

    void record(nlohmann::json event)
    {
        std::lock_guard lock(evidence_mutex_);
        evidence_ << event.dump() << std::endl;
    }

public:
    LostReplyBroker(boost::asio::io_context& ioc, TradeBrokerRepository& repository,
                    const char* evidence_path)
        : ioc_(ioc), repository_(repository), evidence_(evidence_path)
    {
        if (!evidence_) throw std::runtime_error("Cannot open evidence file");
    }

    void stop() { stopping_.store(true); }

    grpc::Status SubmitOrder(grpc::ServerContext* context,
                             const broker::SubmitOrderRequest* request,
                             broker::SubmitOrderResponse*) override
    {
        const auto begin = std::chrono::steady_clock::now();
        try {
            ::SubmitOrderRequest input{
                .gateway_namespace = request->gateway_namespace(),
                .gateway_order_id = request->gateway_order_id(),
                .account_id = request->account_id(),
                .symbol = request->symbol(),
                .limit_price_cent = request->limit_price_cent(),
                .quantity = request->quantity()
            };
            // Blocking this test RPC thread is intentional; the Asio worker remains independent.
            auto [code, result] = boost::asio::co_spawn(
                ioc_, repository_.async_submit_order(std::move(input)), boost::asio::use_future).get();
            if (code != grpc::StatusCode::OK || !result) {
                record({{"event", "repository_failed"}, {"grpc_code", static_cast<int>(code)}});
                return grpc::Status(grpc::StatusCode::INTERNAL, "Fault fixture could not accept order");
            }
            record({{"event", "committed_reply_withheld"},
                    {"gateway_namespace", request->gateway_namespace()},
                    {"gateway_order_id", request->gateway_order_id()},
                    {"broker_order_id", result->broker_order_id},
                    {"state", static_cast<int>(result->state)}});

            // Keep the RPC open until the production BrokerClient's own deadline expires.
            while (!context->IsCancelled() && !stopping_.load()) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
            }
            record({{"event", "withheld_call_finished"}, {"cancelled", context->IsCancelled()},
                    {"gateway_order_id", request->gateway_order_id()},
                    {"elapsed_ms", std::chrono::duration_cast<std::chrono::milliseconds>(
                        std::chrono::steady_clock::now() - begin).count()}});
            return grpc::Status(grpc::StatusCode::CANCELLED, "Successful reply intentionally withheld");
        } catch (const std::exception& error) {
            record({{"event", "fixture_error"}, {"message", error.what()}});
            return grpc::Status(grpc::StatusCode::INTERNAL, error.what());
        }
    }

    grpc::Status GetOrder(grpc::ServerContext*, const broker::GetBrokerOrderRequest* request,
                          broker::GetBrokerOrderResponse*) override
    {
        // Hold recovery at a deterministic observation point until the runner restarts the real broker.
        record({{"event", "confirmation_unavailable"},
                {"gateway_namespace", request->gateway_namespace()},
                {"gateway_order_id", request->gateway_order_id()}});
        return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Confirmation temporarily unavailable");
    }

    grpc::Status CancelOrder(grpc::ServerContext*, const broker::GetBrokerOrderRequest*,
                             broker::GetBrokerOrderResponse*) override
    {
        return grpc::Status(grpc::StatusCode::UNAVAILABLE, "Confirmation temporarily unavailable");
    }
};

int main(int argc, char** argv)
{
    try {
        const char* database = std::getenv("TRADE_MYSQL_DATABASE");
        if (argc != 2 || !database || !std::string(database).starts_with("tradewritefaultm2broker")) {
            throw std::invalid_argument("Requires evidence path and an isolated M2 broker database");
        }
        sigset_t signals;
        sigemptyset(&signals);
        sigaddset(&signals, SIGINT);
        sigaddset(&signals, SIGTERM);
        if (pthread_sigmask(SIG_BLOCK, &signals, nullptr) != 0) {
            throw std::runtime_error("Cannot block shutdown signals");
        }
        signal(SIGPIPE, SIG_IGN);

        AsioRuntime runtime(1);
        boost::mysql::pool_params params;
        params.server_address.emplace_host_and_port("127.0.0.1", 3306);
        params.initial_size = params.max_size = 1;
        params.thread_safe = true;
        params.username = "a";
        params.password = "111111";
        params.database = database;
        TradeBrokerRepository repository(std::move(params), runtime.get_io_context());
        runtime.start();
        LostReplyBroker service(runtime.get_io_context(), repository, argv[1]);
        grpc::ServerBuilder builder;
        const std::string address = "127.0.0.1:3666";
        builder.AddListeningPort(address, grpc::InsecureServerCredentials());
        builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 0);
        builder.RegisterService(address, &service);
        auto server = builder.BuildAndStart();
        if (!server) throw std::runtime_error("Cannot listen on fault fixture port");
        int received = 0;
        const int wait_error = sigwait(&signals, &received);
        service.stop();
        server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(3));
        server->Wait();
        server.reset();
        repository.stop();
        runtime.stop();
        if (wait_error != 0) throw std::runtime_error("sigwait failed");
        return 0;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 1;
    }
}
