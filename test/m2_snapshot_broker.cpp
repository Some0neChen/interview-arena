#include "trade_broker.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <nlohmann/json.hpp>
#include <csignal>
#include <fstream>
#include <limits>
#include <map>
#include <mutex>
#include <stdexcept>
#include <string>

// Deliberately faulty remote peer. No real broker data is changed by this fixture.
class SnapshotBroker final : public broker::TradeBroker::Service {
    std::mutex mutex_;
    std::map<std::pair<std::string, std::uint64_t>, broker::SubmitOrderRequest> orders_;
    std::ofstream events_;
public:
    explicit SnapshotBroker(const char* file) : events_(file) {
        if (!events_) throw std::runtime_error("Cannot open snapshot evidence");
    }
    grpc::Status SubmitOrder(grpc::ServerContext*, const broker::SubmitOrderRequest* request,
                            broker::SubmitOrderResponse* response) override {
        std::lock_guard lock(mutex_);
        orders_.try_emplace({request->gateway_namespace(), request->gateway_order_id()}, *request);
        response->set_broker_order_id(request->gateway_order_id() + 10000);
        response->set_state(broker::BROKER_ORDER_STATE_ACCEPTED);
        return grpc::Status::OK;
    }
    grpc::Status GetOrder(grpc::ServerContext*, const broker::GetBrokerOrderRequest* request,
                         broker::GetBrokerOrderResponse* response) override {
        std::lock_guard lock(mutex_);
        auto it = orders_.find({request->gateway_namespace(), request->gateway_order_id()});
        if (it == orders_.end()) return {grpc::StatusCode::NOT_FOUND, "No fixture order"};
        const auto& original = it->second;
        const auto& mode = original.symbol();
        response->set_broker_order_id(original.gateway_order_id() + 10000);
        response->set_gateway_namespace(original.gateway_namespace());
        response->set_gateway_order_id(original.gateway_order_id());
        response->set_account_id(original.account_id());
        response->set_symbol(original.symbol());
        response->set_quantity(original.quantity());
        response->set_limit_price_cent(original.limit_price_cent());
        response->set_state(broker::BROKER_ORDER_STATE_FILLED);
        response->set_filled_quantity(10);
        response->set_fill_price_cent(280);
        response->set_broker_version(2);
        if (mode == "BAD.NAMESPACE") response->set_gateway_namespace("another-gateway");
        if (mode == "BAD.ORDER") response->set_gateway_order_id(original.gateway_order_id() + 999);
        if (mode == "BAD.ACCOUNT") response->set_account_id(original.account_id() + 999);
        if (mode == "BAD.SYMBOL") response->set_symbol("ANOTHER.SYMBOL");
        if (mode == "BAD.QUANTITY") response->set_quantity(11);
        if (mode == "BAD.LIMIT") response->set_limit_price_cent(301);
        if (mode == "BAD.BROKERID") response->set_broker_order_id(original.gateway_order_id() + 99999);
        if (mode == "BAD.ZEROID") response->set_broker_order_id(0);
        if (mode == "BAD.VERSION") response->set_broker_version(0);
        if (mode == "BAD.PARTIAL") response->set_filled_quantity(1);
        if (mode == "BAD.PRICE") {
            response->set_filled_quantity(5);
            response->set_fill_price_cent(500); // Total 2500 fits reserve; unit price violates limit 300.
        }
        if (mode == "BAD.STATE") response->set_state(static_cast<broker::BrokerOrderState>(99));
        if (mode == "BAD.ZEROQTY") response->set_filled_quantity(0);
        if (mode == "BAD.OVERBUDGET") response->set_fill_price_cent(400);
        if (mode == "BAD.OVERFLOW") response->set_filled_quantity(std::numeric_limits<std::uint64_t>::max());
        events_ << nlohmann::json({{"event", "snapshot_sent"}, {"mode", mode},
            {"local_id", original.gateway_order_id()}, {"remote_id", response->gateway_order_id()},
            {"account", response->account_id()}, {"broker_id", response->broker_order_id()},
            {"state", static_cast<int>(response->state())}, {"version", response->broker_version()},
            {"quantity", response->quantity()}, {"filled", response->filled_quantity()},
            {"price", response->fill_price_cent()}}).dump() << std::endl;
        return grpc::Status::OK;
    }
};

int main(int argc, char** argv) {
    if (argc != 2) return 2;
    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals, SIGINT);
    sigaddset(&signals, SIGTERM);
    if (pthread_sigmask(SIG_BLOCK, &signals, nullptr) != 0) return 2;
    SnapshotBroker service(argv[1]);
    grpc::ServerBuilder builder;
    const std::string address = "127.0.0.1:3666";
    builder.AddListeningPort(address, grpc::InsecureServerCredentials());
    builder.AddChannelArgument(GRPC_ARG_ALLOW_REUSEPORT, 0);
    builder.RegisterService(address, &service);
    auto server = builder.BuildAndStart();
    if (!server) return 2;
    int received = 0;
    const int error = sigwait(&signals, &received);
    server->Shutdown(std::chrono::system_clock::now() + std::chrono::seconds(3));
    server->Wait();
    server.reset();
    return error == 0 ? 0 : 2;
}
