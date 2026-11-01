#include "trade_broker.grpc.pb.h"

#include <charconv>
#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>

#include <grpcpp/grpcpp.h>
#include <nlohmann/json.hpp>

namespace {
std::uint64_t parse_number(std::string_view text) {
    std::uint64_t value = 0;
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc{} || end != text.data() + text.size()) {
        throw std::invalid_argument("Expected an unsigned integer: " + std::string(text));
    }
    return value;
}

void usage(const char* program) {
    std::cerr << "Usage: " << program
              << " endpoint gateway_namespace gateway_order_id account_id symbol limit_price_cent quantity\n"
              << "       " << program << " endpoint get|cancel gateway_namespace gateway_order_id\n"
              << "Sends one real broker RPC. Use an isolated test server/database.\n"
              << "Exit codes: 0 = RPC OK (inspect business state), 1 = RPC error, 2 = invalid CLI.\n";
}

int call_order_rpc(char** argv) {
    const std::string_view operation(argv[2]);
    if (operation != "get" && operation != "cancel") {
        throw std::invalid_argument("Expected get or cancel");
    }
    broker::GetBrokerOrderRequest request;
    request.set_gateway_namespace(argv[3]);
    request.set_gateway_order_id(parse_number(argv[4]));
    auto stub = broker::TradeBroker::NewStub(
        grpc::CreateChannel(argv[1], grpc::InsecureChannelCredentials()));
    grpc::ClientContext context;
    context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
    broker::GetBrokerOrderResponse response;
    const auto begin = std::chrono::steady_clock::now();
    const auto status = operation == "get"
        ? stub->GetOrder(&context, request, &response)
        : stub->CancelOrder(&context, request, &response);
    nlohmann::json output = {
        {"grpc_code", static_cast<int>(status.error_code())},
        {"grpc_message", status.error_message()},
        {"elapsed_ms", std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - begin).count()}
    };
    if (status.ok()) {
        output["broker_order_id"] = response.broker_order_id();
        output["gateway_namespace"] = response.gateway_namespace();
        output["gateway_order_id"] = response.gateway_order_id();
        output["account_id"] = response.account_id();
        output["symbol"] = response.symbol();
        output["quantity"] = response.quantity();
        output["limit_price_cent"] = response.limit_price_cent();
        output["state"] = static_cast<int>(response.state());
        output["state_name"] = broker::BrokerOrderState_Name(response.state());
        output["filled_quantity"] = response.filled_quantity();
        output["fill_price_cent"] = response.fill_price_cent();
        output["reject_reason"] = response.reject_reason();
        // Keep building before the missing version field is added to the contract.
        const auto* version = response.GetDescriptor()->FindFieldByName("broker_version");
        if (version && version->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_UINT64) {
            output["broker_version"] = response.GetReflection()->GetUInt64(response, version);
        }
    }
    std::cout << output.dump(2) << '\n';
    return status.ok() ? 0 : 1;
}
}  // namespace

int main(int argc, char** argv) {
    if (argc == 2 && std::string_view(argv[1]) == "--help") {
        usage(argv[0]);
        return 0;
    }
    if (argc != 8 && argc != 5) {
        usage(argv[0]);
        return 2;
    }
    try {
        if (argc == 5) return call_order_rpc(argv);
        broker::SubmitOrderRequest request;
        request.set_gateway_namespace(argv[2]);
        request.set_gateway_order_id(parse_number(argv[3]));
        request.set_account_it(parse_number(argv[4]));  // Current proto spelling.
        request.set_symbol(argv[5]);
        request.set_limit_price_cent(parse_number(argv[6]));
        request.set_quantity(parse_number(argv[7]));

        auto channel = grpc::CreateChannel(argv[1], grpc::InsecureChannelCredentials());
        auto stub = broker::TradeBroker::NewStub(channel);
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        broker::SubmitOrderResponse response;
        const auto begin = std::chrono::steady_clock::now();
        const auto status = stub->SubmitOrder(&context, request, &response);
        const auto elapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - begin).count();

        nlohmann::json output = {
            {"grpc_code", static_cast<int>(status.error_code())},
            {"grpc_message", status.error_message()},
            {"elapsed_ms", elapsed}
        };
        // A non-OK RPC does not supply an authoritative business result.
        if (status.ok()) {
            output["broker_order_id"] = response.broker_order_id();
            output["state"] = static_cast<int>(response.state());
            output["state_name"] = broker::BrokerOrderState_Name(response.state());
        }
        std::cout << output.dump(2) << '\n';
        return status.ok() ? 0 : 1;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 2;
    }
}
