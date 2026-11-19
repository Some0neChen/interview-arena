#include "trade_account.grpc.pb.h"
#include <grpcpp/grpcpp.h>
#include <nlohmann/json.hpp>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <iostream>
#include <stdexcept>
#include <string_view>

namespace {
std::uint64_t number(std::string_view text)
{
    std::uint64_t value{};
    const auto [end, error] = std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc{} || end != text.data() + text.size()) {
        throw std::invalid_argument("Expected an unsigned integer");
    }
    return value;
}
}

// A synchronous test driver, not the production BrokerClient coroutine bridge.
int main(int argc, char** argv)
{
    try {
        if (argc < 4) {
            throw std::invalid_argument(
                "Usage: m2_rpc_client endpoint account account_id | "
                "endpoint get|cancel account_id client_order_id | "
                "endpoint submit account_id client_order_id symbol price_cent quantity");
        }
        const std::string_view method(argv[2]);
        const auto account_id = number(argv[3]);
        auto stub = trade::TradeAccount::NewStub(
            grpc::CreateChannel(argv[1], grpc::InsecureChannelCredentials()));
        grpc::ClientContext context;
        context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(5));
        grpc::Status status;
        nlohmann::json result = nlohmann::json::object();
        const auto begin = std::chrono::steady_clock::now();
        if (method == "account" && argc == 4) {
            trade::GetTradeAccountRequest request;
            trade::GetTradeAccountResponse response;
            request.set_account_id(account_id);
            status = stub->GetAccount(&context, request, &response);
            if (status.ok()) {
                result = {{"account_id", response.account_id()},
                    {"initial_budget_cent", response.initial_budget_cent()},
                    {"available_cent", response.available_cent()},
                    {"frozen_cent", response.frozen_cent()}, {"spent_cent", response.spent_cent()}};
            }
        } else if (method == "get" && argc == 5) {
            trade::GetOrderRequest request;
            trade::GetOrderResponse response;
            request.set_account_id(account_id);
            request.set_client_order_id(argv[4]);
            status = stub->GetOrder(&context, request, &response);
            if (status.ok()) {
                result = {{"order_id", response.order_id()}, {"account_id", response.account_id()},
                    {"symbol", response.symbol()}, {"quantity", response.quantity()},
                    {"limit_price_cent", response.limit_price_cent()}, {"reserved_cent", response.reserved_cent()},
                    {"state", static_cast<int>(response.state())}, {"version", response.version()}};
            }
        } else if (method == "cancel" && argc == 5) {
            trade::CancelOrderRequest request;
            trade::CancelOrderResponse response;
            request.set_account_id(account_id);
            request.set_client_order_id(argv[4]);
            status = stub->CancelOrder(&context, request, &response);
            if (status.ok()) result = {{"order_id", response.order_id()}, {"state", static_cast<int>(response.state())}};
        } else if (method == "submit" && argc == 8) {
            trade::SubmitOrderRequest request;
            trade::SubmitOrderResponse response;
            request.set_account_id(account_id);
            request.set_client_order_id(argv[4]);
            request.set_symbol(argv[5]);
            request.set_limit_price_cent(number(argv[6]));
            request.set_quantity(number(argv[7]));
            status = stub->SubmitOrder(&context, request, &response);
            if (status.ok()) result = {{"order_id", response.order_id()}, {"state", static_cast<int>(response.state())}};
        } else {
            throw std::invalid_argument("Unknown command or incorrect argument count");
        }
        result["grpc_code"] = static_cast<int>(status.error_code());
        result["message"] = status.error_message();
        result["elapsed_ms"] = std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::steady_clock::now() - begin).count();
        std::cout << result.dump() << '\n';
        return status.ok() ? 0 : 1;
    } catch (const std::exception& error) {
        std::cerr << error.what() << '\n';
        return 2;
    }
}
