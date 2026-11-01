#include "trade_account.grpc.pb.h"
#include "trade_account.pb.h"
#include "log_init.h"
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/stub_options.h>
#include <iostream>
#include <memory>
#include <optional>
#include <spdlog/common.h>
#include <string>
#include <thread>
#include <utility>
#include <vector>

void HandleSignal(int signal) {

}

enum RequestType {
    GET_ACCOUNT_INFO = 0,
    SUBMIT_ORDER,
    MAX_REQUEST_TYPE
};

enum RequestResult {
    OK = 0,
    ERR = 1
};

struct Request {
    uint64_t account_id = 0;
    uint64_t cancel_after_ms = 0;
    uint32_t deadline_ms = 0;
    uint32_t mock_delay_ms = 0;
    uint64_t limit_price_cent = 0;
    uint64_t quantity = 0;
    std::string client_order_id = "";
    std::string symbol = "";
    std::string server_addr = "127.0.0.1:13401";
    RequestType type = RequestType::GET_ACCOUNT_INFO;
};

void mock_delay_oper(Request& req, std::shared_ptr<grpc::ClientContext> context, std::thread& cancel_thread)
{
    if (req.deadline_ms > 0) {
        auto deadline = std::chrono::system_clock::now() + std::chrono::milliseconds(req.deadline_ms);
        context->set_deadline(deadline);
    }
    if (req.cancel_after_ms > 0) {
        auto cancel_time = std::chrono::system_clock::now() + std::chrono::milliseconds(req.cancel_after_ms);
        cancel_thread = std::thread([clock = std::move(cancel_time), ct = std::weak_ptr<grpc::ClientContext>(context)]() {
            std::this_thread::sleep_until(clock);
            auto ct_shared = ct.lock();
            if (ct_shared) {
                ct_shared->TryCancel();
            }
        });
        cancel_thread.detach();
    }
}

RequestResult GetAccountInfo(Request& req)
{
    // 建立连接，并为连接注册grpc服务
    auto channel = grpc::CreateChannel(req.server_addr, grpc::InsecureChannelCredentials());
    auto stub = trade::TradeAccount::NewStub(channel);

    trade::GetTradeAccountRequest request;
    request.set_account_id(req.account_id);
    request.set_mock_delay_ms(req.mock_delay_ms);

    std::shared_ptr<grpc::ClientContext> context = std::make_shared<grpc::ClientContext>();
    trade::GetTradeAccountResponse response;

    std::thread cancel_thread;
    mock_delay_oper(req, context, cancel_thread);

    auto status = stub->GetAccount(context.get(), request, &response);
    if (status.ok()) {
        std::cout << "Account ID            : " << response.account_id() << std::endl;
        std::cout << "Account Name          : " << response.account_name() << std::endl;
        std::cout << "Intial budget cent    : " << response.initial_budget_cent() << std::endl;
        std::cout << "Available cent        : " << response.available_cent() << std::endl;
        std::cout << "Frozen cent           : " << response.frozen_cent() << std::endl;
        std::cout << "Spent cent            : " << response.spent_cent() << std::endl;
    } else {
        std::cerr << "gRPC failed with status code: " << status.error_code()
                  << ", message: " << status.error_message() << std::endl;
    }

    return status.ok() ? RequestResult::OK : RequestResult::ERR;
}

// ./trade_client --service-type 1 --account-id 2 --limit-price-cent 100 --quantity 10 --client-order-id demo-006 --symbol demo
RequestResult SubmitOrder(Request& req)
{
    // 建立连接，并为连接注册grpc服务
    auto channel = grpc::CreateChannel(req.server_addr, grpc::InsecureChannelCredentials());
    auto stub = trade::TradeAccount::NewStub(channel);

    trade::SubmitOrderRequest request;
    request.set_account_id(req.account_id);
    request.set_client_order_id(req.client_order_id);
    request.set_symbol(req.symbol);
    request.set_limit_price_cent(req.limit_price_cent);
    request.set_quantity(req.quantity);

    std::shared_ptr<grpc::ClientContext> context = std::make_shared<grpc::ClientContext>();
    trade::SubmitOrderResponse response;

    std::thread cancel_thread;
    mock_delay_oper(req, context, cancel_thread);

    auto status = stub->SubmitOrder(context.get(), request, &response);
    if (status.ok()) {
        std::cout << "Order Id  : " << response.order_id() << std::endl;
        std::cout << "State     : " << 
            ((response.state() <= trade::OrderState::ORDER_STATE_CANCELLED) ? trade::OrderState_Name(response.state()) : "UnKnown") << std::endl;
    } else {
        std::cerr << "gRPC failed with status code: " << status.error_code()
                  << ", message: " << status.error_message() << std::endl;
    }

    return status.ok() ? RequestResult::OK : RequestResult::ERR;
}

RequestResult GetOrder(Request& req)
{
    // 建立连接，并为连接注册grpc服务
    auto channel = grpc::CreateChannel(req.server_addr, grpc::InsecureChannelCredentials());
    auto stub = trade::TradeAccount::NewStub(channel);

    trade::GetOrderRequest request;
    request.set_account_id(req.account_id);
    request.set_client_order_id(req.client_order_id);

    std::shared_ptr<grpc::ClientContext> context = std::make_shared<grpc::ClientContext>();
    trade::GetOrderResponse response;

    std::thread cancel_thread;
    mock_delay_oper(req, context, cancel_thread);

    auto status = stub->GetOrder(context.get(), request, &response);
    if (status.ok()) {
        std::cout << "Order Id          : " << response.order_id() << std::endl;
        std::cout << "Symbol            : " << response.symbol() << std::endl;
        std::cout << "Limit Price Cent  : " << response.limit_price_cent() << std::endl;
        std::cout << "Quantity          : " << response.quantity() << std::endl;
        std::cout << "Reserved Cent     : " << response.reserved_cent() << std::endl;
        std::cout << "State             : " << trade::OrderState_Name(response.state()) << std::endl;
        std::cout << "Version           : " << response.version() << std::endl;
    } else {
        std::cerr << "gRPC failed with status code: " << status.error_code()
                  << ", message: " << status.error_message() << std::endl;
    }

    return status.ok() ? RequestResult::OK : RequestResult::ERR;
}

RequestResult CancelOrer(Request& req)
{
    // 建立连接，并为连接注册grpc服务
    auto channel = grpc::CreateChannel(req.server_addr, grpc::InsecureChannelCredentials());
    auto stub = trade::TradeAccount::NewStub(channel);

    trade::CancelOrderRequest request;
    request.set_account_id(req.account_id);
    request.set_client_order_id(req.client_order_id);

    std::shared_ptr<grpc::ClientContext> context = std::make_shared<grpc::ClientContext>();
    trade::CancelOrderResponse response;

    std::thread cancel_thread;
    mock_delay_oper(req, context, cancel_thread);

    auto status = stub->CancelOrder(context.get(), request, &response);
    if (status.ok()) {
        std::cout << "Order Id          : " << response.order_id() << std::endl;
        std::cout << "State             : " << trade::OrderState_Name(response.state()) << std::endl;
    } else {
        std::cerr << "gRPC failed with status code: " << status.error_code()
                  << ", message: " << status.error_message() << std::endl;
    }

    return status.ok() ? RequestResult::OK : RequestResult::ERR;
}

std::vector<std::function<RequestResult(Request&)>> gloabl_grpc_handle = {
    GetAccountInfo,
    SubmitOrder,
    GetOrder,
    CancelOrer
};

std::optional<Request> get_input_request(int argc, char** argv) {
    Request req;
        for (int i = 1; i < argc; ++i) {
        std::string_view arg(argv[i]);
        if (arg == "--account-id" && i + 1 < argc) {
            req.account_id = std::stoll(argv[++i]);
            continue;
        } else if (arg == "--deadline-ms" && i + 1 < argc) {
            req.deadline_ms = std::stol(argv[++i]);
            continue;
        } else if (arg == "--mock-delay-ms" && i + 1 < argc) {
            req.mock_delay_ms = std::stol(argv[++i]);
            continue;
        } else if (arg == "--server" && i + 1 < argc) {
            req.server_addr = argv[++i];
            continue;
        } else if (arg == "--cancel-after-ms") {
            req.cancel_after_ms = std::stoll(argv[++i]);
            continue;
        } else if (arg == "--limit-price-cent") {
            req.limit_price_cent = std::stoll(argv[++i]);
            continue;
        } else if (arg == "--quantity") {
            req.quantity = std::stoll(argv[++i]);
            continue;
        }else if (arg == "--client-order-id") {
            req.client_order_id = argv[++i];
            continue;
        }else if (arg == "--symbol") {
            req.symbol = argv[++i];
            continue;
        }
        else if (arg == "--service-type") {
            auto type = std::stoul(argv[++i]);
            if (type >= gloabl_grpc_handle.size()) {
                std::cerr << "Unknown grpc request service_type: " << argv[i] << std::endl;
                return std::nullopt;
            }
            req.type = static_cast<RequestType>(type);
            continue;
        }
        else {
            std::cerr << "Unknown argument: " << arg << std::endl;
            return std::nullopt;
        }
    }
    return req;
}

// 传入参数为question-id，deadline定时，mock模拟对端延迟耗时
int main(int argc, char** argv)
{
    InitLogging("./diaglogs/clientLog", spdlog::level::info, spdlog::level::debug, spdlog::level::info);

    auto req = get_input_request(argc, argv);
    if (!req.has_value()) {
        exit(EXIT_FAILURE);
    }
    if (req->type > gloabl_grpc_handle.size()) {
        std::cerr << "Unknown grpc request type. " << std::endl;
        exit(EXIT_FAILURE);
    }
    return gloabl_grpc_handle[req->type](req.value());;
}