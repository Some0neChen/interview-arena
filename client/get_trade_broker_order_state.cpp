#include "AsioRuntime.h"
#include "trade_broker.grpc.pb.h"
#include "trade_broker.pb.h"
#include <boost/asio/async_result.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/this_coro.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <chrono>
#include <format>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <grpcpp/support/status.h>
#include <iostream>
#include <memory>
#include <string>
#include <utility>

struct AsyncClientGetOrderCall {
    grpc::ClientContext context;
    broker::GetBrokerOrderRequest request;
    broker::GetBrokerOrderResponse reponse;
};

boost::asio::awaitable<grpc::Status> async_get_broker_order_state(
    std::shared_ptr<AsyncClientGetOrderCall> call,
    std::unique_ptr<broker::TradeBroker::Stub>& stub)
{
    // 获取当前协程所在的io_context
    auto ex = co_await boost::asio::this_coro::executor;
    auto token = boost::asio::use_awaitable;

    // 自定义异步协程运行处理逻辑
    // completion_handler是后续asio::async_initiate隐式传入的参数，可用来唤醒外界等待co_await通知处理完成
    auto initiate = [&stub, call, ex](auto completion_handler) {
        // completion_handler不可拷贝，提前转为shared_ptr延长生命周期
        auto finish_notify = std::make_shared<decltype(completion_handler)>(std::move(completion_handler));
        stub->async()->GetOrder(&call->context, &call->request, &call->reponse, [ex, finish_notify](grpc::Status status) {
            boost::asio::post(ex, [finish_notify, status]() {
                // 相当于给外界co_await丢了返回值，就是status
                (*finish_notify)(status);
            });
        });
    };
    co_return co_await boost::asio::async_initiate<boost::asio::use_awaitable_t<>, void(grpc::Status)>(initiate, token);
}

int main(int argc, char** argv)
{
    auto gateway_order_id = std::stoull(argv[1]);
    auto gateway_namespace = std::string(argv[2]);

    auto channel = grpc::CreateChannel("127.0.0.1:3666", grpc::InsecureChannelCredentials());
    auto stub = broker::TradeBroker::NewStub(channel);

    auto async_call = std::make_shared<AsyncClientGetOrderCall>();
    async_call->request.set_gateway_order_id(gateway_order_id);
    async_call->request.set_gateway_namespace(std::move(gateway_namespace));
    async_call->context.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(3));

    AsioRuntime runtime(1);
    runtime.start();

    auto get_order_task = [async_call, &stub, &runtime]() -> boost::asio::awaitable<void> {
        auto ret_status = co_await async_get_broker_order_state(async_call, stub);
        if (!ret_status.ok()) {
            std::cout << "GetOrder failed: " << ret_status.error_message() << std::endl;
            co_return;
        }
        auto state = async_call->reponse.state();
        auto broker_order_id = async_call->reponse.broker_order_id();
        auto version = async_call->reponse.broker_version();
        std::cout << std::format("GetOrder success: state={}, broker_order_id={}, version={}.\n",
            broker::BrokerOrderState_Name(state), broker_order_id, version);
    };

    boost::asio::co_spawn(runtime.get_io_context(), get_order_task(),
        [](std::exception_ptr e) {
            if (!e) {
                return;
            }
            std::cerr << "mysql get broker order coroutine failed\n";
            return;
    });

    runtime.stop();
    return 0;
}