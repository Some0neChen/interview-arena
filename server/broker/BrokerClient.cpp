#include "BrokerClient.h"
#include <boost/asio/detail/chrono.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <grpcpp/create_channel.h>

BrokerClient::BrokerClient(const std::string& broker_address) {
    // 连接失败是否有兜底报错措施或者抛出异常？
    auto channel = grpc::CreateChannel(broker_address, grpc::InsecureChannelCredentials());
    stub_ = broker::TradeBroker::NewStub(channel);
}

boost::asio::awaitable<BrokerSubmitOrderResult>
BrokerClient::async_submit_order(std::shared_ptr<const broker::SubmitOrderRequest> request)
{
    auto call = std::make_shared<AsyncSubmitCall>();
    if (!stub_) {
        co_return BrokerSubmitOrderResult{.ret_code = grpc::StatusCode::INTERNAL, .response = std::move(call->response)};
    }
    if (!request) {
        co_return BrokerSubmitOrderResult{.ret_code = grpc::StatusCode::INVALID_ARGUMENT, .response = std::move(call->response)};
    }

    call->request = request;
    call->context.set_deadline(boost::asio::chrono::system_clock::now() + std::chrono::seconds(3));
    auto ex = co_await boost::asio::this_coro::executor;
    auto token = boost::asio::use_awaitable;

    auto initiate = [this, call, ex](auto completion_handler) mutable {
        auto finish_notify = std::make_shared<decltype(completion_handler)>(std::move(completion_handler));
        stub_->async()->SubmitOrder(&call->context, call->request.get(), &call->response, [call, finish_notify, ex](grpc::Status status) {
            boost::asio::post(ex, [call, finish_notify, status]() {
                BrokerSubmitOrderResult result;
                result.ret_code = status.error_code();  // 如果调用成功，这里就是 grpc::StatusCode::OK
                result.response = std::move(call->response);
                (*finish_notify)(std::move(result));
            });
            
        });
    };
    co_return co_await boost::asio::async_initiate<boost::asio::use_awaitable_t<>, void(BrokerSubmitOrderResult)>
        (initiate, token);
}

boost::asio::awaitable<BrokerOrderResult>
BrokerClient::async_get_order(std::shared_ptr<const broker::GetBrokerOrderRequest> request)
{
    auto call = std::make_shared<AsyncGetOrderCall>();
    if (!stub_) {
        co_return BrokerOrderResult{.ret_code = grpc::StatusCode::INTERNAL, .response = std::move(call->response)};
    }
    if (!request) {
        co_return BrokerOrderResult{.ret_code = grpc::StatusCode::INVALID_ARGUMENT, .response = std::move(call->response)};
    }

    call->request = request;
    call->context.set_deadline(boost::asio::chrono::system_clock::now() + std::chrono::seconds(3));
    auto ex = co_await boost::asio::this_coro::executor;
    auto token = boost::asio::use_awaitable;

    auto initiate = [this, call, ex](auto completion_handler) mutable {
        auto finish_notify = std::make_shared<decltype(completion_handler)>(std::move(completion_handler));
        stub_->async()->GetOrder(&call->context, call->request.get(), &call->response, [call, finish_notify, ex](grpc::Status status) {
            boost::asio::post(ex, [call, finish_notify, status]() {
                BrokerOrderResult result;
                result.ret_code = status.error_code();  // 如果调用成功，这里就是 grpc::StatusCode::OK
                result.response = std::move(call->response);
                (*finish_notify)(std::move(result));
            });
            
        });
    };
    co_return co_await boost::asio::async_initiate<boost::asio::use_awaitable_t<>, void(BrokerOrderResult)>
        (initiate, token);
}

boost::asio::awaitable<BrokerOrderResult>
BrokerClient::async_cancel_order(std::shared_ptr<const broker::GetBrokerOrderRequest> request)
{
    // 取消订单的实现与获取订单的请求结构体类似，这里还是用一样的请求结构体
    auto call = std::make_shared<AsyncGetOrderCall>();
    if (!stub_) {
        co_return BrokerOrderResult{.ret_code = grpc::StatusCode::INTERNAL, .response = std::move(call->response)};
    }
    if (!request) {
        co_return BrokerOrderResult{.ret_code = grpc::StatusCode::INVALID_ARGUMENT, .response = std::move(call->response)};
    }

    call->request = request;
    call->context.set_deadline(boost::asio::chrono::system_clock::now() + std::chrono::seconds(3));
    auto ex = co_await boost::asio::this_coro::executor;
    auto token = boost::asio::use_awaitable;

    auto initiate = [this, call, ex](auto completion_handler) mutable {
        auto finish_notify = std::make_shared<decltype(completion_handler)>(std::move(completion_handler));
        stub_->async()->CancelOrder(&call->context, call->request.get(), &call->response, [call, finish_notify, ex](grpc::Status status) {
            boost::asio::post(ex, [call, finish_notify, status]() {
                BrokerOrderResult result;
                result.ret_code = status.error_code();  // 如果调用成功，这里就是 grpc::StatusCode::OK
                result.response = std::move(call->response);
                (*finish_notify)(std::move(result));
            });
            
        });
    };
    co_return co_await boost::asio::async_initiate<boost::asio::use_awaitable_t<>, void(BrokerOrderResult)>
        (initiate, token);
}