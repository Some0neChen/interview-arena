#pragma once

#include "AsioRuntime.h"
#include "BrokerClient.h"
#include "TradeAccountRepository.h"
#include "trade_account.pb.h"
#include <array>
#include <atomic>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detached.hpp>
#include <boost/asio/error.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/asio/use_future.hpp>
#include <boost/system/detail/error_code.hpp>
#include <chrono>
#include <cstdint>
#include <future>
#include <memory>
#include <spdlog/spdlog.h>
#include <string>

// 订单服务中的后台扫描器
// 主流程先 start，返回后才允许 stop，二者不并发调用
// stop 会阻塞等待，必须在 Asio 工作线程之外调用，并在关闭连接池、运行时之前完成。
class OrderDispatcher {
    std::string dispatcher_namespace_;
    TradeAccountRepository* rp_;
    std::unique_ptr<BrokerClient> broker_client_;
    // 串行执行扫描和停止的代码片段；协程挂起时仍能处理停止操作，不独占一条线程。
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    // timer 作为成员，stop 才能取消当前等待；操作仍统一放在 strand 中。
    std::unique_ptr<boost::asio::steady_timer> timer_;
    // 对应整条 run_loop 的完成结果，而不是一次查询或一次停止命令。
    std::future<void> stop_sync_wait_;
    std::atomic<bool> running_;

    using DispatchHandleFunc = boost::asio::awaitable<void>(OrderDispatcher::*)(uint64_t);
    boost::asio::awaitable<void> dispatch_local_accepted_order(uint64_t order_id);
    boost::asio::awaitable<void> dispatch_dispatching_order(uint64_t order_id);
    boost::asio::awaitable<void> dispatch_canceld_order(uint64_t order_id);
    static inline constexpr auto dispatcher_handle = []() consteval {
        std::array<DispatchHandleFunc, trade::OrderState_ARRAYSIZE> table{};
        table[trade::OrderState::ORDER_STATE_LOCAL_ACCEPTED]    = &OrderDispatcher::dispatch_local_accepted_order;
        table[trade::OrderState::ORDER_STATE_DISPATCHING]       = &OrderDispatcher::dispatch_dispatching_order;
        table[trade::OrderState::ORDER_STATE_CANCELLED]         = &OrderDispatcher::dispatch_canceld_order;
        return table;
    }();

    boost::asio::awaitable<void> run_loop();

    

    static constexpr auto kOrderDispatcherExecuteInterval = std::chrono::milliseconds(300);
    static constexpr uint16_t kOrderDispatcherDefaultHandleOrderLimit = 32;
public:
    OrderDispatcher(std::string name, TradeAccountRepository* rp, AsioRuntime* runtime, const std::string& broker_address) :
        dispatcher_namespace_(name), rp_(rp), strand_(boost::asio::make_strand(runtime->get_io_context().get_executor())),
        broker_client_(std::make_unique<BrokerClient>(broker_address)),
        timer_(std::make_unique<boost::asio::steady_timer>(strand_))
    {
        running_.store(false, std::memory_order::relaxed);
    }

    ~OrderDispatcher();
    void start();
    void stop();
};
