#pragma once


#include <boost/asio/awaitable.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <future>
#include "AsioRuntime.h"

class TradeBrokerRepository;
class BrokerExecutionWoker
{
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    TradeBrokerRepository* rp_;
    std::atomic<bool> running_;
    boost::asio::steady_timer timer_;
    std::future<void> stop_sync_wait_;

    static constexpr auto kWorkerExecuteInterval = std::chrono::milliseconds(300);
    static constexpr uint64_t kWorkerHandleLimit = 32;

    boost::asio::awaitable<void> run_loop();
public:
    BrokerExecutionWoker(AsioRuntime* runtime, TradeBrokerRepository* repository) :
        strand_(boost::asio::make_strand(runtime->get_io_context())),
        rp_(repository),
        timer_(strand_)
    {
        running_.store(false, std::memory_order_relaxed);
    }
     ~BrokerExecutionWoker();

    void start();
    void stop();
};