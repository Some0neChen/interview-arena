#include "BrokerExecutionWoker.h"
#include "TradeBrokerRepository.h"

BrokerExecutionWoker::~BrokerExecutionWoker()
{
    stop();
}

boost::asio::awaitable<void> BrokerExecutionWoker::run_loop()
{
    while (running_.load(std::memory_order_relaxed)) {
        auto next_start_timepoint = std::chrono::steady_clock::now() + kWorkerExecuteInterval;

        try {
            auto list = co_await rp_->async_list_handle_order(kWorkerHandleLimit);
            for (const auto& [order_id, state] : list) {
                if (!running_.load(std::memory_order_relaxed)) {
                    co_return;
                }
                if (state != broker::BrokerOrderState::BROKER_ORDER_STATE_ACCEPTED) {
                    continue;
                }

                try {
                    co_await rp_->async_try_fill_order(order_id, state);
                } catch (std::exception& e) {
                    SPDLOG_ERROR("BrokerExecutionWoker run_loop order_id:{} handle occur exception:{}", order_id, e.what());
                }
            }
        } catch (std::exception& e) {
            SPDLOG_ERROR("BrokerExecutionWoker run_loop occur exception:{}", e.what());
        }

        if (!running_.load(std::memory_order_relaxed)) {
            co_return;
        }
        boost::system::error_code ec;
        timer_.expires_at(next_start_timepoint);
        co_await timer_.async_wait(boost::asio::redirect_error(boost::asio::use_awaitable, ec));
        if (ec && ec != boost::asio::error::operation_aborted) {
            SPDLOG_ERROR("OrderDispatcher run_loop timer wait error: {}", ec.message());
        }
    }
    co_return;
}

void BrokerExecutionWoker::start()
{
    if (running_.exchange(true, std::memory_order_relaxed)) {
        return;
    }
    this->stop_sync_wait_ = boost::asio::co_spawn(strand_, run_loop(), boost::asio::use_future);
}

void BrokerExecutionWoker::stop()
{
    if (!stop_sync_wait_.valid()) {
        return;
    }
    auto stop_loop = [this]() -> boost::asio::awaitable<void> {
        running_.store(false, std::memory_order_relaxed);
        this->timer_.cancel();
        co_return;
    };
    std::future<void> fut = boost::asio::co_spawn(strand_, stop_loop(), boost::asio::use_future);
    fut.get();
    try {
        stop_sync_wait_.get();
    } catch (std::exception& e) {
        SPDLOG_WARN("BrokerExecutionWoker stopping timer occur exception: {}", e.what());
    }
}