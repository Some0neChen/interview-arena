#include "OrderDispatcher.h"

boost::asio::awaitable<void> OrderDispatcher::run_loop() {
    while (running_.load(std::memory_order::relaxed)) {
        // 按本轮开始时间计算周期，查询耗时占用这 300ms；超过周期就不额外等待，也不并发补跑。
        auto deadline = boost::asio::chrono::steady_clock::now() + kOrderDispatcherExecuteInterval;
        timer_->expires_at(deadline);
        // 只捕获本轮查询异常，避免一次失败结束整个循环；等待放在 catch 之后，快失败也会等待。
        try {
            auto list = co_await rp_->async_list_handle_order(kOrderDispatcherDefaultHandleOrderLimit);
            for (const auto& [gateway_order_id, order_state] : list) {
                SPDLOG_INFO("id:{}, state:{}", gateway_order_id, static_cast<uint8_t>(order_state));
            }
        } catch (std::exception& e) {
            SPDLOG_ERROR("OrderDispatcher run loop exception : {}.", e.what());
        }
        boost::system::error_code ec;
        // stop 可能发生在查库期间，此时 cancel 没有定时等待可取消；恢复后不能再挂新的等待。
        if (!running_.load(std::memory_order::relaxed)) {
            co_return;
        }
        // redirect_error 将等待结果写入 ec；主动取消产生 operation_aborted，属于正常收尾。
        co_await timer_->async_wait(boost::asio::redirect_error(boost::asio::use_awaitable, ec));
        if (ec && ec != boost::asio::error::operation_aborted) {
            SPDLOG_WARN("OrderDispatcher run_loop timer wait error: {}", ec.message());
        }
    }
    co_return;
}

OrderDispatcher::~OrderDispatcher()
{
    // 主流程应在运行时关闭前显式 stop；析构只作兜底
    stop();
}

void OrderDispatcher::start() {
    // 提交协程前就置 true，exchange 同时返回旧值，防止协程尚未执行时重复启动并覆盖 future。
    if (running_.exchange(true, std::memory_order::relaxed)) {
        return;
    }
    // use_future 返回主循环的完成凭证；这里只保存，不 get，启动调用才能立即返回。
    stop_sync_wait_ = boost::asio::co_spawn(strand_, run_loop(), boost::asio::use_future);
}

void OrderDispatcher::stop() {
    // valid 表示还持有可 get 的结果，不表示已经完成；get 取走结果后 future 会失效，再 get 会出错
    // 不能用 running_ 为 false 判断已结束，它只是停止标志，查询可能仍在收尾。
    if (!stop_sync_wait_.valid()) {
        // 未启动过，或者之前的 stop 已经取走主循环的完成结果。
        return;
    }
    // 与 expires_at/async_wait 使用同一个 strand，避免并发操作 timer；cancel 不会取消 MySQL 查询。
    auto stop_loop = [this]() -> boost::asio::awaitable<void> {
        running_.store(false, std::memory_order::relaxed);
        this->timer_->cancel();
        co_return;
    };
    std::future<void> fut = boost::asio::co_spawn(strand_, stop_loop(), boost::asio::use_future);
    // 第一层等待：停止命令执行完，也保证本地 stop_loop 闭包存活到其协程结束。
    fut.get();
    try {
        // 第二层等待：run_loop 真正退出，才能销毁它使用的成员和 Repository；未捕获异常在此重抛。
        stop_sync_wait_.get();
    } catch (std::exception& e) {
        SPDLOG_ERROR("OrderDispatcher run_loop stop get future error: {}", e.what());
    }
}
