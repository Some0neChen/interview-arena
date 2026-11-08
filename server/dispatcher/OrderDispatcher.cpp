#include "OrderDispatcher.h"
#include "TradeAccountRepository.h"
#include "trade_account.pb.h"
#include "trade_broker.pb.h"
#include <cstdint>
#include <functional>
#include <grpcpp/support/status.h>
#include <memory>
#include <spdlog/spdlog.h>
#include <utility>

boost::asio::awaitable<void> OrderDispatcher::run_loop() {
    while (running_.load(std::memory_order::relaxed)) {
        // 按本轮开始时间计算周期，查询耗时占用这 300ms；超过周期就不额外等待，也不并发补跑。
        auto deadline = boost::asio::chrono::steady_clock::now() + kOrderDispatcherExecuteInterval;
        timer_->expires_at(deadline);
        // 只捕获本轮查询异常，避免一次失败结束整个循环；等待放在 catch 之后，快失败也会等待。
        try {
            auto list = co_await rp_->async_list_handle_order(kOrderDispatcherDefaultHandleOrderLimit);
            for (const auto& [gateway_order_id, order_state] : list) {
                auto state_no = static_cast<uint16_t>(order_state);
                if (state_no >= dispatcher_handle_.size() || dispatcher_handle_[state_no] == nullptr) {
                    SPDLOG_INFO("id:{}, state:{} unfound int the service handles.", gateway_order_id, static_cast<uint8_t>(order_state));
                    continue;
                }
                co_await std::invoke(dispatcher_handle_[state_no], this, gateway_order_id);
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

void OrderDispatcher::applay_broker_response(uint64_t order_id, std::unique_ptr<OrderRefreshPkg>& pkg,
        broker::GetBrokerOrderResponse& response)
{
    pkg->order_id = order_id;
    if (response.state() == broker::BrokerOrderState::BROKER_ORDER_STATE_CANCELLED) {
        pkg->ledger_type = LEDGER_ENTRY_TYPE::ORDER_CANCELLED;
        pkg->outbox_type = OUTBOX_EVENT_TYPE::OUTBOX_EVENT_CANCELLED;
        pkg->unfreeze_cent = response.quantity() * response.limit_price_cent();
        pkg->target_state = trade::OrderState::ORDER_STATE_CANCELLED;
        pkg->spent_cent = 0;
    } else if (response.state() == broker::BrokerOrderState::BROKER_ORDER_STATE_REJECTED) {
        pkg->ledger_type = LEDGER_ENTRY_TYPE::ORDER_REJECTED;
        pkg->outbox_type = OUTBOX_EVENT_TYPE::OUTBOX_EVENT_REJECTED;
        pkg->unfreeze_cent = response.quantity() * response.limit_price_cent();
        pkg->target_state = trade::OrderState::ORDER_STATE_REJECTED;
        pkg->spent_cent = 0;
    } else if (response.state() == broker::BrokerOrderState::BROKER_ORDER_STATE_FILLED) {
        pkg->ledger_type = LEDGER_ENTRY_TYPE::ORDER_FILLED;
        pkg->outbox_type = OUTBOX_EVENT_TYPE::OUTBOX_EVENT_FILLED;
        pkg->target_state = trade::OrderState::ORDER_STATE_FILLED;
        pkg->unfreeze_cent = response.quantity() * response.limit_price_cent();
        pkg->spent_cent = response.fill_price_cent() * response.filled_quantity();
    }
}

boost::asio::awaitable<void> OrderDispatcher::dispatch_local_accepted_order(uint64_t order_id)
{
    auto submit_brief_info = co_await this->rp_->async_get_order_submit_brief(order_id);
    if (!submit_brief_info.has_value()) {
        SPDLOG_WARN("OrderDispatcher dispatch_local_accepted_order orderId:{} not found in db.", order_id);
        co_return;
    }
    auto request = std::make_shared<broker::SubmitOrderRequest>();
    request->set_gateway_namespace(this->dispatcher_namespace_);
    request->set_gateway_order_id(order_id);
    request->set_account_id(submit_brief_info->account_id);
    request->set_symbol(std::move(submit_brief_info->symbol));
    request->set_limit_price_cent(submit_brief_info->limit_price_cent);
    request->set_quantity(submit_brief_info->quantity);

    auto res = co_await this->broker_client_->async_submit_order(request);

    if (res.ret_code != grpc::StatusCode::OK || res.response.state() == broker::BrokerOrderState::BROKER_ORDER_STATE_UNSPECIFIED) {
        SPDLOG_WARN("OrderDispatcher dispatch_canceld_order orderId:{} unexpected error.", order_id);
        co_return;
    }
    if (res.response.state() == broker::BrokerOrderState::BROKER_ORDER_STATE_REJECTED) {
        auto pkg = std::make_unique<OrderRefreshPkg>();
        pkg->order_id = order_id;
        pkg->ledger_type = LEDGER_ENTRY_TYPE::ORDER_REJECTED;
        pkg->outbox_type = OUTBOX_EVENT_TYPE::OUTBOX_EVENT_REJECTED;
        pkg->spent_cent = 0;
        pkg->unfreeze_cent = submit_brief_info->limit_price_cent * submit_brief_info->quantity;
        pkg->target_state = trade::OrderState::ORDER_STATE_REJECTED;
        co_await this->rp_->async_dispatcher_refresh_data(std::move(pkg));
        co_return;
    }
    if (res.response.state() == broker::BrokerOrderState::BROKER_ORDER_STATE_ACCEPTED) {
        auto pkg = std::make_unique<OrderRefreshPkg>();
        pkg->order_id = order_id;
        pkg->ledger_type = LEDGER_ENTRY_TYPE::ORDER_NONE;
        pkg->outbox_type = OUTBOX_EVENT_TYPE::OUTBOX_EVENT_DISPATCHED;
        pkg->spent_cent = 0;
        pkg->unfreeze_cent = 0;
        pkg->target_state = trade::OrderState::ORDER_STATE_DISPATCHING;
        co_await this->rp_->async_dispatcher_refresh_data(std::move(pkg));
        co_return;
    }
    co_return;
}

boost::asio::awaitable<void> OrderDispatcher::dispatch_dispatching_order(uint64_t order_id)
{
    auto request = std::make_shared<broker::GetBrokerOrderRequest>();
    request->set_gateway_namespace(this->dispatcher_namespace_);
    request->set_gateway_order_id(order_id);

    auto res = co_await this->broker_client_->async_get_order(request);
    if (res.ret_code != grpc::StatusCode::OK || res.response.state() == broker::BrokerOrderState::BROKER_ORDER_STATE_UNSPECIFIED) {
        SPDLOG_WARN("OrderDispatcher dispatch_canceld_order orderId:{} unexpected error.", order_id);
        co_return;
    }
    if (res.response.state() == broker::BrokerOrderState::BROKER_ORDER_STATE_ACCEPTED) {
        co_return;
    }

    auto pkg = std::make_unique<OrderRefreshPkg>();
    applay_broker_response(order_id, pkg, res.response);

    co_await this->rp_->async_dispatcher_refresh_data(std::move(pkg));
    co_return;
}

boost::asio::awaitable<void> OrderDispatcher::dispatch_canceld_order(uint64_t order_id)
{
    auto request = std::make_shared<broker::GetBrokerOrderRequest>();
    request->set_gateway_namespace(this->dispatcher_namespace_);
    request->set_gateway_order_id(order_id);

    auto res = co_await this->broker_client_->async_cancel_order(request);
    if (res.ret_code != grpc::StatusCode::OK || res.response.state() == broker::BrokerOrderState::BROKER_ORDER_STATE_UNSPECIFIED) {
        SPDLOG_WARN("OrderDispatcher dispatch_canceld_order orderId:{} unexpected error.", order_id);
        co_return;
    }
    if (res.response.state() == broker::BrokerOrderState::BROKER_ORDER_STATE_ACCEPTED) {
        co_return;
    }

    auto pkg = std::make_unique<OrderRefreshPkg>();
    applay_broker_response(order_id, pkg, res.response);

    co_await this->rp_->async_dispatcher_refresh_data(std::move(pkg));
    co_return;
}