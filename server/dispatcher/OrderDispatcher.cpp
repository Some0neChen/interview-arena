#include "OrderDispatcher.h"
#include "TradeAccountRepository.h"
#include "trade_account.pb.h"
#include "trade_broker.pb.h"
#include <atomic>
#include <cstdint>
#include <exception>
#include <functional>
#include <grpcpp/support/status.h>
#include <memory>
#include <optional>
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
            for (const auto& info : list) {
                if (!this->running_.load(std::memory_order_relaxed)) {
                    // 停机后直接退出
                    co_return;
                }
                auto state_no = static_cast<uint16_t>(info.state);
                if (state_no >= dispatcher_handle_.size() || dispatcher_handle_[state_no] == nullptr) {
                    SPDLOG_INFO("id:{}, state:{} unfound int the service handles.", info.order_id, static_cast<uint16_t>(info.state));
                    continue;
                }

                RET_CODE ret;
                try {
                    // 单独 try-catch 防止中途出错导致本次批量处理直接退出，不执行批次后续的状态处理
                    ret = co_await std::invoke(dispatcher_handle_[state_no], this, info.order_id);
                    if (ret == RET_CODE::OK && info.retry_count == 0) {
                        continue;
                    }
                } catch (const std::exception& e) {
                    ret = RET_CODE::ERR;
                    SPDLOG_INFO("OrderDispatcher handle id:{}, state:{} failed:{}.",
                        info.order_id, static_cast<uint16_t>(info.state), e.what());
                }
                // 确保上面 try-catch 后都能走此分支兜底
                try {
                    co_await rp_->async_update_retry_info(info.order_id, info.retry_count, ret);
                } catch (const std::exception& e) {
                    SPDLOG_INFO("OrderDispatcher try update retry info for id:{}, state:{} failed:{}.",
                        info.order_id, static_cast<uint16_t>(info.state), e.what());
                }
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

bool OrderDispatcher::apply_broker_response(uint64_t order_id, OrderRefreshPkg& pkg,
        broker::GetBrokerOrderResponse& response)
{
    pkg.order_id = order_id;
    if (response.state() == broker::BrokerOrderState::BROKER_ORDER_STATE_CANCELLED) {
        pkg.ledger_type = LEDGER_ENTRY_TYPE::ORDER_CANCELLED;
        pkg.outbox_type = OUTBOX_EVENT_TYPE::OUTBOX_EVENT_CANCELLED;
        pkg.unfreeze_cent = 0;
        pkg.target_state = trade::OrderState::ORDER_STATE_CANCELLED;
        pkg.spent_cent = 0;
    } else if (response.state() == broker::BrokerOrderState::BROKER_ORDER_STATE_REJECTED) {
        pkg.ledger_type = LEDGER_ENTRY_TYPE::ORDER_REJECTED;
        pkg.outbox_type = OUTBOX_EVENT_TYPE::OUTBOX_EVENT_REJECTED;
        pkg.unfreeze_cent = 0;
        pkg.target_state = trade::OrderState::ORDER_STATE_REJECTED;
        pkg.spent_cent = 0;
    } else if (response.state() == broker::BrokerOrderState::BROKER_ORDER_STATE_FILLED) {
        pkg.ledger_type = LEDGER_ENTRY_TYPE::ORDER_FILLED;
        pkg.outbox_type = OUTBOX_EVENT_TYPE::OUTBOX_EVENT_FILLED;
        pkg.target_state = trade::OrderState::ORDER_STATE_FILLED;
        pkg.unfreeze_cent = 0;
        uint64_t cost = 0;
        if (__builtin_mul_overflow(response.fill_price_cent(), response.filled_quantity(), &cost) || cost == 0) {
            return false;
        }
        pkg.spent_cent = cost;
    } else {
        return false;
    }
    return true;
}

boost::asio::awaitable<std::optional<LocalSubmitResult>> OrderDispatcher::async_submit_local_order(uint64_t order_id, bool fresh_order_state)
{
    auto submit_brief_info = co_await this->rp_->async_prepare_order_dispatch(order_id, fresh_order_state);
    if (!submit_brief_info.has_value()) {
        SPDLOG_WARN("OrderDispatcher dispatch_local_accepted_order orderId:{} not found in db.", order_id);
        co_return std::nullopt;
    }
    auto request = std::make_shared<broker::SubmitOrderRequest>();
    request->set_gateway_namespace(this->dispatcher_namespace_);
    request->set_gateway_order_id(order_id);
    request->set_account_id(submit_brief_info->account_id);
    request->set_symbol(std::move(submit_brief_info->symbol));
    request->set_limit_price_cent(submit_brief_info->limit_price_cent);
    request->set_quantity(submit_brief_info->quantity);

    LocalSubmitResult res;
    res.brief_info = std::move(submit_brief_info.value());
    res.broker_info = co_await this->broker_client_->async_submit_order(request);

    if (res.broker_info.ret_code != grpc::StatusCode::OK) {
        SPDLOG_WARN("OrderDispatcher dispatch_local_accepted_order orderId:{} submit by rpc failed.", order_id);
        co_return std::nullopt;
    }
    if (res.broker_info.response.broker_order_id() == 0) {
        SPDLOG_WARN("OrderDispatcher dispatch_local_accepted_order orderId:{} get submitted info faild,"
            " broker_order_id zero.", order_id);
        co_return std::nullopt;
    }
    auto flag = co_await rp_->async_bind_broker_order_id(order_id, res.broker_info.response.broker_order_id());
    if (!flag) {
        co_return std::nullopt;
    }

    co_return res;
}

void OrderDispatcher::format_broker_term(OrderRefreshPkg& pkg, broker::GetBrokerOrderResponse& response)
{
    pkg.broker_terms = BrokerOrderTerms{
        .account_id = response.account_id(),
        .broker_order_id = response.broker_order_id(),
        .quantity = response.quantity(),
        .limit_price_cent = response.limit_price_cent(),
        .broker_version = response.broker_version(),
        .reject_reason = std::move(response.reject_reason()),
        .symbol = response.symbol(),
        .broker_state = static_cast<uint8_t>(response.state())
    };
    pkg.fill_price_cent = response.fill_price_cent();
    pkg.filled_quntity = response.filled_quantity();
}

bool OrderDispatcher::validate_broker_feedback(uint64_t order_id, const broker::GetBrokerOrderResponse& response)
{
    return order_id == response.gateway_order_id() &&
        this->dispatcher_namespace_ == response.gateway_namespace() &&
        response.broker_order_id() != 0 &&
        response.broker_version() != 0;
}

boost::asio::awaitable<RET_CODE> OrderDispatcher::dispatch_local_accepted_order(uint64_t order_id)
{
    auto res = co_await this->async_submit_local_order(order_id);
    if (!res.has_value()) {
        co_return RET_CODE::SKIP;
    }

    if (res->broker_info.ret_code != grpc::StatusCode::OK || res->broker_info.response.state() == broker::BrokerOrderState::BROKER_ORDER_STATE_UNSPECIFIED) {
        SPDLOG_WARN("OrderDispatcher dispatch_local_accepted_order orderId:{} unexpected error.", order_id);
        co_return RET_CODE::ERR;
    }
    // if (res->broker_info.response.state() == broker::BrokerOrderState::BROKER_ORDER_STATE_REJECTED) {
    //     OrderRefreshPkg pkg{};
    //     pkg.order_id = order_id;
    //     pkg.ledger_type = LEDGER_ENTRY_TYPE::ORDER_REJECTED;
    //     pkg.outbox_type = OUTBOX_EVENT_TYPE::OUTBOX_EVENT_REJECTED;
    //     pkg.spent_cent = 0;
    //     pkg.unfreeze_cent = res->brief_info.reserved_cent;
    //     pkg.target_state = trade::OrderState::ORDER_STATE_REJECTED;
    //     co_await this->rp_->async_dispatcher_refresh_data(pkg);
    //     co_return;
    // }
    // if (res->broker_info.response.state() == broker::BrokerOrderState::BROKER_ORDER_STATE_ACCEPTED) {
    //     OrderRefreshPkg pkg{};
    //     pkg.order_id = order_id;
    //     pkg.ledger_type = LEDGER_ENTRY_TYPE::ORDER_NONE;
    //     pkg.outbox_type = OUTBOX_EVENT_TYPE::OUTBOX_EVENT_DISPATCHED;
    //     pkg.spent_cent = 0;
    //     pkg.unfreeze_cent = 0;
    //     pkg.target_state = trade::OrderState::ORDER_STATE_BROKER_ACCEPTED;
    //     co_await this->rp_->async_dispatcher_refresh_data(pkg);
    //     co_return;
    // }
    co_return RET_CODE::OK;
}

boost::asio::awaitable<RET_CODE> OrderDispatcher::dispatch_dispatching_order(uint64_t order_id)
{
    auto request = std::make_shared<broker::GetBrokerOrderRequest>();
    request->set_gateway_namespace(this->dispatcher_namespace_);
    request->set_gateway_order_id(order_id);

    auto res = co_await this->broker_client_->async_get_order(request);
    if (res.ret_code == grpc::StatusCode::NOT_FOUND) {
        // 上次派单未成功，重新派单
        co_return co_await this->dispatch_local_accepted_order(order_id);
    }
    if (res.ret_code != grpc::StatusCode::OK ||
        res.response.state() == broker::BrokerOrderState::BROKER_ORDER_STATE_UNSPECIFIED) {
        SPDLOG_WARN("OrderDispatcher dispatch_canceld_order orderId:{} unexpected error.", order_id);
        co_return RET_CODE::ERR;
    }
    if (!validate_broker_feedback(order_id, res.response)) {
        SPDLOG_WARN("OrderDispatcher dispatch_canceld_order orderId:{} async_get_order data invalid.", order_id);
        co_return RET_CODE::ERR;
    }
    if (res.response.gateway_namespace() != this->dispatcher_namespace_ || res.response.gateway_order_id() != order_id) {
        SPDLOG_WARN("OrderDispatcher dispatch_canceld_order broker data incorporate with local.", order_id);
        co_return RET_CODE::ERR;
    }

    OrderRefreshPkg pkg{};
    this->format_broker_term(pkg, res.response);
    if (res.response.state() == broker::BrokerOrderState::BROKER_ORDER_STATE_ACCEPTED) {
        // 之前发送 Dispatching 状态后中断业务，对端已接收但本地未更新
        pkg.ledger_type = LEDGER_ENTRY_TYPE::ORDER_NONE;
        pkg.outbox_type = OUTBOX_EVENT_TYPE::OUTBOX_EVENT_DISPATCHED;
        pkg.unfreeze_cent = 0;
        pkg.target_state = trade::OrderState::ORDER_STATE_BROKER_ACCEPTED;
        pkg.spent_cent = 0;
        pkg.order_id = order_id;
        co_return co_await rp_->async_dispatcher_refresh_data(pkg);
    }

    if (!apply_broker_response(order_id, pkg, res.response)) {
        SPDLOG_WARN("OrderDispatcher dispatch_dispatching_order orderId:{} get broker date error.", order_id);
        co_return RET_CODE::ERR;
    }

    co_return co_await this->rp_->async_dispatcher_refresh_data(pkg);
}

boost::asio::awaitable<RET_CODE> OrderDispatcher::dispatch_cancelled_order(uint64_t order_id)
{
    auto request = std::make_shared<broker::GetBrokerOrderRequest>();
    request->set_gateway_namespace(this->dispatcher_namespace_);
    request->set_gateway_order_id(order_id);

    auto res = co_await this->broker_client_->async_cancel_order(request);
    if (res.ret_code == grpc::StatusCode::NOT_FOUND) {
        // 上次派单未成功，此时下发请求撤单，对应处理逻辑为重新派单至broker留等下次再执行
        auto submit_brief = co_await this->async_submit_local_order(order_id, false);\
        co_return submit_brief.has_value() ? RET_CODE::OK : RET_CODE::ERR;
    }
    if (res.ret_code != grpc::StatusCode::OK || res.response.state() == broker::BrokerOrderState::BROKER_ORDER_STATE_UNSPECIFIED) {
        SPDLOG_WARN("OrderDispatcher dispatch_canceld_order orderId:{} unexpected error.", order_id);
        co_return RET_CODE::ERR;
    }
    if (!validate_broker_feedback(order_id, res.response)) {
        SPDLOG_WARN("OrderDispatcher dispatch_cancelled_order orderId:{} async_cancel_order broker data invalid.", order_id);
        co_return RET_CODE::ERR;
    }
    if (res.response.state() == broker::BrokerOrderState::BROKER_ORDER_STATE_ACCEPTED) {
        co_return RET_CODE::SKIP;
    }
    if (res.response.gateway_namespace() != this->dispatcher_namespace_ || res.response.gateway_order_id() != order_id) {
        SPDLOG_WARN("OrderDispatcher dispatch_cancelled_order broker data incorporate with local.", order_id);
        co_return RET_CODE::ERR;
    }

    OrderRefreshPkg pkg{};
    this->format_broker_term(pkg, res.response);
    if (!apply_broker_response(order_id, pkg, res.response)) {
        SPDLOG_WARN("OrderDispatcher dispatch_cancelled_order orderId:{} get broker date error.", order_id);
        co_return RET_CODE::ERR;
    }

    co_return co_await this->rp_->async_dispatcher_refresh_data(pkg);
}