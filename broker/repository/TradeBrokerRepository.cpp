#include <boost/asio/cancel_at.hpp>
#include <boost/asio/detail/chrono.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/mysql/connection_pool.hpp>
#include <boost/mysql/error_code.hpp>
#include <boost/mysql/impl/internal/sansio/run_pipeline.hpp>
#include <boost/mysql/pool_params.hpp>
#include <boost/mysql/results.hpp>
#include <boost/system/system_error.hpp>
#include <cstddef>
#include <cstdint>
#include <grpcpp/support/status.h>
#include <nlohmann/json.hpp>
#include <optional>
#include <spdlog/spdlog.h>
#include "TradeBrokerRepository.h"
#include "trade_broker.pb.h"
#include <boost/mysql/with_params.hpp>
#include <utility>

namespace {
    constexpr std::size_t kMaxGatewayNamespaceBytes = 255;
}

TradeBrokerRepository::TradeBrokerRepository(boost::mysql::pool_params params, boost::asio::io_context& ioc) :
    strand_(boost::asio::make_strand(ioc)),
    pool_(boost::mysql::connection_pool(strand_, std::move(params)))
{
    pool_.async_run([](boost::mysql::error_code code) {
        if (code) {
            SPDLOG_ERROR("MySQL connection pool stopped with error: {}.", code.message());
        } else {
            SPDLOG_INFO("MySQL connection pool stopped.");
        }
    });
}

void TradeBrokerRepository::stop()
{
    pool_.cancel();
}

TradeBrokerRepository::~TradeBrokerRepository()
{
    stop();
}

boost::asio::awaitable<std::pair<grpc::StatusCode, std::optional<SubmitOrderResponse>>>
    TradeBrokerRepository::async_submit_order(SubmitOrderRequest request)
{
    auto deadline = boost::asio::chrono::steady_clock::now() + std::chrono::seconds(3);
    SubmitOrderResponse res;

    // 防止溢出
    uint64_t cost = 0;
    if (__builtin_mul_overflow(request.quantity, request.limit_price_cent, &cost) || cost == 0) {
        co_return std::make_pair(grpc::StatusCode::INVALID_ARGUMENT, std::nullopt);
    }
    if (request.gateway_order_id == 0 ||
        request.gateway_namespace.empty() ||
        request.gateway_namespace.size() > kMaxGatewayNamespaceBytes ||
        request.account_id == 0 ||
        request.limit_price_cent == 0 ||
        request.quantity == 0 ||
        request.symbol.empty()) {
        co_return std::make_pair(grpc::StatusCode::INVALID_ARGUMENT, std::nullopt);
    }

    // 保存连接状态，方便后续异常情况后结束连接
    boost::mysql::pooled_connection conn;
    std::exception_ptr failure;
    bool closed_signal = false;
    try {
        conn = co_await pool_.async_get_connection(
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
        );
        boost::mysql::results result;
        // 开启事务
        co_await conn->async_execute("START TRANSACTION;", result,
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable));

        // 先将本次操作插入订单表 orders 中
        co_await conn->async_execute(
            boost::mysql::with_params(
                "SELECT broker_order_id, account_id, symbol, quantity, limit_price_cent, state "
                "FROM broker_orders WHERE gateway_namespace = {} AND gateway_order_id = {} for update;",
                request.gateway_namespace, request.gateway_order_id),
            result,
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
        );
        // 查出来已有原订单
        if (!result.rows().empty()) {
            auto broker_order_id = result.rows()[0][0].as_uint64();
            auto account_id = result.rows()[0][1].as_uint64();
            auto symbol = result.rows()[0][2].as_string();
            auto quantity = result.rows()[0][3].as_uint64();
            auto limit_price_cent = result.rows()[0][4].as_uint64();
            auto state_no = result.rows()[0][5].as_uint64();
            if (!broker::BrokerOrderState_IsValid(state_no) ||
                state_no == static_cast<uint8_t>(broker::BrokerOrderState::BROKER_ORDER_STATE_UNSPECIFIED)) {
                co_return std::make_pair(grpc::StatusCode::INTERNAL, std::nullopt);
            }
            auto state  = static_cast<broker::BrokerOrderState>(result.rows()[0][5].as_uint64());
            if (account_id != request.account_id || symbol != request.symbol ||
                quantity != request.quantity || limit_price_cent != request.limit_price_cent) {
                co_return std::make_pair(grpc::StatusCode::ALREADY_EXISTS, std::nullopt);
            }
            res.broker_order_id = broker_order_id;
            res.state = state;
            co_return std::make_pair(grpc::StatusCode::OK, std::move(res));
        }

        auto reason = BROKER_ORDER_REJECT_REASON::NORMAL;
        broker::BrokerOrderState state = broker::BrokerOrderState::BROKER_ORDER_STATE_ACCEPTED;

        // 查询购置的标号是否存在或是否有效
        co_await conn->async_execute(
            boost::mysql::with_params(
                "SELECT enabled FROM broker_instruments WHERE symbol = {};",
                request.symbol),
            result,
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
        );
        // 查出来标号不存在
        if (result.rows().empty()) {
            reason = BROKER_ORDER_REJECT_REASON::UNSUPPORT_SYMBOL;
            state = broker::BrokerOrderState::BROKER_ORDER_STATE_REJECTED;
        } else if (result.rows()[0][0].as_int64() != true) {
            reason = BROKER_ORDER_REJECT_REASON::SYMBOL_DISABLED;
            state = broker::BrokerOrderState::BROKER_ORDER_STATE_REJECTED;
        }

        // 将数据插入券商订单表
        boost::mysql::error_code ec;
        co_await conn->async_execute(
            boost::mysql::with_params(
                "INSERT INTO broker_orders (gateway_namespace, gateway_order_id, account_id, symbol, "
                "quantity, limit_price_cent, state, reject_reason) "
                "VALUES ({}, {}, {}, {}, {}, {}, {}, {});",
                request.gateway_namespace, request.gateway_order_id, request.account_id, request.symbol,
                request.quantity, request.limit_price_cent, static_cast<uint8_t>(state), std::string(reason)),
            result,
            boost::asio::cancel_at(deadline, boost::asio::redirect_error(boost::asio::use_awaitable, ec))
        );
        // 插入异常
        if (ec || result.affected_rows() != 1) {
            throw boost::system::system_error(ec);
        }

        res.broker_order_id = result.last_insert_id();
        res.state = state;

        co_await conn->async_execute("COMMIT;", result,
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
        co_return std::make_pair(grpc::StatusCode::OK, std::move(res));
    } catch (const boost::mysql::error_with_diagnostics& diag) {
        failure = std::current_exception();
        closed_signal = boost::mysql::is_fatal_error(diag.code());
        SPDLOG_ERROR("MySQL error: {}.", diag.what());
        SPDLOG_ERROR("Server message: {}", diag.get_diagnostics().server_message());
    } catch (const boost::system::system_error& e) {
        failure = std::current_exception();
        SPDLOG_ERROR("mysql system_error failed: {}", e.what());
        closed_signal = true;
    } catch (const std::exception& e) {
        failure = std::current_exception();
        closed_signal = true;
        SPDLOG_ERROR("mysql exception failed: {}", e.what());
    }
    if (failure) {
        if (conn.valid() && closed_signal) {
            // 判断为需要清理连接类型的异常错误
            boost::mysql::error_code close_ec;
            co_await conn->async_close(boost::asio::cancel_after(
                boost::asio::chrono::milliseconds(300),
                boost::asio::redirect_error(boost::asio::use_awaitable, close_ec)));
        }
        std::rethrow_exception(failure);
    }
    co_return std::make_pair(grpc::StatusCode::INTERNAL, std::nullopt);
}

boost::asio::awaitable<std::pair<grpc::StatusCode, std::optional<BrokerOrderInfo>>>
    TradeBrokerRepository::async_get_order(BrokerOrderClintId request)
{
    auto deadline = boost::asio::chrono::steady_clock::now() + std::chrono::seconds(3);
    SubmitOrderResponse res;

    if (request.gateway_order_id == 0 ||
        request.gateway_namespace.empty() ||
        request.gateway_namespace.size() > kMaxGatewayNamespaceBytes) {
        co_return std::make_pair(grpc::StatusCode::INVALID_ARGUMENT, std::nullopt);
    }

    // 保存连接状态，方便后续异常情况后结束连接
    boost::mysql::pooled_connection conn;
    std::exception_ptr failure;
    bool closed_signal = false;

    try {
        conn = co_await pool_.async_get_connection(
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
        );
        boost::mysql::results result;
        co_await conn->async_execute(
            boost::mysql::with_params("SELECT broker_order_id, gateway_namespace, gateway_order_id, "
            "account_id, symbol, quantity, limit_price_cent, state, filled_quantity, fill_price_cent, "
            "broker_version, reject_reason FROM broker_orders WHERE gateway_namespace = {} AND gateway_order_id = {};",
            request.gateway_namespace, request.gateway_order_id),
            result,
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
        );
        
        // 未查询到记录
        if (result.rows().empty()) {
            SPDLOG_WARN("Unfound broker order for namespace = {} and gateway_order_id = {}.", request.gateway_namespace, request.gateway_order_id);
            co_return std::make_pair(grpc::StatusCode::NOT_FOUND, std::nullopt);
        }

        auto row = result.rows()[0];
        auto state_no = row[7].as_uint64();
        if (!broker::BrokerOrderState_IsValid(state_no) ||
            state_no == static_cast<uint8_t>(broker::BrokerOrderState::BROKER_ORDER_STATE_UNSPECIFIED)) {
            // 状态码非法
            co_return std::make_pair(grpc::StatusCode::INTERNAL, std::nullopt);
        }

        BrokerOrderInfo res;
        res.broker_order_id = row[0].as_uint64();
        res.gateway_namespace = row[1].as_string();
        res.gateway_order_id = row[2].as_uint64();
        res.account_id = row[3].as_uint64();
        res.symbol = row[4].as_string();
        res.quantity = row[5].as_uint64();
        res.limit_price_cent = row[6].as_uint64();
        res.state = static_cast<broker::BrokerOrderState>(state_no);
        res.filled_quantity = row[8].as_uint64();
        res.fill_price_cent = row[9].as_uint64();
        res.broker_version = row[10].as_uint64();
        res.reject_reason = row[11].as_string();

        co_return std::make_pair(grpc::StatusCode::OK, res);
    } catch (const boost::mysql::error_with_diagnostics& diag) {
        failure = std::current_exception();
        closed_signal = boost::mysql::is_fatal_error(diag.code());
        SPDLOG_ERROR("MySQL error: {}.", diag.what());
        SPDLOG_ERROR("Server message: {}", diag.get_diagnostics().server_message());
    } catch (const boost::system::system_error& e) {
        failure = std::current_exception();
        SPDLOG_ERROR("mysql system_error failed: {}", e.what());
        closed_signal = true;
    } catch (const std::exception& e) {
        failure = std::current_exception();
        closed_signal = true;
        SPDLOG_ERROR("mysql exception failed: {}", e.what());
    }
    if (failure) {
        if (conn.valid() && closed_signal) {
            // 判断为需要清理连接类型的异常错误
            boost::mysql::error_code close_ec;
            co_await conn->async_close(boost::asio::cancel_after(
                boost::asio::chrono::milliseconds(300),
                boost::asio::redirect_error(boost::asio::use_awaitable, close_ec)));
        }
        std::rethrow_exception(failure);
    }
    co_return std::make_pair(grpc::StatusCode::INTERNAL, std::nullopt);
}

boost::asio::awaitable<std::pair<grpc::StatusCode, std::optional<BrokerOrderInfo>>>
    TradeBrokerRepository::async_cancel_order(BrokerOrderClintId request)
{
    auto deadline = boost::asio::chrono::steady_clock::now() + std::chrono::seconds(3);
    SubmitOrderResponse res;

    if (request.gateway_order_id == 0 ||
        request.gateway_namespace.empty() ||
        request.gateway_namespace.size() > kMaxGatewayNamespaceBytes) {
        co_return std::make_pair(grpc::StatusCode::INVALID_ARGUMENT, std::nullopt);
    }

    // 保存连接状态，方便后续异常情况后结束连接
    boost::mysql::pooled_connection conn;
    std::exception_ptr failure;
    bool closed_signal = false;

    try {
        conn = co_await pool_.async_get_connection(
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
        );
        boost::mysql::results result;
        co_await conn->async_execute("START TRANSACTION;", result,
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
        );
        // 更新订单状态为已取消
        co_await conn->async_execute(
            boost::mysql::with_params("UPDATE broker_orders SET state = {}, broker_version = broker_version + 1 "
                "WHERE gateway_namespace = {} AND gateway_order_id = {} AND state = {};",
            static_cast<uint16_t>(broker::BrokerOrderState::BROKER_ORDER_STATE_CANCELLED),
            request.gateway_namespace, request.gateway_order_id, static_cast<uint16_t>(broker::BrokerOrderState::BROKER_ORDER_STATE_ACCEPTED)),
            result,
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
        );

        // 再次将数据查出来
        co_await conn->async_execute(
            boost::mysql::with_params("SELECT broker_order_id, gateway_namespace, gateway_order_id, "
            "account_id, symbol, quantity, limit_price_cent, state, filled_quantity, fill_price_cent, "
            "broker_version, reject_reason FROM broker_orders WHERE gateway_namespace = {} AND gateway_order_id = {};",
            request.gateway_namespace, request.gateway_order_id),
            result,
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
        );

        // 未查询到记录，返回给前台 NOT_FOUND 信息，更新状态失败
        if (result.rows().empty()) {
            SPDLOG_WARN("Unfound broker order for namespace = {} and gateway_order_id = {}.", request.gateway_namespace, request.gateway_order_id);
            co_return std::make_pair(grpc::StatusCode::NOT_FOUND, std::nullopt);
        }

        // 如果返回当前状态，筛出状态为 UNSPECIFIED 和未知值的情况，返回INTERNAL
        auto row = result.rows()[0];
        auto state_no = row[7].as_uint64();
        if (!broker::BrokerOrderState_IsValid(state_no) ||
            state_no == static_cast<uint8_t>(broker::BrokerOrderState::BROKER_ORDER_STATE_UNSPECIFIED)) {
            co_return std::make_pair(grpc::StatusCode::INTERNAL, std::nullopt);
        }

        BrokerOrderInfo res;
        res.broker_order_id = row[0].as_uint64();
        res.gateway_namespace = row[1].as_string();
        res.gateway_order_id = row[2].as_uint64();
        res.account_id = row[3].as_uint64();
        res.symbol = row[4].as_string();
        res.quantity = row[5].as_uint64();
        res.limit_price_cent = row[6].as_uint64();
        res.state = static_cast<broker::BrokerOrderState>(state_no);
        res.filled_quantity = row[8].as_uint64();
        res.fill_price_cent = row[9].as_uint64();
        res.broker_version = row[10].as_uint64();
        res.reject_reason = row[11].as_string();

        co_await conn->async_execute("COMMIT;", result,
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
        );
        co_return std::make_pair(grpc::StatusCode::OK, res);
    } catch (const boost::mysql::error_with_diagnostics& diag) {
        failure = std::current_exception();
        closed_signal = boost::mysql::is_fatal_error(diag.code());
        SPDLOG_ERROR("MySQL error: {}.", diag.what());
        SPDLOG_ERROR("Server message: {}", diag.get_diagnostics().server_message());
    } catch (const boost::system::system_error& e) {
        failure = std::current_exception();
        SPDLOG_ERROR("mysql system_error failed: {}", e.what());
        closed_signal = true;
    } catch (const std::exception& e) {
        failure = std::current_exception();
        closed_signal = true;
        SPDLOG_ERROR("mysql exception failed: {}", e.what());
    }
    if (failure) {
        if (conn.valid() && closed_signal) {
            // 判断为需要清理连接类型的异常错误
            boost::mysql::error_code close_ec;
            co_await conn->async_close(boost::asio::cancel_after(
                boost::asio::chrono::milliseconds(300),
                boost::asio::redirect_error(boost::asio::use_awaitable, close_ec)));
        }
        std::rethrow_exception(failure);
    }
    co_return std::make_pair(grpc::StatusCode::INTERNAL, std::nullopt);
}