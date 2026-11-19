#include "TradeAccountRepository.h"
#include "trade_account.pb.h"
#include "trade_broker.pb.h"
#include <array>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancel_after.hpp>
#include <boost/asio/cancel_at.hpp>
#include <boost/asio/detail/chrono.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/mysql/common_server_errc.hpp>
#include <boost/mysql/connection_pool.hpp>
#include <boost/mysql/error_code.hpp>
#include <boost/mysql/error_with_diagnostics.hpp>
#include <boost/mysql/impl/internal/sansio/run_pipeline.hpp>
#include <boost/mysql/results.hpp>
#include <boost/system/detail/error_code.hpp>
#include <boost/system/system_error.hpp>
#include <chrono>
#include <cstdint>
#include <exception>
#include <grpcpp/support/status.h>
#include <nlohmann/json.hpp>
#include <optional>
#include <spdlog/spdlog.h>
#include <type_traits>
#include <utility>
#include <vector>

boost::asio::awaitable<bool> TradeAccountRepository::check_account_exist_by_id(uint64_t id, boost::mysql::pooled_connection& conn,
    boost::asio::chrono::time_point<std::chrono::steady_clock>& deadline)
{
    boost::mysql::results result;
    co_await conn->async_execute(
        boost::mysql::with_params(
            "select count(1) from accounts where account_id = {};", id),
        result,
        boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
    co_return result.rows()[0][0].as_int64() > 0;
}

TradeAccountRepository::TradeAccountRepository(boost::mysql::pool_params params, boost::asio::io_context& ioc) :
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

void TradeAccountRepository::stop()
{
    pool_.cancel();
}

TradeAccountRepository::~TradeAccountRepository()
{
    stop();
}

template<typename Handler>
auto TradeAccountRepository::with_transaction(Handler&& handler)
    -> boost::asio::awaitable<typename std::invoke_result_t<Handler, boost::mysql::pooled_connection&, TimePoint&>::value_type::second_type>
{
    auto deadline = boost::asio::chrono::steady_clock::now() + kRepositoryExcutionLimitInterval;
    boost::mysql::pooled_connection conn;
    std::exception_ptr failure;
    bool closed_signal = false;
    // 第一个参数为执行后返回码，第二个为返回给外界的值
    using payload_t = typename std::invoke_result_t<Handler, boost::mysql::pooled_connection&, TimePoint&>::value_type::second_type;
    try {
        conn = co_await pool_.async_get_connection(
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
        boost::mysql::results result;
        co_await conn->async_execute("START TRANSACTION;", result,
                boost::asio::cancel_at(deadline, boost::asio::use_awaitable));

        auto [code, res] = co_await handler(conn, deadline);

        if (code == SQL_EXCUTED_CODE::OK) {
            co_await conn->async_execute("COMMIT;", result,
                boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
        } else {
            co_await conn->async_execute("ROLLBACK;", result,
                boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
        }
        co_return res;
    } catch (const boost::mysql::error_with_diagnostics& diag) {
        failure = std::current_exception();
        closed_signal = boost::mysql::is_fatal_error(diag.code());
        SPDLOG_ERROR("MySQL error: {}.", diag.what());
        SPDLOG_ERROR("Server message: {}", diag.get_diagnostics().server_message());
    } catch (const boost::system::system_error& e) {
        failure = std::current_exception();
        closed_signal = true;
        SPDLOG_ERROR("mysql system_error failed: {}", e.what());
    }
    catch (std::exception& e) {
        failure = std::current_exception();
        closed_signal = true;
        SPDLOG_ERROR("mysql exception failed: {}", e.what());
    }
    if (failure) {
        if (conn.valid() && closed_signal) {
            // 判断为需要清理连接类型的异常错误
            boost::system::error_code close_ec;
            co_await conn->async_close(boost::asio::cancel_after(
                std::chrono::milliseconds(300),
                boost::asio::redirect_error(boost::asio::use_awaitable, close_ec)));
        }
        std::rethrow_exception(failure);
    }
    co_return payload_t{};
}

boost::asio::awaitable<std::optional<Account>> TradeAccountRepository::async_get_account_by_id(uint64_t id)
{
    co_return co_await boost::asio::co_spawn(strand_, [this, id]() -> boost::asio::awaitable<std::optional<Account>> {
        // mysql协程任务只给三秒期限
        auto deadline = boost::asio::chrono::steady_clock::now() + std::chrono::seconds(3);
        // 保存连接状态，方便后续异常情况后结束连接
        boost::mysql::pooled_connection conn;
        std::exception_ptr failure;
        bool closed_signal = false;
        try {
            boost::mysql::results result;
            {
                conn = co_await this->pool_.async_get_connection(
                    boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
                );
                auto statement = co_await conn->async_prepare_statement(
                    "select account_id, account_name, initial_budget_cent, available_cent, frozen_cent, spent_cent from accounts where account_id = ?;",
                    boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
                );
                auto sql = statement.bind(id);
                co_await conn->async_execute(sql, result, 
                    boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
            }

            auto rows = result.rows();

            if (rows.empty()) {
                SPDLOG_DEBUG("Sql excute result is empty. questionid: {}.", id);
                // 这里不处理连接关闭，当前为持久化连接
                co_return std::nullopt;
            } else {
                Account account;
                // 当前暂且都统一取第一条数据
                account.account_id = rows[0][0].as_uint64();
                account.account_name = rows[0][1].as_string();
                account.initial_budget_cent = rows[0][2].as_uint64();
                account.available_cent = rows[0][3].as_uint64();
                account.frozen_cent = rows[0][4].as_uint64();
                account.spent_cent = rows[0][5].as_uint64();
                co_return account;
            }
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
        co_return std::nullopt;
    }, boost::asio::use_awaitable);
}

boost::asio::awaitable<std::pair<grpc::StatusCode, std::optional<SubmitResult>>> TradeAccountRepository::submit_order(uint64_t account_id, std::string client_order_id,
    std::string symbol, uint64_t quantity, uint64_t price)
{
    auto deadline = boost::asio::chrono::steady_clock::now() + std::chrono::seconds(3);
    SubmitResult res;
    res.order_id = 0;
    res.execute_state = trade::OrderState::ORDER_STATE_LOCAL_ACCEPTED;

    // 防止溢出
    uint64_t cost = 0;
    if (__builtin_mul_overflow(quantity, price, &cost) || cost == 0) {
        res.execute_state = trade::OrderState::ORDER_STATE_UNSPECIFIED;
        co_return std::make_pair(grpc::StatusCode::INVALID_ARGUMENT, std::move(res));
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
        boost::mysql::error_code ec;
        co_await conn->async_execute(
            boost::mysql::with_params(
                "insert into orders (account_id, client_order_id, symbol, limit_price_cent, quantity, reserved_cent, state, version) "
                "values({}, {}, {}, {}, {}, {}, {}, {});",
                account_id, client_order_id, symbol, price, quantity,
                cost, static_cast<uint16_t>(trade::OrderState::ORDER_STATE_LOCAL_ACCEPTED), 1),
            result,
            boost::asio::cancel_at(deadline, boost::asio::redirect_error(boost::asio::use_awaitable, ec))
        );

        if (ec == boost::mysql::common_server_errc::er_dup_entry) {
            // 客户端请求插入了重复键
            co_await conn->async_execute("ROLLBACK;", result, 
                boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
            // order_id 为自增，冲突为account_id和client_order_id都相同，因此查订单历史数据与当前是否一致判断是重复下单还是单号复用
            co_await conn->async_execute(
                boost::mysql::with_params(
                    "select symbol, limit_price_cent, quantity, state, order_id from orders where account_id = {} and client_order_id = {};",
                    account_id, client_order_id),
                result,
                boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
            );
            if (!result.rows().empty()) {
                if (result.rows()[0][0].as_string() == symbol &&
                    result.rows()[0][1].as_uint64() == price &&
                    result.rows()[0][2].as_uint64() == quantity) {
                    // 三个输入条件一致，说明当前为重复下单，还是返回OK，但不插入新语句，将历史订单信息进行返回
                    auto historic_state = result.rows()[0][3].as_uint64();
                    if (!trade::OrderState_IsValid(historic_state)) {
                        throw std::runtime_error("Unknown or unspecified order state");
                    }
                    res.execute_state = static_cast<trade::OrderState>(historic_state);
                    res.order_id = result.rows()[0][4].as_uint64();
                    co_return std::make_pair(grpc::StatusCode::OK, std::move(res));
                }
                // 误用历史单号进行下新单
                res.execute_state = trade::OrderState::ORDER_STATE_UNSPECIFIED;
                res.order_id = result.rows()[0][4].as_uint64();
                co_return std::make_pair(grpc::StatusCode::ALREADY_EXISTS, std::move(res));
            }
            res.execute_state = trade::OrderState::ORDER_STATE_UNSPECIFIED;
            res.order_id = 0;
            co_return std::make_pair(grpc::StatusCode::INTERNAL, std::move(res));
        } 
        else if (ec) {
            throw boost::system::system_error(ec);
        }

        if (result.affected_rows() != 1) {
            // 插入出现非预期情况
            SPDLOG_ERROR("Insert order failed: affected rows is not 1.");
            co_await conn->async_execute("ROLLBACK;", result,
                boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
            res.execute_state = trade::OrderState::ORDER_STATE_UNSPECIFIED;
            co_return std::make_pair(grpc::StatusCode::INTERNAL, std::move(res));
        }

        // 记录自递增成功插入的订单id
        res.order_id = result.last_insert_id();

        // 将本次变化记入ledger簿
        co_await conn->async_execute(
            boost::mysql::with_params(
                "insert into fund_ledger (account_id, order_id, entry_type, amount_cent) "
                "values({}, {}, {}, {});",
                account_id, res.order_id, cast_type(LEDGER_ENTRY_TYPE::ORDER_FREEZE), cost),
            result,
            boost::asio::cancel_at(deadline, boost::asio::redirect_error(boost::asio::use_awaitable, ec))
        );

        if (ec == boost::mysql::common_server_errc::er_dup_entry) {
            // ledger表里已经有这条订单记录，但是order表里未体现
            co_await conn->async_execute("ROLLBACK;", result,
                boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
            res.execute_state = trade::OrderState::ORDER_STATE_UNSPECIFIED;
            co_return std::make_pair(grpc::StatusCode::INTERNAL, std::move(res));
        }
        else if (ec) {
            throw boost::system::system_error(ec);
        }

        if (result.affected_rows() != 1) {
            // 插入出现非预期情况
            SPDLOG_ERROR("Insert order failed: affected rows is not 1.");
            co_await conn->async_execute("ROLLBACK;", result,
                boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
            res.execute_state = trade::OrderState::ORDER_STATE_UNSPECIFIED;
            co_return std::make_pair(grpc::StatusCode::FAILED_PRECONDITION, std::move(res));
        }

        // 将记录落盘至对外通知 outbox_events 表中
        nlohmann::ordered_json payload = {
            {"account_id", account_id},
            {"client_order_id", client_order_id},
            {"symbol", symbol},
            {"limit_price_cent", price},
            {"quantity", quantity},
            {"reserved_cent", cost},
            {"state", static_cast<int>(trade::OrderState::ORDER_STATE_LOCAL_ACCEPTED)}
        };
        co_await conn->async_execute(
            boost::mysql::with_params(
                "insert into outbox_events (order_id, order_version, event_type, payload) "
                "values({}, {}, {}, {});",
                res.order_id, 1, cast_type(OUTBOX_EVENT_TYPE::OUTBOX_EVENT_RECEIVED), payload.dump()),
            result,
            boost::asio::cancel_at(deadline, boost::asio::redirect_error(boost::asio::use_awaitable, ec))
        );
        if (ec == boost::mysql::common_server_errc::er_dup_entry) {
            // ledger表里已经有order_id, order_version相同的记录，重复插入错误
            co_await conn->async_execute("ROLLBACK;", result,
                boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
            res.execute_state = trade::OrderState::ORDER_STATE_UNSPECIFIED;
            co_return std::make_pair(grpc::StatusCode::INTERNAL, std::move(res));
        }
        else if (ec) {
            throw boost::system::system_error(ec);
        }

        // 使用with_params减少一次预发送prepare包，直接以sql语句进行请求
        // 一次性对accounts数据库里数据进行操作，并加上校验
        co_await conn->async_execute(
            boost::mysql::with_params(
                "update accounts set available_cent = available_cent - {}, frozen_cent = frozen_cent + {} where account_id = {} and available_cent >= {};",
                cost, cost, account_id, cost),
            result,
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
        if (result.affected_rows() != 1) {
            // 表内没有匹配的行进行修改，回滚事务，退出查询
            // 根据当前account表里是否有对应数据判断是账户不存在，还是余额不足
            co_await conn->async_execute("ROLLBACK;", result,
                boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
            res.execute_state = trade::OrderState::ORDER_STATE_UNSPECIFIED;
            auto ret_code = co_await check_account_exist_by_id(account_id, conn, deadline) ?
                grpc::StatusCode::FAILED_PRECONDITION : grpc::StatusCode::NOT_FOUND;;
            co_return std::make_pair(ret_code, std::move(res));
        }

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

boost::asio::awaitable<std::pair<grpc::StatusCode, std::optional<Order>>> TradeAccountRepository::async_get_order(uint64_t account_id, const std::string& client_order_id)
{
    Order res;
    auto deadline = boost::asio::chrono::steady_clock::now() + boost::asio::chrono::seconds(3);
    // 保存连接状态，方便后续异常情况后结束连接
    boost::mysql::pooled_connection conn;
    std::exception_ptr failure;
    bool closed_signal = false;
    try {
        conn = co_await pool_.async_get_connection(
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
        );
        boost::mysql::results result;

        // 查询获取orders表中单条记录
        boost::mysql::error_code ec;
        co_await conn->async_execute(
            boost::mysql::with_params(
                "select order_id, symbol, account_id, limit_price_cent, quantity, reserved_cent, state, version from orders "
                "where account_id = {} and client_order_id = {};",
                account_id, client_order_id),
            result,
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
        );

        if (result.rows().empty()) {
            co_return std::make_pair(grpc::StatusCode::NOT_FOUND, std::nullopt);
        }

        auto row = result.rows()[0];
        res.order_id = row[0].as_uint64();
        res.symbol = row[1].as_string();
        res.account_id = row[2].as_uint64();
        res.limit_price_cent = row[3].as_uint64();
        res.quantity = row[4].as_uint64();
        res.reserved_cent = row[5].as_uint64();
        auto state_val = row[6].as_uint64();
        res.state = static_cast<trade::OrderState>(row[6].as_uint64());
        res.version = row[7].as_uint64();

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

boost::asio::awaitable<std::optional<OrderSubmitBrief>> TradeAccountRepository::async_prepare_order_dispatch(uint64_t order_id, bool refresh_order_state)
{
    OrderSubmitBrief res;
    auto deadline = boost::asio::chrono::steady_clock::now() + boost::asio::chrono::seconds(3);
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

        // 查询获取orders表中单条记录
        boost::mysql::error_code ec;
        co_await conn->async_execute(
            boost::mysql::with_params(
                "SELECT account_id, symbol, limit_price_cent, quantity, reserved_cent, state, "
                "client_order_id, version FROM orders WHERE order_id = {} FOR UPDATE;",
                order_id),
            result,
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
        );
        if (result.rows().empty()) {
            co_return std::nullopt;
        }
        auto row = result.rows()[0];
        res.account_id = row[0].as_uint64();
        res.symbol = row[1].as_string();
        res.limit_price_cent = row[2].as_uint64();
        res.quantity = row[3].as_uint64();
        res.reserved_cent = row[4].as_uint64();
        auto state = row[5].as_uint64();
        if (!refresh_order_state && state != cast_type(trade::OrderState::ORDER_STATE_CANCEL_PENDING)) {
            // 当前不刷新本地状态只放行取消本地但远程broker未接收信号的情况
            co_return std::nullopt;
        }
        if (state == cast_type(trade::OrderState::ORDER_STATE_DISPATCHING)) {
            // 继续完成上次未完成的派发
            co_return res;
        }
        auto client_order_id = std::string(row[6].as_string());
        auto version = row[7].as_uint64();
        ++version;

        if (refresh_order_state) {
            // 刷新订单状态为 DISPATCHING，如果没有该条记录或者状态不为LOCAL_ACCEPTED就返回空值
            co_await conn->async_execute(
                boost::mysql::with_params(
                    "UPDATE orders SET state = {}, version = {} WHERE order_id = {} AND state = {};",
                    trade::OrderState::ORDER_STATE_DISPATCHING, version, order_id, trade::OrderState::ORDER_STATE_LOCAL_ACCEPTED),
                result,
                boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
            );
            if (result.affected_rows() != 1) {
                co_return std::nullopt;
            }

            // 插入 outbox_events 撤单记录
            nlohmann::ordered_json payload = {
                {"account_id", res.account_id},
                {"client_order_id", client_order_id},
                {"symbol", res.symbol},
                {"limit_price_cent", res.limit_price_cent},
                {"quantity", res.quantity},
                {"reserved_cent", res.reserved_cent},
                {"state", static_cast<int>(trade::OrderState::ORDER_STATE_DISPATCHING)}
            };
            co_await conn->async_execute(
                boost::mysql::with_params(
                    "INSERT INTO outbox_events (order_id, order_version, event_type, payload) "
                    "VALUES({}, {}, {}, {});",
                    order_id, version, cast_type(OUTBOX_EVENT_TYPE::OUTBOX_EVENT_DISPATCH_STARTED), payload.dump()),
                result,
                boost::asio::cancel_at(deadline, boost::asio::redirect_error(boost::asio::use_awaitable, ec))
            );
            if (ec && ec != boost::mysql::common_server_errc::er_dup_entry) {
                throw boost::system::system_error(ec);
            } else if (ec == boost::mysql::common_server_errc::er_dup_entry || result.affected_rows() != 1) {
                co_await conn->async_execute("ROLLBACK;", result,
                    boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
                SPDLOG_WARN("TradeAccountRepository async_prepare_order_dispatch insert outbox_events to record dispatching started "
                    "for order_id[{}] affected_rows[{}] unnormal.",
                    order_id, result.affected_rows());
                co_return std::nullopt;
            } 
        }
        

        co_await conn->async_execute("COMMIT;", result,
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
        );

        co_return res;
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
    co_return std::nullopt;
}

boost::asio::awaitable<std::pair<grpc::StatusCode, std::optional<CancelOrderResult>>> TradeAccountRepository::async_cancel_order(
    uint64_t account_id, const std::string& client_order_id)
{
    CancelOrderResult res;
    // 保存连接状态，方便后续异常情况后结束连接
    boost::mysql::pooled_connection conn;
    std::exception_ptr failure;
    bool closed_signal = false;
    auto deadline = boost::asio::chrono::steady_clock::now() + std::chrono::seconds(3);
    try {
        conn = co_await pool_.async_get_connection(
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
        );
        boost::mysql::results result;

        // 开启事务
        co_await conn->async_execute("START TRANSACTION;", result,
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable));

        // 查询获取orders表中历史订单记录
        boost::mysql::error_code ec;
        co_await conn->async_execute(
            boost::mysql::with_params(
                "select order_id, symbol, limit_price_cent, quantity, reserved_cent, state, version from orders "
                "where account_id = {} and client_order_id = {} for update;",
                account_id, client_order_id),
            result,
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
        );

        if (result.rows().empty()) {
            SPDLOG_WARN("TradeAccountRepository async_cancel_order search order for account_id[{}] client_order_id[{}] is empty.",
                account_id, client_order_id);
            co_return std::make_pair(grpc::StatusCode::NOT_FOUND, std::nullopt);
        }

        auto row = result.rows().front();
        auto order_id = row[0].as_uint64();
        std::string symbol(row[1].as_string());
        auto limit_price_cent = row[2].as_uint64();
        auto quantity = row[3].as_uint64();
        auto reserved_cent = row[4].as_uint64();
        auto state_num = row[5].as_uint64();
        if (!trade::OrderState_IsValid(state_num)) {
            throw std::runtime_error("Unknown or unspecified order state");
        }
        auto state = static_cast<trade::OrderState>(state_num);
        auto version = row[6].as_uint64();
        res.order_id = order_id;

        if (state == trade::OrderState::ORDER_STATE_CANCELLED || state == trade::OrderState::ORDER_STATE_CANCEL_PENDING) {
            res.order_state = state; 
            co_return std::make_pair(grpc::StatusCode::OK, res);
        }
        if (state != trade::OrderState::ORDER_STATE_LOCAL_ACCEPTED &&
            state != trade::OrderState::ORDER_STATE_BROKER_ACCEPTED &&
            state != trade::OrderState::ORDER_STATE_DISPATCHING) {
            SPDLOG_WARN("TradeAccountRepository async_cancel_order order for account_id[{}] client_order_id[{}] state was ORDER_STATE_LOCAL_ACCEPTED.",
                account_id, client_order_id);
            co_return std::make_pair(grpc::StatusCode::FAILED_PRECONDITION, std::nullopt);
        }

        if (state == trade::OrderState::ORDER_STATE_DISPATCHING || state == trade::OrderState::ORDER_STATE_BROKER_ACCEPTED) {
            // 当前此两种状态下取消订单，登记取消请求，不直接回撤金额
            co_await conn->async_execute(
            boost::mysql::with_params(
                "UPDATE orders SET state = {}, version = version + 1 where order_id = {};",
                cast_type(trade::OrderState::ORDER_STATE_CANCEL_PENDING), order_id),
            result,
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
            if (result.affected_rows() != 1) {
                // 主键不匹配，未知错误
                co_await conn->async_execute("ROLLBACK;", result,
                    boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
                SPDLOG_WARN("TradeAccountRepository async_cancel_order update orders state for order_id[{}] affected_rows[{}] unnormal.",
                    order_id, result.affected_rows());
                co_return std::make_pair(grpc::StatusCode::INTERNAL, std::nullopt);
            }
            co_await conn->async_execute("COMMIT;", result,
                boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
            res.order_state = trade::OrderState::ORDER_STATE_CANCEL_PENDING;
            co_return std::make_pair(grpc::StatusCode::OK, std::move(res));
        }

        // 以下状态为 LOCAL_ACCEPTED，直接回退金额

        // 更新订单状态为撤销，并刷新版本号
        ++version;
        co_await conn->async_execute(
            boost::mysql::with_params(
                "update orders set version = {}, state = {} where order_id = {};",
                version, static_cast<uint16_t>(trade::OrderState::ORDER_STATE_CANCELLED), order_id),
            result,
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
        if (result.affected_rows() != 1) {
            // 主键不匹配，未知错误
            co_await conn->async_execute("ROLLBACK;", result,
                boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
            SPDLOG_WARN("TradeAccountRepository async_cancel_order update orders for order_id[{}] affected_rows[{}] unnormal.",
                order_id, result.affected_rows());
            co_return std::make_pair(grpc::StatusCode::INTERNAL, std::nullopt);
        }

        // 恢复账户的冻结金额
        co_await conn->async_execute(
            boost::mysql::with_params(
                "update accounts set frozen_cent = frozen_cent - {}, available_cent = available_cent + {} "
                "where account_id = {} and frozen_cent >= {};",
                reserved_cent, reserved_cent, account_id, reserved_cent),
            result,
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
        if (result.affected_rows() != 1) {
            // 主键不匹配，或者账户显示冻结金额小于订单表里的花费金额，判定为未知错误
            co_await conn->async_execute("ROLLBACK;", result,
                boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
            SPDLOG_WARN("TradeAccountRepository async_cancel_order update accounts for "
                "account_id[{}] client_order_id[{}] affected_rows[{}] is unnormal.",
                account_id, client_order_id, result.affected_rows());
            co_return std::make_pair(grpc::StatusCode::INTERNAL, std::nullopt);
        }

        // 新增fund_ledger撤单记录
        co_await conn->async_execute(
            boost::mysql::with_params(
                "insert into fund_ledger (account_id, order_id, entry_type, amount_cent) "
                "values({}, {}, {}, {});",
                account_id, order_id, cast_type(LEDGER_ENTRY_TYPE::ORDER_CANCELLED), reserved_cent),
            result,
            boost::asio::cancel_at(deadline, boost::asio::redirect_error(boost::asio::use_awaitable, ec))
        );
        if (ec == boost::mysql::common_server_errc::er_dup_entry) {
            // ledger 表里已经有这条订单记录，但是 order 表里未体现
            co_await conn->async_execute("ROLLBACK;", result,
                boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
            SPDLOG_WARN("TradeAccountRepository async_cancel_order update fund_ledger for "
                "account_id[{}] client_order_id[{}] affected_rows[{}] is unnormal.",
                account_id, client_order_id, result.affected_rows());
            co_return std::make_pair(grpc::StatusCode::INTERNAL, std::nullopt);
        }
        else if (ec) {
            throw boost::system::system_error(ec);
        }

        // 插入 outbox_events 撤单记录
        nlohmann::ordered_json payload = {
            {"account_id", account_id},
            {"client_order_id", client_order_id},
            {"symbol", symbol},
            {"limit_price_cent", limit_price_cent},
            {"quantity", quantity},
            {"reserved_cent", reserved_cent},
            {"state", static_cast<int>(trade::OrderState::ORDER_STATE_CANCELLED)}
        };
        co_await conn->async_execute(
            boost::mysql::with_params(
                "insert into outbox_events (order_id, order_version, event_type, payload) "
                "values({}, {}, {}, {});",
                order_id, version, cast_type(OUTBOX_EVENT_TYPE::OUTBOX_EVENT_CANCELLED), payload.dump()),
            result,
            boost::asio::cancel_at(deadline, boost::asio::redirect_error(boost::asio::use_awaitable, ec))
        );
        if (ec == boost::mysql::common_server_errc::er_dup_entry) {
            // ledger表里已经有 order_id, order_version 相同的撤单记录，重复插入错误
            co_await conn->async_execute("ROLLBACK;", result,
                boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
            SPDLOG_WARN("TradeAccountRepository async_cancel_order update outbox_events for "
                "account_id[{}] client_order_id[{}] affected_rows[{}] is unnormal.",
                account_id, client_order_id, result.affected_rows());
            co_return std::make_pair(grpc::StatusCode::INTERNAL, std::nullopt);
        }
        else if (ec) {
            throw boost::system::system_error(ec);
        }

        res.order_state = trade::OrderState::ORDER_STATE_CANCELLED;
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

boost::asio::awaitable<std::vector<OrderStatusInfo>> TradeAccountRepository::async_list_handle_order(uint16_t limit)
{
    std::vector<OrderStatusInfo> res;
    // if (limit == 0) {
    //     co_return 
    // }
    auto deadline = boost::asio::chrono::steady_clock::now() + boost::asio::chrono::seconds(3);
    // 保存连接状态，方便后续异常情况后结束连接
    boost::mysql::pooled_connection conn;
    std::exception_ptr failure;
    bool closed_signal = false;
    try {
        conn = co_await pool_.async_get_connection(
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
        );
        boost::mysql::results result;

        if (this->scan_upper_id_ == 0) {
            // 查当前最大单号，刷新扫描范围
            co_await conn->async_execute("SELECT order_id FROM orders ORDER BY order_id DESC LIMIT 1;", result,
                boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
            );
            if (result.rows().empty()) {
                co_return res;
            }
            this->scan_upper_id_ = result.rows()[0][0].as_uint64();
            this->scan_after_id_ = 0;
        }

        // 查询获取orders表中需要派单器跟踪刷新的记录，查询状态为非最终态且下次刷新时间小于当前时间的order_id
        // 时间比对精度为三个小数点
        co_await conn->async_execute(
            boost::mysql::with_params(
                "SELECT order_id, state, retry_count FROM orders "
                "WHERE state IN ({}, {}, {}, {}) AND order_id > {} AND order_id <= {} "
                "AND (next_retry_at IS NULL OR next_retry_at <= CURRENT_TIMESTAMP(3)) ORDER BY order_id ASC LIMIT {};",
                cast_type(trade::OrderState::ORDER_STATE_LOCAL_ACCEPTED),
                cast_type(trade::OrderState::ORDER_STATE_DISPATCHING),
                cast_type(trade::OrderState::ORDER_STATE_CANCEL_PENDING),
                cast_type(trade::OrderState::ORDER_STATE_BROKER_ACCEPTED),
                this->scan_after_id_, this->scan_upper_id_, limit),
            result,
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
        );

        if (result.rows().empty()) {
            this->scan_upper_id_ = 0;
            this->scan_after_id_ = 0;
            co_return res;
        }

        auto row_size = result.rows().size();
        res.reserve(row_size);

        for (const auto& row : result.rows()) {
            res.emplace_back(
                OrderStatusInfo {
                    .order_id = row[0].as_uint64(),
                    .retry_count = row[2].as_uint64(),
                    .state = static_cast<trade::OrderState>(row[1].as_uint64()),
                }
            );
        }
        if (res.size() < limit || res.empty()) {
            // 触及扫描上限，重新从头扫描
            this->scan_upper_id_ = 0;
            this->scan_after_id_ = 0;
        } else {
            // 以当前最后一个元素为左开边界，下次继续往后搜，不反复处理前面的 order_id
            this->scan_after_id_ = res.back().order_id;
        }

        co_return res;
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
    co_return res;
}

BrokerUpdateDecision TradeAccountRepository::decide_broker_state_update(trade::OrderState cur_state, trade::OrderState target_state)
{
    using S = trade::OrderState;
    switch (target_state) {
        case S::ORDER_STATE_BROKER_ACCEPTED: {
            switch (cur_state) {
                case S::ORDER_STATE_DISPATCHING:
                    return BrokerUpdateDecision::APPLY;
                case S::ORDER_STATE_CANCEL_PENDING:
                case S::ORDER_STATE_BROKER_ACCEPTED:
                case S::ORDER_STATE_FILLED:
                case S::ORDER_STATE_CANCELLED:
                case S::ORDER_STATE_REJECTED:
                    return BrokerUpdateDecision::IGNORE;
                default:
                    return BrokerUpdateDecision::REJECT;
            }
        }
        case S::ORDER_STATE_CANCELLED:
        case S::ORDER_STATE_FILLED:
        case S::ORDER_STATE_REJECTED: {
            switch (cur_state) {
                case S::ORDER_STATE_BROKER_ACCEPTED:
                case S::ORDER_STATE_CANCEL_PENDING:
                case S::ORDER_STATE_DISPATCHING:
                    return BrokerUpdateDecision::APPLY;
                case S::ORDER_STATE_FILLED:
                case S::ORDER_STATE_CANCELLED:
                case S::ORDER_STATE_REJECTED:
                    return cur_state == target_state ? BrokerUpdateDecision::IGNORE : BrokerUpdateDecision::REJECT;
                default:
                    return BrokerUpdateDecision::REJECT;
            }
        }
        default:
            return BrokerUpdateDecision::REJECT;
    }
}
BrokerUpdateDecision TradeAccountRepository::decide_broker_snapshot_update(OrderRefreshPkg& pkg, uint64_t broker_version, uint8_t broker_state, uint64_t filled_quantiy,
        uint64_t fill_price_cent, std::string reject_reason)
{
    if (broker_version == 0) {
        return BrokerUpdateDecision::APPLY;
    }

    if (pkg.broker_terms.broker_version < broker_version) {
        return BrokerUpdateDecision::IGNORE;
    }

    if (pkg.broker_terms.broker_version == broker_version) {
        return (pkg.broker_terms.broker_state == broker_state &&
            pkg.fill_price_cent == fill_price_cent &&
            pkg.filled_quntity == filled_quantiy &&
            pkg.broker_terms.reject_reason == reject_reason) ? BrokerUpdateDecision::IGNORE : BrokerUpdateDecision::REJECT;
    }

    return (broker_state == cast_type(broker::BrokerOrderState::BROKER_ORDER_STATE_ACCEPTED) &&
        pkg.broker_terms.broker_state != cast_type(broker::BrokerOrderState::BROKER_ORDER_STATE_ACCEPTED)) ?
        BrokerUpdateDecision::APPLY : BrokerUpdateDecision::REJECT;
}

boost::asio::awaitable<RET_CODE> TradeAccountRepository::async_dispatcher_refresh_data(OrderRefreshPkg refresh_pkg)
{
    auto deadline = boost::asio::chrono::steady_clock::now() + boost::asio::chrono::seconds(3);
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
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable));

        // 查询获取orders表中单条记录
        boost::mysql::error_code ec;
        co_await conn->async_execute(
            boost::mysql::with_params(
                "SELECT account_id, client_order_id, symbol, limit_price_cent, quantity, reserved_cent, state, version, "
                "broker_order_id, broker_version, broker_state, filled_quantity, fill_price_cent, reject_reason FROM orders "
                "WHERE order_id = {} FOR UPDATE;",
                refresh_pkg.order_id),
            result,
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
        );

        if (result.rows().empty()) {
            SPDLOG_ERROR("The dispacher handle order_id={} unexisted!", refresh_pkg.order_id);
            co_return RET_CODE::ERR;
        }

        auto order = result.rows()[0];
        auto account_id = order[0].as_uint64();
        auto client_order_id = std::string(order[1].as_string());
        auto symbol = std::string(order[2].as_string());
        auto limit_price_cent = order[3].as_uint64();
        auto quantity = order[4].as_uint64();
        auto reserved_cent = order[5].as_uint64();
        auto state = order[6].as_uint64();
        auto version = order[7].as_uint64();
        auto broker_version = order[9].as_uint64();
        auto broker_state = order[10].as_uint64();
        auto filled_quantity = order[11].as_uint64();
        auto fill_price_cent = order[12].as_uint64();
        auto reject_reason = std::string(order[13].as_string());
        ++version;
        uint64_t broker_order_id;
        if (order[8].is_null()) {
            broker_order_id = refresh_pkg.broker_terms.broker_order_id;
            co_await conn->async_execute(
                boost::mysql::with_params(
                    "UPDATE orders SET broker_order_id = {}, version = {} WHERE order_id = {};",
                    broker_order_id, version, refresh_pkg.order_id),
                result,
                boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
            );
            if (result.affected_rows() != 1) {
                co_await conn->async_execute("ROLLBACK;", result,
                    boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
                SPDLOG_WARN("TradeAccountRepository async_dispatcher_refresh_data update orders broker_order_id for order_id[{}] affected_rows[{}] unnormal.",
                    refresh_pkg.order_id, result.affected_rows());
                co_return RET_CODE::ERR;
            }
            ++version;
        } else {
            broker_order_id = order[8].as_uint64();
        }

        // 将本地数据与broker的数据进行比对
        refresh_pkg.unfreeze_cent = reserved_cent;
        if (refresh_pkg.broker_terms.account_id != account_id ||
            refresh_pkg.broker_terms.symbol != symbol ||
            refresh_pkg.broker_terms.quantity != quantity ||
            refresh_pkg.broker_terms.limit_price_cent != limit_price_cent ||
            refresh_pkg.broker_terms.broker_order_id != broker_order_id) {
            SPDLOG_ERROR("TradeAccountRepository async_dispatcher_refresh_data broker date incorporate with local. OrderId:{}.",
                refresh_pkg.order_id);
            co_return RET_CODE::ERR;
        }
        if (refresh_pkg.unfreeze_cent < refresh_pkg.spent_cent) {
            SPDLOG_ERROR("TradeAccountRepository async_dispatcher_refresh_data update data unnormal. OrderId:{} Unfreeze:{} Spent:{}",
                refresh_pkg.order_id, refresh_pkg.unfreeze_cent, refresh_pkg.spent_cent);
            co_return RET_CODE::ERR;
        }
        if (refresh_pkg.target_state == trade::OrderState::ORDER_STATE_FILLED) {
            if (refresh_pkg.filled_quntity != quantity ||
                refresh_pkg.fill_price_cent == 0 ||
                refresh_pkg.fill_price_cent > limit_price_cent) {
                SPDLOG_ERROR("TradeAccountRepository async_dispatcher_refresh_data get broker data unnormal. OrderId:{}",
                    refresh_pkg.order_id);
                co_return RET_CODE::ERR;
            }
        } else {
            if (refresh_pkg.fill_price_cent != 0 || refresh_pkg.filled_quntity != 0) {
                SPDLOG_WARN("TradeAccountRepository async_dispatcher_refresh_data non-filled broker snapshot contains filled data. OrderId:{}",
                    refresh_pkg.order_id);
                co_return RET_CODE::ERR;
            }
        }
        auto broker_snapshot_compare = decide_broker_snapshot_update(
            refresh_pkg, broker_version, broker_state, filled_quantity, fill_price_cent, reject_reason);
        if (broker_snapshot_compare == BrokerUpdateDecision::IGNORE) {
            co_return RET_CODE::SKIP;
        }
        if (broker_snapshot_compare == BrokerUpdateDecision::REJECT) {
            SPDLOG_WARN("TradeAccountRepository async_dispatcher_refresh_data broker info unnormal order_id:{} broker_order_id:{}.",
                    refresh_pkg.order_id, refresh_pkg.broker_terms.broker_order_id);
            co_return RET_CODE::ERR;
        }
        auto check_state_converse = decide_broker_state_update(static_cast<trade::OrderState>(state),
            refresh_pkg.target_state);
        if (check_state_converse != BrokerUpdateDecision::APPLY) {
            if (check_state_converse == BrokerUpdateDecision::REJECT) {
                SPDLOG_ERROR("TradeAccountRepository async_dispatcher_refresh_data Local state{} incompatible with broker state{}.",
                    state, cast_type(refresh_pkg.target_state));
                co_return RET_CODE::ERR;
            }
            co_return RET_CODE::SKIP;
        }
        
        // 更新本地订单信息与broker快照相照应
        co_await conn->async_execute(
            boost::mysql::with_params(
                "UPDATE orders SET state = {}, version = {}, broker_version = {}, broker_state = {}, "
                "filled_quantity = {}, fill_price_cent = {}, reject_reason = {} WHERE order_id = {};",
                cast_type(refresh_pkg.target_state), version,
                refresh_pkg.broker_terms.broker_version, cast_type(refresh_pkg.broker_terms.broker_state),
                refresh_pkg.filled_quntity, refresh_pkg.fill_price_cent, refresh_pkg.broker_terms.reject_reason, refresh_pkg.order_id),
            result,
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
        );
        if (result.affected_rows() != 1) {
            co_await conn->async_execute("ROLLBACK;", result,
                boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
            SPDLOG_WARN("TradeAccountRepository async_dispatcher_refresh_data update orders state for order_id[{}] affected_rows[{}] unnormal.",
                refresh_pkg.order_id, result.affected_rows());
            co_return RET_CODE::ERR;
        }

        if (refresh_pkg.ledger_type != LEDGER_ENTRY_TYPE::ORDER_NONE) {
            uint64_t amount = 0;
            if (refresh_pkg.ledger_type == LEDGER_ENTRY_TYPE::ORDER_CANCELLED ||
                refresh_pkg.ledger_type == LEDGER_ENTRY_TYPE::ORDER_REJECTED) {
                amount = refresh_pkg.unfreeze_cent;
            }
            if (refresh_pkg.ledger_type == LEDGER_ENTRY_TYPE::ORDER_FILLED) {
                amount = refresh_pkg.spent_cent;
            }
            co_await conn->async_execute(
                boost::mysql::with_params(
                    "INSERT INTO fund_ledger (account_id, order_id, entry_type, amount_cent) VALUES ({}, {}, {}, {});",
                    account_id, refresh_pkg.order_id, cast_type(refresh_pkg.ledger_type), amount),
                result,
                boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
            );
            if (result.affected_rows() != 1) {
                co_await conn->async_execute("ROLLBACK;", result,
                    boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
                SPDLOG_WARN("TradeAccountRepository async_dispatcher_refresh_data insert fund_ledger record "
                    "for order_id[{}] affected_rows[{}] unnormal.",
                    refresh_pkg.order_id, result.affected_rows());
                co_return RET_CODE::ERR;
            }

            if (refresh_pkg.ledger_type == LEDGER_ENTRY_TYPE::ORDER_FILLED &&
                refresh_pkg.unfreeze_cent > refresh_pkg.spent_cent) {
                // 记录退还金额流水
                auto refund_cent = refresh_pkg.unfreeze_cent - refresh_pkg.spent_cent;
                co_await conn->async_execute(
                    boost::mysql::with_params(
                        "INSERT INTO fund_ledger (account_id, order_id, entry_type, amount_cent) VALUES ({}, {}, {}, {});",
                        account_id, refresh_pkg.order_id, cast_type(LEDGER_ENTRY_TYPE::ORDER_PRICE_REFUND), refund_cent),
                    result,
                    boost::asio::cancel_at(deadline, boost::asio::redirect_error(boost::asio::use_awaitable, ec))
                );
                if (ec == boost::mysql::common_server_errc::er_dup_entry) {
                    // ledger表里已经有 order_id, order_version 相同的撤单记录，重复插入错误
                    co_await conn->async_execute("ROLLBACK;", result,
                        boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
                    SPDLOG_WARN("TradeAccountRepository async_dispatcher_refresh_data insert fund_leger for refund "
                        "account_id[{}] order_id[{}] is repeated.",
                        account_id, refresh_pkg.order_id);
                    co_return RET_CODE::ERR;
                }
                else if (ec) {
                    throw boost::system::system_error(ec);
                }
            }
        }

        if (refresh_pkg.outbox_type != OUTBOX_EVENT_TYPE::OUTBOX_EVENT_NONE) {
            // 插入 outbox_events 记录
            nlohmann::ordered_json payload = {
                {"account_id", account_id},
                {"client_order_id", client_order_id},
                {"symbol", symbol},
                {"limit_price_cent", limit_price_cent},
                {"quantity", quantity},
                {"reserved_cent", reserved_cent},
                {"state", static_cast<int>(refresh_pkg.target_state)}
            };
            if (refresh_pkg.outbox_type == OUTBOX_EVENT_TYPE::OUTBOX_EVENT_FILLED) {
                // 成交需扩充成交信息相关字段
                payload["filled_quantity"] = refresh_pkg.filled_quntity;
                payload["fill_price_cent"] = refresh_pkg.fill_price_cent;
                payload["spent_cent"] = refresh_pkg.spent_cent;
                payload["refund_cent"] = refresh_pkg.unfreeze_cent - refresh_pkg.spent_cent;
            }
            co_await conn->async_execute(
                boost::mysql::with_params(
                    "INSERT INTO outbox_events (order_id, order_version, event_type, payload) "
                    "VALUES({}, {}, {}, {});",
                    refresh_pkg.order_id, version, cast_type(refresh_pkg.outbox_type), payload.dump()),
                result,
                boost::asio::cancel_at(deadline, boost::asio::redirect_error(boost::asio::use_awaitable, ec))
            );
            if (ec && ec != boost::mysql::common_server_errc::er_dup_entry) {
                throw boost::system::system_error(ec);
            } else if (ec == boost::mysql::common_server_errc::er_dup_entry || result.affected_rows() != 1) {
                // 重复键错误
                co_await conn->async_execute("ROLLBACK;", result,
                    boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
                SPDLOG_WARN("TradeAccountRepository async_prepare_order_dispatch insert outbox_events to record dispatching started "
                    "for order_id[{}] affected_rows[{}] unnormal.",
                    refresh_pkg.order_id, result.affected_rows());
                co_return RET_CODE::ERR;
            } 
        }

        if (refresh_pkg.target_state == trade::OrderState::ORDER_STATE_FILLED ||
            refresh_pkg.target_state == trade::OrderState::ORDER_STATE_CANCELLED ||
            refresh_pkg.target_state == trade::OrderState::ORDER_STATE_REJECTED) {
            co_await conn->async_execute(
                boost::mysql::with_params(
                    "UPDATE accounts SET available_cent = available_cent + {}, frozen_cent = frozen_cent - {}, "
                    "spent_cent = spent_cent + {} WHERE account_id = {} AND frozen_cent >= {};",
                    refresh_pkg.unfreeze_cent - refresh_pkg.spent_cent,
                    refresh_pkg.unfreeze_cent, refresh_pkg.spent_cent,
                    account_id, refresh_pkg.unfreeze_cent),
                result,
                boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
            );
            if (result.affected_rows() != 1) {
                co_await conn->async_execute("ROLLBACK;", result,
                    boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
                SPDLOG_WARN("TradeAccountRepository async_dispatcher_refresh_data update accounts state for accountId[{}] affected_rows[{}] unnormal.",
                    account_id, result.affected_rows());
                co_return RET_CODE::ERR;
            }
        }
        
        co_await conn->async_execute("COMMIT;", result,
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable));
        co_return RET_CODE::OK;
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
    co_return RET_CODE::ERR;
}

boost::asio::awaitable<bool> TradeAccountRepository::async_bind_broker_order_id(uint64_t order_id, uint64_t broker_order_id)
{
    co_return co_await this->with_transaction([order_id, broker_order_id](auto& conn, auto& endtime)
            -> boost::asio::awaitable<std::pair<SQL_EXCUTED_CODE, bool>>  {
        boost::mysql::results result;
        co_await conn->async_execute(
            boost::mysql::with_params(
                "SELECT broker_order_id FROM orders WHERE order_id = {} FOR UPDATE;",
                order_id),
            result,
            boost::asio::cancel_at(endtime, boost::asio::use_awaitable)
        );
        if (result.rows().empty()) {
            co_return std::make_pair(SQL_EXCUTED_CODE::ERR, false);
        }
        if (!result.rows()[0][0].is_null()) {
            if (result.rows()[0][0].as_uint64() == broker_order_id) {
                co_return std::make_pair(SQL_EXCUTED_CODE::OK, true);
            } else {
                co_return std::make_pair(SQL_EXCUTED_CODE::ERR, false);
            }
        }

        co_await conn->async_execute(
            boost::mysql::with_params(
                "UPDATE orders SET version = version + 1, broker_order_id = {} WHERE order_id = {};", broker_order_id, order_id),
            result,
            boost::asio::cancel_at(endtime, boost::asio::use_awaitable)
        );
        if (result.affected_rows() != 1) {
            co_return std::make_pair(SQL_EXCUTED_CODE::ERR, false);
        }
        co_return std::make_pair(SQL_EXCUTED_CODE::OK, true);
    });
}

boost::asio::awaitable<bool> TradeAccountRepository::async_update_retry_info(uint64_t order_id, uint64_t retry_count, RET_CODE status)
{
    co_return co_await this->with_transaction([order_id, retry_count, status](auto& conn, auto& endtime) mutable
        -> boost::asio::awaitable<std::pair<SQL_EXCUTED_CODE, bool>> {
        boost::mysql::results result;

        // 根据"连续没有取得刷新进展"次数指数退避下次重试时间，大于等于六次则按30秒为约定
        static constexpr std::array<uint8_t, 6> RETRY_SECONDS = {
            1, 2, 4, 8, 16, 30
        };

        if (status == RET_CODE::OK) {
            // 清空重试次数
            co_await conn->async_execute(
                boost::mysql::with_params(
                    "UPDATE orders SET retry_count = {}, next_retry_at = NULL WHERE order_id = {};",
                    0, order_id),
                result,
                boost::asio::cancel_at(endtime, boost::asio::use_awaitable)
            );
        } else {
            // 重试时间指数退避
            ++retry_count;
            auto seconds = retry_count > RETRY_SECONDS.size() ? RETRY_SECONDS.back() : RETRY_SECONDS[retry_count - 1];
            co_await conn->async_execute(
                boost::mysql::with_params(
                    "UPDATE orders SET retry_count = {}, next_retry_at = TIMESTAMPADD(SECOND, {}, CURRENT_TIMESTAMP(3)) WHERE order_id = {};",
                    retry_count, seconds, order_id),
                result,
                boost::asio::cancel_at(endtime, boost::asio::use_awaitable)
            );
        }
        if (result.affected_rows() != 1) {
            co_return std::make_pair(SQL_EXCUTED_CODE::ERR, false);
        }
        co_return std::make_pair(SQL_EXCUTED_CODE::OK, true);
    });
}