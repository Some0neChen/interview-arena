#include "TradeAccountRepository.h"
#include "trade_account.pb.h"
#include <boost/asio/cancel_after.hpp>
#include <boost/asio/cancel_at.hpp>
#include <boost/asio/detail/chrono.hpp>
#include <boost/asio/redirect_error.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/mysql/connection_pool.hpp>
#include <boost/mysql/error_code.hpp>
#include <boost/mysql/impl/internal/sansio/run_pipeline.hpp>
#include <boost/system/system_error.hpp>
#include <chrono>
#include <cstdint>
#include <exception>
#include <grpcpp/support/status.h>
#include <nlohmann/json.hpp>
#include <optional>
#include <spdlog/spdlog.h>
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
                account_id, res.order_id, LEDGER_ENTRY_TYPE::ORDER_FREEZE, cost),
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
                res.order_id, 1, OUTBOX_EVENT_TYPE::OUTBOX_EVENT_RECEIVED, payload.dump()),
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

        if (state == trade::OrderState::ORDER_STATE_CANCELLED) {
            res.order_state = state; 
            co_return std::make_pair(grpc::StatusCode::OK, res);
        }
        if (state != trade::OrderState::ORDER_STATE_LOCAL_ACCEPTED) {
            SPDLOG_WARN("TradeAccountRepository async_cancel_order order for account_id[{}] client_order_id[{}] state was ORDER_STATE_LOCAL_ACCEPTED.",
                account_id, client_order_id);
            co_return std::make_pair(grpc::StatusCode::FAILED_PRECONDITION, std::nullopt);
        }

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
                account_id, order_id, LEDGER_ENTRY_TYPE::ORDER_CANCELLED, reserved_cent),
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
                order_id, version, OUTBOX_EVENT_TYPE::OUTBOX_EVENT_CANCELLED, payload.dump()),
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

        res.order_state = trade::ORDER_STATE_CANCELLED;
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

boost::asio::awaitable<std::vector<std::pair<uint64_t, trade::OrderState>>> TradeAccountRepository::async_list_handle_order(uint16_t limit)
{
    std::vector<std::pair<uint64_t, trade::OrderState>> res;
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
                "SELECT order_id, state FROM orders "
                "WHERE state IN ({}, {}, {}) ORDER BY order_id ASC LIMIT {};",
                static_cast<uint16_t>(trade::OrderState::ORDER_STATE_LOCAL_ACCEPTED),
                static_cast<uint16_t>(trade::OrderState::ORDER_STATE_DISPATCHING),
                static_cast<uint16_t>(trade::OrderState::ORDER_STATE_BROKER_ACCEPTED), limit),
            result,
            boost::asio::cancel_at(deadline, boost::asio::use_awaitable)
        );

        if (result.rows().empty()) {
            co_return res;
        }

        auto row_size = result.rows().size();
        res.reserve(row_size);

        for (const auto& row : result.rows()) {
            res.emplace_back(std::make_pair(row[0].as_uint64(), static_cast<trade::OrderState>(row[1].as_uint64())));
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