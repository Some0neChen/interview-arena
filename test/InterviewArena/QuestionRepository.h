#pragma once
#include "interview_arena.pb.h"
#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancel_after.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detail/chrono.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/use_awaitable.hpp>
#include <boost/exception/exception.hpp>
#include <boost/mysql/connection_pool.hpp>
#include <boost/mysql/error_code.hpp>
#include <boost/mysql/handshake_params.hpp>
#include <boost/mysql/pool_params.hpp>
#include <boost/mysql/results.hpp>
#include <boost/mysql/tcp.hpp>
#include <cstdint>
#include <grpcpp/server_context.h>
#include <optional>
#include <spdlog/spdlog.h>
#include <utility>

struct Question {
    uint64_t question_id;
    std::string title;
    uint64_t difficulty;
    uint64_t torture_score;
    std::string source;
};

class QuestionRepository {
    // strand仅串行执行片段；共享连接仍缺少整次查询的独占。
    boost::asio::strand<boost::asio::io_context::executor_type> strand_;
    boost::mysql::connection_pool pool_;

public:
    QuestionRepository(boost::mysql::pool_params params,
        boost::asio::io_context& ioc) :
            strand_(boost::asio::make_strand(ioc)), pool_(boost::mysql::connection_pool(strand_, std::move(params)))
    {
        pool_.async_run([](boost::mysql::error_code code) {
            if (code) {
                SPDLOG_ERROR("MySQL connection pool stopped with error: {}.", code.message());
            } else {
                SPDLOG_INFO("MySQL connection pool stopped.");
            }
        });
    }

    void stop() {
        pool_.cancel();
    }

    ~QuestionRepository()
    {
        stop();
    }

    boost::asio::awaitable<std::optional<Question>> async_get_question_by_id(const uint64_t qid)
    {
        co_return co_await boost::asio::co_spawn(strand_, [this, qid]() -> boost::asio::awaitable<std::optional<Question>> {
            boost::mysql::results result;
            try {
                {
                    auto conn = co_await pool_.async_get_connection(
                        boost::asio::cancel_after(boost::asio::chrono::seconds(3)));
                    auto statement = co_await conn->async_prepare_statement(
                        "select question_id, title, difficulty, torture_score, source from questions where question_id = ?;",
                        boost::asio::use_awaitable);
                    auto sql = statement.bind(qid);
                    co_await conn->async_execute(sql, result);
                }

                auto rows = result.rows();

                if (rows.empty()) {
                    SPDLOG_DEBUG("sql excute result is empty. questionid: {}.", qid);
                    // 这里不处理连接关闭，当前为持久化连接
                    co_return std::nullopt;
                } else {
                    Question question;
                    // 当前暂且都统一取第一条数据
                    question.question_id = rows[0][0].as_uint64();
                    question.title = rows[0][1].as_string();
                    question.difficulty = rows[0][2].as_uint64();
                    question.torture_score = rows[0][3].as_uint64();
                    question.source = rows[0][4].as_string();
                    co_return question;
                }
            } catch (const boost::mysql::error_with_diagnostics& ex) {
                SPDLOG_ERROR("MySQL error: {}.", ex.what());
                SPDLOG_ERROR("Server message: {}", ex.get_diagnostics().server_message());
                throw;
            } catch (const std::exception& e) {
                SPDLOG_ERROR("mysql smoke failed: {}", e.what());
                throw;
            }
        }, boost::asio::use_awaitable);
    }
};
