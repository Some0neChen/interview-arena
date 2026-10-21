#include <algorithm>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/cancel_after.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/detail/chrono.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/exception/exception.hpp>
#include <boost/mysql/error_with_diagnostics.hpp>
#include <boost/mysql/handshake_params.hpp>
#include <boost/mysql/results.hpp>
#include <boost/mysql/tcp.hpp>
#include <boost/mysql/connection_pool.hpp>
#include <boost/system/detail/error_code.hpp>
#include <cstdint>
#include <exception>
#include <format>
#include <functional>
#include <iostream>
#include <boost/asio/use_awaitable.hpp>
#include <memory>
#include <optional>
#include <string>

struct Question {
    uint64_t question_id;
    std::string title;
    uint64_t difficulty;
    uint64_t torture_score;
    std::string source;
};

boost::asio::awaitable<std::optional<Question>> async_connect_and_excute_sql(boost::mysql::connection_pool& pool,
    const uint64_t request_id = 0)
{
    try
    {
        auto pool_ = std::unique_ptr<boost::mysql::connection_pool, std::function<void(boost::mysql::connection_pool*)>>(&pool, [](boost::mysql::connection_pool* p) {
            p->cancel();
        });
        boost::mysql::results result;
        {
            // 通过协程获取连接，期间阻塞协程。并限制最大等待3秒。
            // 套在括号里是为了让conn在使用完后自动析构，释放连接回连接池，不在不涉及的地方延长占用时间
            auto conn = co_await pool.async_get_connection(
                boost::asio::cancel_after(boost::asio::chrono::seconds(3), boost::asio::use_awaitable)
            );

            const auto statement = co_await conn->async_prepare_statement(
                "select question_id, title, difficulty, torture_score, source from questions where question_id = ?;",
                boost::asio::use_awaitable
            );
            auto sql = statement.bind(static_cast<uint64_t>(request_id));
            co_await conn->async_execute(sql, result, boost::asio::use_awaitable);
        }

    
        auto rows = result.rows();
        if (rows.empty()) {
            std::cout << "sql excute result is empty." << std::endl;
            co_return std::nullopt;
        } else {
            Question question;
            question.question_id = rows[0][0].as_uint64();
            question.title = std::string(rows[0][1].as_string());
            question.difficulty = rows[0][2].as_uint64();
            question.torture_score = rows[0][3].as_uint64();
            question.source = std::string(rows[0][4].as_string());
            co_return question;
        }
    } catch (const boost::mysql::error_with_diagnostics& ex) {
        std::cerr << ex.what() << std::endl;
        std::cerr << ex.get_diagnostics().server_message() << std::endl;
        throw;
    } catch (const std::exception& e) {
        std::cerr << "mysql smoke failed: " << e.what() << std::endl;
        throw;
    }
}

int main (int argc, char* argv[])
{
    boost::asio::io_context ioc;

    // 创建连接池
    boost::mysql::pool_params pool_params;
    pool_params.server_address.emplace_host_and_port("127.0.0.1", 3306);
    pool_params.username = "a";
    pool_params.password = "111111";
    pool_params.database = "arena";
    pool_params.initial_size = 1; // 初始连接数
    pool_params.max_size = 1; // 最大连接数
    boost::mysql::connection_pool pool(ioc, std::move(pool_params));
    pool.async_run([](boost::system::error_code code) {
        // 回调内函数在连接池停止后调用
        std::cout << "Pool stopped." << std::endl;
    });

    std::cout << "start coroutine to connect mysql and excute sql." << std::endl;

    uint64_t request_id = 0;
    if (argc >= 2) {
        request_id = std::stoll(argv[1]);
    }

    int ret_code = 0;
    std::optional<Question> res;
    boost::asio::co_spawn(ioc, async_connect_and_excute_sql(pool, request_id),
        [&ret_code, &res](std::exception_ptr e, std::optional<Question> question) {
        if (e) {
            std::cerr << "mysql smoke async excute failed: " << std::endl;
            ret_code = 1;
            return;
        }
        res = std::move(question);
        std::cout << "coroutine to connect mysql and excute sql succeeded." << std::endl;
    });

    ioc.run();

    if (ret_code != 0 || !res.has_value()) {
        return ret_code;
    }

    std::cout << std::format(
        "question_id :      {}\n"
        "title :            {}\n"
        "difficulty :       {}\n"
        "torture_score :   {}\n"
        "source :           {}\n",
        res->question_id,
        res->title,
        res->difficulty,
        res->torture_score,
        res->source
    );

    return ret_code;
}