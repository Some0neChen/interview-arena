#include <algorithm>
#include <boost/asio/awaitable.hpp>
#include <boost/asio/co_spawn.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/exception/exception.hpp>
#include <boost/mysql/error_with_diagnostics.hpp>
#include <boost/mysql/handshake_params.hpp>
#include <boost/mysql/results.hpp>
#include <boost/mysql/tcp.hpp>
#include <cstdint>
#include <exception>
#include <format>
#include <iostream>
#include <boost/asio/use_awaitable.hpp>
#include <optional>
#include <string>

struct Question {
    uint64_t question_id;
    std::string title;
    uint64_t difficulty;
    uint64_t torture_score;
    std::string source;
};

boost::asio::awaitable<std::optional<Question>> async_connect_and_excute_sql(boost::mysql::tcp_connection& conn,
    const boost::asio::ip::tcp::endpoint& end,
    const boost::mysql::handshake_params& params,
    const uint64_t request_id = 0)
{
    try {
        co_await conn.async_connect(end, params, boost::asio::use_awaitable);

        const auto statement = co_await conn.async_prepare_statement(
            "select question_id, title, difficulty, torture_score, source from questions where question_id = ?;",
            boost::asio::use_awaitable
        );
        auto sql = statement.bind(static_cast<uint64_t>(request_id));
        boost::mysql::results result;
        co_await conn.async_execute(sql, result, boost::asio::use_awaitable);
    
        auto rows = result.rows();
        if (rows.empty()) {
            std::cout << "sql excute result is empty." << std::endl;
            co_await conn.async_close(boost::asio::use_awaitable);
            co_return std::nullopt;
        } else {
            Question question;
            question.question_id = rows[0][0].as_uint64();
            question.title = std::string(rows[0][1].as_string());
            question.difficulty = rows[0][2].as_uint64();
            question.torture_score = rows[0][3].as_uint64();
            question.source = std::string(rows[0][4].as_string());
            co_await conn.async_close(boost::asio::use_awaitable);
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
    const boost::asio::ip::tcp::endpoint endpoint(
        boost::asio::ip::make_address("127.0.0.1"),
        3306
    );
    boost::mysql::tcp_connection conn(ioc);
    const boost::mysql::handshake_params params("a", "111111", "arena");
    
    std::cout << "start coroutine to connect mysql and excute sql." << std::endl;

    uint64_t request_id = 0;
    if (argc >= 2) {
        request_id = std::stoll(argv[1]);
    }

    int ret_code = 0;
    std::optional<Question> res;
    boost::asio::co_spawn(ioc, async_connect_and_excute_sql(conn, endpoint, params, request_id),
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
        "torture_scoure :   {}\n"
        "source :           {}\n",
        res->question_id,
        res->title,
        res->difficulty,
        res->torture_score,
        res->source);

    return ret_code;
}