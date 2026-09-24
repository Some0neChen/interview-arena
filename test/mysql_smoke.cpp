#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/address.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/exception/exception.hpp>
#include <boost/mysql/error_with_diagnostics.hpp>
#include <boost/mysql/handshake_params.hpp>
#include <boost/mysql/results.hpp>
#include <boost/mysql/tcp.hpp>
#include <exception>
#include <format>
#include <iostream>

int main ()
{
    try {
        boost::asio::io_context ioc;
        const boost::asio::ip::tcp::endpoint endpoint(
            boost::asio::ip::make_address("127.0.0.1"),
            3306
        );
        boost::mysql::tcp_connection conn(ioc);
        const boost::mysql::handshake_params params("a", "111111", "arena");
        conn.connect(endpoint, params);

        const auto statement = conn.prepare_statement(
            "select question_id, title, difficulty, torture_score, source from questions where question_id = ?;"
        );
        auto sql = statement.bind(static_cast<uint64_t>(1));
        boost::mysql::results result;
        conn.execute(sql, result);
        auto rows = result.rows();
        if (rows.empty()) {
            std::cout << "sql excute result is empty." << std::endl;
            return 0;
        } else {
            std::cout << std::format(
                "question_id :      {}\n"
                "title :            {}\n"
                "difficulty :       {}\n"
                "torture_scoure :   {}\n"
                "source :           {}\n",
                rows[0][0].as_uint64(),
                std::string(rows[0][1].as_string()),
                rows[0][2].as_uint64(),
                rows[0][3].as_uint64(),
                std::string(rows[0][4].as_string()));
        }
    } catch (const boost::mysql::error_with_diagnostics& ex) {
        std::cerr << ex.what() << std::endl;
        std::cerr << ex.get_diagnostics().server_message() << std::endl;
    } catch (const std::exception& e) {
        std::cerr << "mysql smoke failed: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}