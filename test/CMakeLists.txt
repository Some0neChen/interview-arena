add_executable(mysql_smoke mysql_smoke.cpp)
target_link_libraries(mysql_smoke PRIVATE
    Boost::charconv
    Threads::Threads
    OpenSSL::SSL
    OpenSSL::Crypto
)

add_executable(mysql_async_smoke mysql_async_smoke.cpp)
target_link_libraries(mysql_async_smoke PRIVATE
    Boost::charconv
    Threads::Threads
    OpenSSL::SSL
    OpenSSL::Crypto
)

add_executable(mysql_async_pool_smoke mysql_async_pool_smoke.cpp)
target_link_libraries(mysql_async_pool_smoke PRIVATE
    Boost::charconv
    Threads::Threads
    OpenSSL::SSL
    OpenSSL::Crypto
)

add_executable(arena_server InterviewArena/main.cpp)
target_include_directories(arena_server PRIVATE
    ${PUB_INCLUDE_DIR}
    InterviewArena
)
target_link_libraries(arena_server PRIVATE
    arena_proto
    spdlog::spdlog
    Threads::Threads
    OpenSSL::SSL
    OpenSSL::Crypto
    Boost::charconv
)

add_executable(arena_client InterviewArena/client_main.cpp)
target_include_directories(arena_client PRIVATE ${PUB_INCLUDE_DIR})
target_link_libraries(arena_client PRIVATE
    spdlog::spdlog
    arena_proto
)

add_executable(code_test interview_test.cpp)

option(ARENA_BUILD_ACCEPTANCE_TESTS "Enable database-writing acceptance test targets" OFF)
if(ARENA_BUILD_ACCEPTANCE_TESTS)
    add_executable(trade_fault_server EXCLUDE_FROM_ALL
        trade_fault_server.cpp
        ${PROJECT_SOURCE_DIR}/server/repository/TradeAccountRepository.cpp
    )
    target_include_directories(trade_fault_server PRIVATE
        ${PUB_INCLUDE_DIR}
        ${PROJECT_SOURCE_DIR}/server
        ${PROJECT_SOURCE_DIR}/server/repository
        ${PROJECT_SOURCE_DIR}/server/reactor
    )
    target_link_libraries(trade_fault_server PRIVATE
        trade_account_proto
        spdlog::spdlog
        Boost::charconv
        Threads::Threads
        OpenSSL::SSL
        OpenSSL::Crypto
        nlohmann_json::nlohmann_json
    )
    add_executable(submit_order_acceptance EXCLUDE_FROM_ALL submit_order_acceptance.cpp)
    target_link_libraries(submit_order_acceptance PRIVATE
        trade_account_proto
        Boost::charconv
        Threads::Threads
        OpenSSL::SSL
        OpenSSL::Crypto
        nlohmann_json::nlohmann_json
    )
endif()
