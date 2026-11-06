SET(TRADE_SERVER_SOURCE
    ${CMAKE_CURRENT_SOURCE_DIR}/repository/TradeAccountRepository.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/broker/BrokerClient.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/dispatcher/OrderDispatcher.cpp
)

add_executable(trade_server
    trade_server_main.cpp
    ${TRADE_SERVER_SOURCE}
)

target_include_directories(trade_server PRIVATE
    ${PUB_INCLUDE_DIR}
    repository
    reactor
    broker
    dispatcher
)

target_link_libraries(trade_server PRIVATE
    trade_account_proto
    trade_broker_proto
    spdlog::spdlog
    Threads::Threads
    OpenSSL::SSL
    OpenSSL::Crypto
    Boost::charconv
    nlohmann_json::nlohmann_json
)