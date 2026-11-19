SET(TRADE_BROKER_SERVER_SOURCE
    ${CMAKE_CURRENT_SOURCE_DIR}/repository/TradeBrokerRepository.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/worker/BrokerExecutionWoker.cpp
)

add_executable(trade_broker
    main.cpp
    ${TRADE_BROKER_SERVER_SOURCE}
)
target_include_directories(trade_broker PRIVATE
    ${PUB_INCLUDE_DIR}
    repository
    reactor
    worker
)

target_link_libraries(trade_broker PRIVATE
    trade_broker_proto
    spdlog::spdlog
    Threads::Threads
    OpenSSL::SSL
    OpenSSL::Crypto
    Boost::charconv
    nlohmann_json::nlohmann_json
)