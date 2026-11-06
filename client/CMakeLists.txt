add_executable(trade_client trade_client_main.cpp)
target_include_directories(trade_client PRIVATE
    ${PUB_INCLUDE_DIR}
)
target_link_libraries(trade_client PRIVATE
    trade_account_proto
    spdlog::spdlog
)

add_executable(get_trade_broker_order_state get_trade_broker_order_state.cpp)
target_include_directories(get_trade_broker_order_state PRIVATE
    ${PUB_INCLUDE_DIR}
)
target_link_libraries(get_trade_broker_order_state PRIVATE
    trade_broker_proto
    spdlog::spdlog
)