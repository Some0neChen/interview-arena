add_executable(trade_client trade_client_main.cpp)
target_include_directories(trade_client PRIVATE
    ${PUB_INCLUDE_DIR}
)
target_link_libraries(trade_client PRIVATE
    trade_account_proto
    spdlog::spdlog
)