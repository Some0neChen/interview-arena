cmake_minimum_required(VERSION 3.16)
project(InterviewArena CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

find_package(Threads REQUIRED)
# 获取本机gRpc及protobuf
find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)
# 获取本机spdlog
find_package(spdlog REQUIRED)
# 获取本机Boost, 1.88.0 mysql扩展了连接池
find_package(Boost 1.88.0 CONFIG REQUIRED COMPONENTS charconv)
# 获取SSL连接库
find_package(OpenSSL REQUIRED)
# JSON库
find_package(nlohmann_json CONFIG REQUIRED)

# 生成proto文件
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

set(GEN_DIR
    "${CMAKE_CURRENT_BINARY_DIR}/generated"
)

file(MAKE_DIRECTORY "${GEN_DIR}")

file(MAKE_DIRECTORY "${GEN_DIR}/interview_arena")
add_custom_command(
    OUTPUT 
        "${GEN_DIR}/interview_arena/interview_arena.pb.cc"
        "${GEN_DIR}/interview_arena/interview_arena.pb.h"
        "${GEN_DIR}/interview_arena/interview_arena.grpc.pb.cc"
        "${GEN_DIR}/interview_arena/interview_arena.grpc.pb.h"
    COMMAND
        ${PROTOBUF_PROTOC_EXECUTABLE}
            --proto_path="${CMAKE_CURRENT_SOURCE_DIR}/test/InterviewArena"
            --cpp_out="${GEN_DIR}/interview_arena"
            --grpc_out="${GEN_DIR}/interview_arena"
            --plugin=protoc-gen-grpc="${GRPC_CPP_PLUGIN}"
            "${CMAKE_CURRENT_SOURCE_DIR}/test/InterviewArena/interview_arena.proto"
    DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/test/InterviewArena/interview_arena.proto"
    COMMENT
        "Generating gRPC and Protobuf source files for interview_arena.proto"
)

add_library(arena_proto STATIC
    "${GEN_DIR}/interview_arena/interview_arena.pb.cc"
    "${GEN_DIR}/interview_arena/interview_arena.grpc.pb.cc"
)

target_include_directories(arena_proto PUBLIC "${GEN_DIR}/interview_arena")
target_link_libraries(arena_proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)

add_custom_command(
    OUTPUT 
        "${GEN_DIR}/trade_account.pb.cc"
        "${GEN_DIR}/trade_account.pb.h"
        "${GEN_DIR}/trade_account.grpc.pb.cc"
        "${GEN_DIR}/trade_account.grpc.pb.h"
    COMMAND
        ${PROTOBUF_PROTOC_EXECUTABLE}
            --proto_path="${CMAKE_CURRENT_SOURCE_DIR}/proto"
            --cpp_out="${GEN_DIR}"
            --grpc_out="${GEN_DIR}"
            --plugin=protoc-gen-grpc="${GRPC_CPP_PLUGIN}"
            "${CMAKE_CURRENT_SOURCE_DIR}/proto/trade_account.proto"
    DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/proto/trade_account.proto"
    COMMENT
        "Generating gRPC and Protobuf source files for trade_account.proto"
)

add_library(trade_account_proto STATIC
    "${GEN_DIR}/trade_account.grpc.pb.cc"
    "${GEN_DIR}/trade_account.pb.cc"
)

target_include_directories(trade_account_proto PUBLIC "${GEN_DIR}")
target_link_libraries(trade_account_proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)

add_custom_command(
    OUTPUT 
        "${GEN_DIR}/trade_broker.pb.cc"
        "${GEN_DIR}/trade_broker.pb.h"
        "${GEN_DIR}/trade_broker.grpc.pb.cc"
        "${GEN_DIR}/trade_broker.grpc.pb.h"
    COMMAND
        ${PROTOBUF_PROTOC_EXECUTABLE}
            --proto_path="${CMAKE_CURRENT_SOURCE_DIR}/proto"
            --cpp_out="${GEN_DIR}"
            --grpc_out="${GEN_DIR}"
            --plugin=protoc-gen-grpc="${GRPC_CPP_PLUGIN}"
            "${CMAKE_CURRENT_SOURCE_DIR}/proto/trade_broker.proto"
    DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/proto/trade_broker.proto"
    COMMENT
        "Generating gRPC and Protobuf source files for trade_broker.proto"
)

add_library(trade_broker_proto STATIC
    "${GEN_DIR}/trade_broker.grpc.pb.cc"
    "${GEN_DIR}/trade_broker.pb.cc"
)

target_include_directories(trade_broker_proto PUBLIC "${GEN_DIR}")
target_link_libraries(trade_broker_proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)

set(PUB_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/pub")
# 将可执行程序都放在 ${CMAKE_CURRENT_SOURCE_DIR} 下面
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}")

add_subdirectory(client)
add_subdirectory(server)
add_subdirectory(test)
add_subdirectory(broker)