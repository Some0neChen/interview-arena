cmake_minimum_required(VERSION 3.16)
project(InterviewArena CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# 获取本机gRpc及protobuf
find_package(Protobuf REQUIRED)
find_package(gRPC CONFIG REQUIRED)
# 获取本机spdlog
find_package(spdlog REQUIRED)
# 获取本机Boost
find_package(Boost REQUIRED)
# 获取SSL连接库
find_package(OpenSSL REQUIRED)

# 生成proto文件
find_program(GRPC_CPP_PLUGIN grpc_cpp_plugin REQUIRED)

set(GEN_DIR
    "${CMAKE_CURRENT_BINARY_DIR}/generated"
)

file(MAKE_DIRECTORY "${GEN_DIR}")

add_custom_command(
    OUTPUT 
        "${GEN_DIR}/interview_arena.pb.cc"
        "${GEN_DIR}/interview_arena.pb.h"
        "${GEN_DIR}/interview_arena.grpc.pb.cc"
        "${GEN_DIR}/interview_arena.grpc.pb.h"
    COMMAND
        ${PROTOBUF_PROTOC_EXECUTABLE}
            --proto_path="${CMAKE_CURRENT_SOURCE_DIR}/proto"
            --cpp_out="${GEN_DIR}"
            --grpc_out="${GEN_DIR}"
            --plugin=protoc-gen-grpc="${GRPC_CPP_PLUGIN}"
            "${CMAKE_CURRENT_SOURCE_DIR}/proto/interview_arena.proto"
    DEPENDS
        "${CMAKE_CURRENT_SOURCE_DIR}/proto/interview_arena.proto"
    COMMENT
        "Generating gRPC and Protobuf source files for interview_arena.proto"
)

add_library(arena_proto STATIC
    "${GEN_DIR}/interview_arena.pb.cc"
    "${GEN_DIR}/interview_arena.grpc.pb.cc"
)

# 设置头文件搜索路径和链接库
set(PUB_INCLUDE_DIR "${CMAKE_CURRENT_SOURCE_DIR}/pub")

target_include_directories(arena_proto PUBLIC "${GEN_DIR}")
target_link_libraries(arena_proto PUBLIC gRPC::grpc++ protobuf::libprotobuf)

add_executable(arena_server server/main.cpp)
target_include_directories(arena_server PRIVATE ${PUB_INCLUDE_DIR})
target_link_libraries(arena_server PRIVATE arena_proto)
target_link_libraries(arena_server PRIVATE spdlog::spdlog)
target_link_libraries(arena_server PRIVATE Boost::boost)

add_executable(arena_client client/main.cpp)
target_include_directories(arena_client PRIVATE ${PUB_INCLUDE_DIR})
target_link_libraries(arena_client PRIVATE arena_proto)
target_link_libraries(arena_client PRIVATE spdlog::spdlog)

add_executable(mysql_smoke test/mysql_smoke.cpp)
target_link_libraries(mysql_smoke PRIVATE
    Boost::boost
    Threads::Threads
    OpenSSL::SSL
    OpenSSL::Crypto
)
