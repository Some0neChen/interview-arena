#include <chrono>
#include <memory>
#include <spdlog/async_logger.h>
#include <spdlog/common.h>
#include <spdlog/spdlog.h>
#include <spdlog/async.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <vector>

inline void InitLogging(const char* log_file_path,
    spdlog::level::level_enum terminal_log_level,
    spdlog::level::level_enum file_log_level,
    spdlog::level::level_enum least_level)
{
    // 初始化异步线程池，队列大小为8192，线程数为1
    spdlog::init_thread_pool(8192, 1); 

    // 插件A：彩色控制台sink
    auto console_sink = std::make_shared<spdlog::sinks::stdout_color_sink_mt>();
    // 设置终端打印级别
    console_sink->set_level(terminal_log_level);

    // 插件B：滚动文件Sink (设置路径，单个文件最大大小，历史最多文件大小)
    auto file_sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
        log_file_path, 1024 * 1024 * 64, 5
    );
    // 设置文件打印级别
    file_sink->set_level(file_log_level);

    // 将插件加载到异步logger中，设置logger名称为"async_logger"，并指定线程池
    // 最后的block代表当队列满时阻塞等待，overrun_oldest代表丢弃最旧的日志
    std::vector<spdlog::sink_ptr> sinks{console_sink, file_sink};
    auto logger = std::make_shared<spdlog::async_logger>("async_logger", sinks.begin(), sinks.end(),
        spdlog::thread_pool(),
        spdlog::async_overflow_policy::block);

    // 5. 设置日志格式 Pattern: [时间] [日志级别] [线程ID] [源码:行号] 消息内容                                                                                                                                                       
    // %^...%$ 表示在此区间内的文字会根据日志级别自动显示不同颜色！
    logger->set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%1%$] [tid: %t] [%s:%#] %v");

    // 6. 注册为全局默认Logger
    spdlog::set_default_logger(logger);

    spdlog::set_level(least_level);
    // 7. 设置日志刷新策略，flush_on表示当日志级别达到err时立即刷新，flush_every表示每3秒刷新一次
    spdlog::flush_on(spdlog::level::err);
    // 8. 设置每3秒刷新一次日志，确保日志不会长时间滞留在缓冲区中
    spdlog::flush_every(std::chrono::seconds(3));
}