
#pragma once
#include <algorithm>
#include <boost/asio/error.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/system/detail/error_code.hpp>
#include <chrono>
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <optional>
#include <thread>
#include <type_traits>
#include <utility>
#include <vector>
#include <boost/asio/any_io_executor.hpp>
#include <boost/asio/executor_work_guard.hpp>
#include <boost/asio/io_context.hpp>
#include <boost/asio.hpp>
#include <spdlog/spdlog.h>

class AsioRuntime {
    size_t thread_nums_ = 0;
    boost::asio::io_context ioc_;
    std::optional<boost::asio::executor_work_guard<boost::asio::io_context::executor_type>> guard_;
    std::vector<std::thread> threads_;

    void stop()
    {
        if (!guard_.has_value()) {
            return;
        }
        guard_.reset();
        ioc_.stop();
        for (auto& t : threads_) {
            if (t.joinable()) {
                t.join();
            }
        }
        threads_.clear();
        ioc_.restart();
    }
public:
    struct DelayTask : public std::enable_shared_from_this<DelayTask> {
        DelayTask(AsioRuntime* runtime) : runtime_(runtime), timer_(runtime->ioc_) {}

        template<typename Func, typename... Args>
        auto run_timer(std::chrono::milliseconds time, Func func, Args... args) -> std::optional<std::future<std::invoke_result_t<Func, Args...>>>{
            using return_type = std::invoke_result_t<Func, Args...>;
            auto delay_task = std::make_shared<std::packaged_task<return_type()>>(std::bind(std::forward<Func>(func), std::forward<Args>(args)...));
            auto res = delay_task->get_future();
            timer_.expires_after(time);
            timer_.async_wait([task = std::move(delay_task)](const boost::system::error_code& ec) {
                if (ec == boost::asio::error::operation_aborted) {
                    // 代表任务已被提前取消，打印日志。后续依赖task里兜底判断是否已取消
                    SPDLOG_INFO("Task canceled.");
                }
                (*task)();
                return;
            });
            return res;
        }

        void cancel() {
            auto self = this->shared_from_this();
            runtime_->add_task([self]() {
                self->timer_.cancel();
            });
        }
    private:
        AsioRuntime* runtime_;
        boost::asio::steady_timer timer_;
    };
public:
    explicit AsioRuntime(size_t num) : thread_nums_(num) {};

    ~AsioRuntime()
    {
        stop();
    }

    void start()
    {
        if (!guard_.has_value()) {
            guard_.emplace(boost::asio::make_work_guard(ioc_));
        }
        threads_.reserve(thread_nums_);
        for (int i = 0; i < thread_nums_; ++i) {
            threads_.emplace_back(std::thread([this]() {
                this->ioc_.run();
            }));
        }
    }

    template<typename Func, typename... Args>
    auto add_task(Func&& func, Args... args) -> std::optional<std::future<std::invoke_result_t<Func, Args...>>>
    {
        using return_type = std::invoke_result_t<Func, Args...>;
        auto task_ptr = std::make_shared<std::packaged_task<return_type()>>(
            std::bind(std::forward<Func>(func), std::forward<Args>(args)...)
        );
        auto res = task_ptr->get_future();
        boost::asio::post(ioc_, [task = std::move(task_ptr)]() {
            (*task)();
        });
        return res;
    }

    auto get_delay_task()
    {
        auto delay_task = std::make_shared<DelayTask>(this);
        return delay_task;
    }
};