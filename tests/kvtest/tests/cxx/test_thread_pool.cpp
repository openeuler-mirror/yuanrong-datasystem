#include "test_harness.h"
#include "common/thread_pool.h"
#include <atomic>
#include <chrono>

TEST(SubmitAndExecute) {
    std::atomic<int> counter{0};
    {
        ThreadPool pool(2);
        pool.Submit([&]() { counter++; });
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }
    ASSERT_EQ(counter.load(), 1);
}

TEST(MultipleSubmits) {
    std::atomic<int> counter{0};
    {
        ThreadPool pool(4);
        for (int i = 0; i < 100; i++) {
            pool.Submit([&]() { counter++; });
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(200));
    }
    ASSERT_EQ(counter.load(), 100);
}

TEST(StopDrainsTasks) {
    std::atomic<bool> executed{false};
    {
        ThreadPool pool(1);
        pool.Submit([&]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            executed = true;
        });
        pool.Stop();
    }
    ASSERT_TRUE(executed.load());
}

TEST(SubmitAfterStop) {
    std::atomic<int> counter{0};
    auto pool = std::make_unique<ThreadPool>(1);
    pool->Stop();
    pool->Submit([&]() { counter++; });
    ASSERT_EQ(counter.load(), 0);
}

TEST(QueueSize) {
    ThreadPool pool(1);
    std::atomic<bool> block{true};
    // Submit a blocking task to occupy the worker
    pool.Submit([&]() {
        while (block.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
    // Submit more tasks - they should queue up
    for (int i = 0; i < 5; i++) pool.Submit([&]() {});
    ASSERT_TRUE(pool.QueueSize() > 0);
    block = false;
}

