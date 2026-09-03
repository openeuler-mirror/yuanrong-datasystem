#include "test_harness.h"
#include "common/config.h"
#include "common/thread_pool.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>

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

TEST(ConfiguredReadConcurrencyIsApplied) {
    constexpr int kWriteThreads = 2;
    constexpr int kTotalThreads = 5;
    constexpr int kExpectedReadThreads = 3;
    constexpr int kTaskCount = 6;
    constexpr auto kWaitTimeout = std::chrono::seconds(2);

    Config cfg;
    cfg.numThreads = kWriteThreads;
    cfg.numTotalThreads = kTotalThreads;

    ThreadPool pool(cfg.NumReadThreads());
    std::mutex mutex;
    std::condition_variable cv;
    int started = 0;
    int active = 0;
    int maxActive = 0;
    int completed = 0;
    bool release = false;

    for (int i = 0; i < kTaskCount; ++i) {
        pool.Submit([&]() {
            std::unique_lock<std::mutex> lock(mutex);
            ++started;
            ++active;
            maxActive = std::max(maxActive, active);
            cv.notify_all();
            cv.wait(lock, [&]() { return release; });
            --active;
            ++completed;
            cv.notify_all();
        });
    }

    bool saturated = false;
    int startedAtLimit = 0;
    int maxActiveAtLimit = 0;
    size_t queuedAtLimit = 0;
    {
        std::unique_lock<std::mutex> lock(mutex);
        saturated = cv.wait_for(lock, kWaitTimeout, [&]() {
            return started == kExpectedReadThreads;
        });
        startedAtLimit = started;
        maxActiveAtLimit = maxActive;
        queuedAtLimit = pool.QueueSize();
        release = true;
    }
    cv.notify_all();

    bool drained = false;
    int finalMaxActive = 0;
    {
        std::unique_lock<std::mutex> lock(mutex);
        drained = cv.wait_for(lock, kWaitTimeout, [&]() {
            return completed == kTaskCount;
        });
        finalMaxActive = maxActive;
    }

    ASSERT_TRUE(saturated);
    ASSERT_TRUE(drained);
    ASSERT_EQ(startedAtLimit, kExpectedReadThreads);
    ASSERT_EQ(maxActiveAtLimit, kExpectedReadThreads);
    ASSERT_EQ(queuedAtLimit, static_cast<size_t>(kTaskCount - kExpectedReadThreads));
    ASSERT_EQ(finalMaxActive, kExpectedReadThreads);
}
