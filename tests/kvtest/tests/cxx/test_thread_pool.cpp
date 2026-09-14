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

// A bound on the pending queue turns an outrunning producer into a counted
// drop instead of unbounded memory growth.
TEST(QueueBoundDropsExcessAndCounts) {
    constexpr size_t kBound = 4;
    constexpr int kExtra = 16;
    ThreadPool pool(1, kBound);
    std::atomic<bool> block{true};
    pool.Submit([&]() {
        while (block.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));

    std::atomic<int> executed{0};
    for (int i = 0; i < kExtra; i++) pool.Submit([&]() { executed++; });

    ASSERT_EQ(pool.QueueSize(), kBound);
    ASSERT_EQ(pool.DroppedCount(), static_cast<uint64_t>(kExtra - kBound));

    block = false;
    for (int i = 0; i < 200 && pool.QueueSize() > 0; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_EQ(pool.QueueSize(), static_cast<size_t>(0));
    ASSERT_EQ(executed.load(), static_cast<int>(kBound));
}

// Default construction stays unbounded: existing callers see no behavior change.
TEST(QueueBoundDefaultsToUnbounded) {
    ThreadPool pool(1);
    std::atomic<bool> block{true};
    pool.Submit([&]() {
        while (block.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    for (int i = 0; i < 200; i++) pool.Submit([&]() {});
    ASSERT_EQ(pool.QueueSize(), static_cast<size_t>(200));
    ASSERT_EQ(pool.DroppedCount(), static_cast<uint64_t>(0));
    block = false;
}

TEST(QueueBoundZeroIsUnbounded) {
    ThreadPool pool(1, 0);
    std::atomic<bool> block{true};
    pool.Submit([&]() {
        while (block.load()) std::this_thread::sleep_for(std::chrono::milliseconds(1));
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(20));
    for (int i = 0; i < 50; i++) pool.Submit([&]() {});
    ASSERT_EQ(pool.QueueSize(), static_cast<size_t>(50));
    ASSERT_EQ(pool.DroppedCount(), static_cast<uint64_t>(0));
    block = false;
}

// A bound above the offered load must be invisible: nothing dropped, all run.
TEST(NoDropsBelowQueueBound) {
    constexpr int kTasks = 100;
    ThreadPool pool(2, 1024);
    std::atomic<int> executed{0};
    for (int i = 0; i < kTasks; i++) pool.Submit([&]() { executed++; });
    for (int i = 0; i < 200 && executed.load() < kTasks; i++) {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    ASSERT_EQ(executed.load(), kTasks);
    ASSERT_EQ(pool.DroppedCount(), static_cast<uint64_t>(0));
    ASSERT_EQ(pool.QueueSize(), static_cast<size_t>(0));
}
