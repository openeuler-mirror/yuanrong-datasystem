/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#ifndef BARRIER_H
#define BARRIER_H

#include <condition_variable>
#include <mutex>
#include <stdexcept>
#include <thread>

class Barrier {
public:
    explicit Barrier(std::size_t num) : numThreads(num), waitCount(0), instance(0)
    {
        if (num == 0) {
            throw std::invalid_argument("Barrier thread count cannot be 0");
        }
    }

    Barrier(const Barrier &) = delete;
    Barrier &operator=(const Barrier &) = delete;

    void Wait()
    {
        std::unique_lock<std::mutex> lock(mut);
        auto inst = instance;

        if (++waitCount == numThreads) {
            waitCount = 0;
            instance++;
            cv.notify_all();
        } else {
            cv.wait(lock, [this, inst] { return instance != inst; });
        }
    }

private:
    std::size_t numThreads;
    std::size_t waitCount;
    std::size_t instance;
    std::mutex mut;
    std::condition_variable cv;
};

#endif