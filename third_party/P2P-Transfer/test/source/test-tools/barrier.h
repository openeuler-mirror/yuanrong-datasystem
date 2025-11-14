
/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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