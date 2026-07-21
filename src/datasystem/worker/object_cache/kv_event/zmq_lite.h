/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

/**
 * Description: Minimal reusable ZMQ helpers.
 */
#ifndef DATASYSTEM_WORKER_OBJECT_CACHE_KV_EVENT_ZMQ_LITE_H
#define DATASYSTEM_WORKER_OBJECT_CACHE_KV_EVENT_ZMQ_LITE_H

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <set>
#include <string>

#include "datasystem/utils/status.h"

namespace datasystem {

enum class ZmqLiteSocketType { PUB, SUB };
enum class ZmqLiteSendFlags { NONE, SNDMORE };
enum class ZmqLiteRecvFlags { NONE, DONTWAIT };

struct ZmqLiteContextState {
    bool closed{ false };
    std::mutex mutex;
    std::set<intptr_t> sockets;
};

class ZmqLiteMessage {
public:
    ZmqLiteMessage();
    ~ZmqLiteMessage();

    ZmqLiteMessage(const ZmqLiteMessage &) = delete;
    ZmqLiteMessage &operator=(const ZmqLiteMessage &) = delete;

    ZmqLiteMessage(ZmqLiteMessage &&other) noexcept;
    ZmqLiteMessage &operator=(ZmqLiteMessage &&other) noexcept;

    const void *Data() const;
    void *Data();
    size_t Size() const;
    bool More() const;
    bool Empty() const;

    Status CopyBuffer(const void *data, size_t size);

private:
    struct Impl;

    friend class ZmqLiteSocket;

    void *GetHandle();
    Status Close();

    std::unique_ptr<Impl> impl_;
};

class ZmqLiteSocket {
public:
    ZmqLiteSocket() = default;
    ~ZmqLiteSocket();

    ZmqLiteSocket(const ZmqLiteSocket &) = delete;
    ZmqLiteSocket &operator=(const ZmqLiteSocket &) = delete;

    ZmqLiteSocket(ZmqLiteSocket &&other) noexcept;
    ZmqLiteSocket &operator=(ZmqLiteSocket &&other) noexcept;

    bool IsValid() const;
    void *GetHandle() const;
    void Close();

    Status SetLinger(int value);
    Status SetRcvtimeo(int value);
    Status SubscribeAll();
    Status Bind(const std::string &endpoint);
    Status Connect(const std::string &endpoint);
    Status RecvMsg(ZmqLiteMessage &msg, ZmqLiteRecvFlags flags);
    Status SendMsg(ZmqLiteMessage &msg, ZmqLiteSendFlags flags);

    static Status ZmqErrnoToStatus(int err, const std::string &message, StatusCode defaultCode);

private:
    friend class ZmqLiteContext;

    ZmqLiteSocket(void *sock, std::shared_ptr<ZmqLiteContextState> contextState);
    static void CloseHandle(void *sock);
    Status SetOption(int option, const void *value, size_t len);

    void *sock_{ nullptr };
    std::shared_ptr<ZmqLiteContextState> contextState_;
};

class ZmqLiteContext {
public:
    explicit ZmqLiteContext(int ioThreads = 1, int maxSockets = 1024);
    ~ZmqLiteContext();

    ZmqLiteContext(const ZmqLiteContext &) = delete;
    ZmqLiteContext &operator=(const ZmqLiteContext &) = delete;

    Status Init();
    void *GetHandle() const;
    ZmqLiteSocket CreateZmqSocket(ZmqLiteSocketType type);
    void Close(bool logging = true);

#ifdef WITH_TESTS
    size_t GetOpenSocketCount() const;
#endif

private:
    void *ctx_{ nullptr };
    int ioThreads_;
    int maxSockets_;
    std::shared_ptr<ZmqLiteContextState> state_;
};

}  // namespace datasystem

#endif  // DATASYSTEM_WORKER_OBJECT_CACHE_KV_EVENT_ZMQ_LITE_H
