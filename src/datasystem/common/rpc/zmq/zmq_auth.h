/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2022. All rights reserved.
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
 * Description: Zmq Authentication handler.
 */
#ifndef DATASYSTEM_COMMON_RPC_ZMQ_AUTH_H
#define DATASYSTEM_COMMON_RPC_ZMQ_AUTH_H

#include "datasystem/common/rpc/zmq/zmq_socket.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/wait_post.h"

namespace datasystem {
/**
 * A class for working with ZAP requests and replies.
 * Used in auth to simplify working with RFC 27 messages.
 * See ZMQ RFC 27 for more details.
 */
class ZmqAuthRequest {
public:
    void Print(std::ostream &out) const;

    friend std::ostream &operator<<(std::ostream &out, const ZmqAuthRequest &rq)
    {
        rq.Print(out);
        return out;
    }

    /**
     * @brief Send out reply for this authentication request.
     * @param[in] sock Pointer to ZmqSocket to send reply message.
     * @param[in] statusCode Status code.
     * @param[in] statusText Status message.
     * @return Status of the call.
     */
    Status Reply(const std::shared_ptr<ZmqSocket> &sock, const std::string &statusCode, const std::string &statusText);

private:
    friend class ZmqAuthHandler;
    ZmqMessage version_;    // Version number, must be "1.0".
    ZmqMessage sequence_;   // Sequence number of request.
    ZmqMessage domain_;     // Server socket domain.
    ZmqMessage address_;    // Client IP address.
    ZmqMessage identity_;   // Server socket identity.
    ZmqMessage mechanism_;  // Security mechanism.
    ZmqMessage clientKey_;  // CURVE client public key.
};

/**
 * A class for map or set to work with const char * type of zmq authentication keys.
 */
struct AuthKeyComparator {
    bool operator()(const char *a, const char *b) const
    {
        return std::strcmp(a, b) < 0;
    }
};

typedef std::set<const char *, AuthKeyComparator> AuthKeySet;
typedef std::unordered_map<std::string, std::unordered_set<std::string>> RawSvcToKeyMapping;
typedef std::unordered_map<std::string, AuthKeySet> SvcToKeyMapping;

/**
 * A class for doing authentication for incoming connections over ZAP protocol. Support ZMQ CURVE mechanism
 * based on client publickey verification. See ZMQ RFC 27 for more details.
 */
class ZmqAuthHandler {
public:
    ZmqAuthHandler() : globalInterrupt_(false)
    {
    }

    Status Init(const std::shared_ptr<ZmqContext> &ctx);

    void Stop();

    ~ZmqAuthHandler() = default;

    /**
     * @brief Allow CURVE-mechanism client with a specific curve public key to connect.
     * @note This function can be called multiple times to allow multiple curve public keys.
     * @param[in] clientPublicKey Client's public key.
     */
    void ConfigCurve(const char *clientPublicKey);

    /**
     * @brief A helper function to parse the incoming authentication messages to a ZmqAuthRequest object.
     * @param[in] frames A deque containing all the message frames of the incoming authentication request.
     * @param[out] out A unique pointer to a ZmqAuthRequest object.
     * @return Status of call.
     */
    static Status ParseAuthRequest(ZmqMsgFrames &frames, ZmqAuthRequest &out);

    /**
     * @brief Deny or allow the incoming authentication request and send out the corresponding response.
     * @param[in] rq A unique pointer to a ZmqAuthRequest object.
     */
    void Auth(ZmqAuthRequest &rq);

    /**
     * @brief Helper function to parse and process incoming authentication request.
     * @return Status of call.
     */
    Status ProcessAuthHelper();

    /**
     * @brief Main loop for the authentication handler thread.
     * @return Status of call.
     */
    Status WorkerEntry();

private:
    std::atomic<bool> globalInterrupt_;
    std::shared_ptr<ZmqSocket> handler_{ nullptr };
    const std::string authEndpt_ = "inproc://zeromq.zap.01";
    AuthKeySet clientKeys_;  // Client public keys.
};
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_ZMQ_AUTH_H
