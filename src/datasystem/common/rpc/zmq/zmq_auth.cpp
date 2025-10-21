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
#include "datasystem/common/rpc/zmq/zmq_auth.h"

namespace datasystem {
const std::string CURVE = "CURVE";

void ZmqAuthRequest::Print(std::ostream &out) const
{
    out << "Auth Request\n"
        << version_ << "\n"
        << sequence_ << "\n"
        << domain_ << "\n"
        << address_ << "\n"
        << identity_ << "\n"
        << mechanism_;
}

Status ZmqAuthRequest::Reply(const std::shared_ptr<ZmqSocket> &sock, const std::string &statusCode,
                             const std::string &statusText)
{
    ZmqMsgFrames frames;
    frames.push_back(std::move(version_));
    frames.push_back(std::move(sequence_));
    ZmqMessage statusCodeMsg;
    RETURN_IF_NOT_OK(statusCodeMsg.CopyString(statusCode));
    frames.push_back(std::move(statusCodeMsg));
    ZmqMessage statusTextMsg;
    RETURN_IF_NOT_OK(statusTextMsg.CopyString(statusText));
    frames.push_back(std::move(statusTextMsg));
    ZmqMessage userId;
    std::string mechanism = ZmqMessageToString(mechanism_);
    if (mechanism == CURVE) {
        userId = std::move(clientKey_);
    }
    VLOG(RPC_LOG_LEVEL) << "ZmqAuthHandler reply:\n" << statusCode << "\n" << statusText;
    frames.push_back(std::move(userId));
    ZmqMessage metaData;
    frames.push_back(std::move(metaData));
    return sock->SendAllFrames(frames, ZmqSendFlags::NONE);
}

Status ZmqAuthHandler::Init(const std::shared_ptr<ZmqContext> &ctx)
{
    CHECK_FAIL_RETURN_STATUS(ctx != nullptr, K_RUNTIME_ERROR, "ZmqAuthHandler init failed, context is null");
    handler_ = std::make_shared<ZmqSocket>(ctx, ZmqSocketType::REP);
    RETURN_IF_NOT_OK(handler_->Bind(authEndpt_));
    return Status::OK();
}

void ZmqAuthHandler::Stop()
{
    globalInterrupt_ = true;
}

void ZmqAuthHandler::ConfigCurve(const char *clientPublicKey)
{
    clientKeys_.insert(clientPublicKey);
}

Status ZmqAuthHandler::ParseAuthRequest(ZmqMsgFrames &frames, ZmqAuthRequest &out)
{
    constexpr int kMinFrameSize = 6;
    CHECK_FAIL_RETURN_STATUS(
        frames.size() >= kMinFrameSize, StatusCode::K_INVALID,
        "Expect at least " + std::to_string(kMinFrameSize) + " frames, got " + std::to_string(frames.size()));
    ZmqAuthRequest zmqAuthRequest;
    auto rq = &zmqAuthRequest;
    // Parse all standard frames.
    rq->version_ = std::move(frames.front());
    frames.pop_front();
    rq->sequence_ = std::move(frames.front());
    frames.pop_front();
    rq->domain_ = std::move(frames.front());
    frames.pop_front();
    rq->address_ = std::move(frames.front());
    frames.pop_front();
    rq->identity_ = std::move(frames.front());
    frames.pop_front();
    rq->mechanism_ = std::move(frames.front());
    frames.pop_front();

    std::string version = ZmqMessageToString(rq->version_);
    CHECK_FAIL_RETURN_STATUS(version == "1.0", StatusCode::K_INVALID, "Expect libzmq version 1.0, got " + version);

    // Parse mechanism-specific frames.
    std::string mechanism = ZmqMessageToString(rq->mechanism_);
    if (mechanism == CURVE) {
        CHECK_FAIL_RETURN_STATUS(!frames.empty(), StatusCode::K_INVALID, "Unexpected frames!");
        RETURN_IF_NOT_OK(Z85Encode(std::move(frames.front()), &rq->clientKey_));
        frames.pop_front();
    } else {
        RETURN_STATUS(K_INVALID, FormatString("Authentication mechanism(%s) not supported", mechanism));
    }
    CHECK_FAIL_RETURN_STATUS(frames.empty(), StatusCode::K_INVALID, "Unexpected frames!");
    VLOG(RPC_LOG_LEVEL) << zmqAuthRequest;
    out = std::move(zmqAuthRequest);
    return Status::OK();
}

void ZmqAuthHandler::Auth(ZmqAuthRequest &zmqAuthRequest)
{
    bool allowed = false;
    auto rq = &zmqAuthRequest;

    // Mechanism-specific checks.
    std::string mechanism = ZmqMessageToString(rq->mechanism_);
    if (mechanism == CURVE) {
        std::string tmpKey(reinterpret_cast<const char*>(rq->clientKey_.Data()), rq->clientKey_.Size());
        if (clientKeys_.find(tmpKey.c_str()) != clientKeys_.end()) {
            allowed = true;
        }
    }
    if (allowed) {
        VLOG(RPC_LOG_LEVEL) << "ZmqAuthHandler allowed with mechanism: " << mechanism;
        const std::string statusCode = "200";
        rq->Reply(handler_, statusCode, "OK");
    } else {
        LOG(ERROR) << "Authentication failed at ZAP request handling";
        const std::string statusCode = "400";
        rq->Reply(handler_, statusCode, "No access");
    }
}

Status ZmqAuthHandler::ProcessAuthHelper()
{
    ZmqMsgFrames frames;
    RETURN_IF_NOT_OK(handler_->GetAllFrames(frames));
    ZmqAuthRequest rq;
    RETURN_IF_NOT_OK(ParseAuthRequest(frames, rq));
    Auth(rq);
    return Status::OK();
}

Status ZmqAuthHandler::WorkerEntry()
{
    VLOG(RPC_LOG_LEVEL) << "ZmqAuthHandler WorkerEntry started\n";
    CHECK_FAIL_RETURN_STATUS(handler_ != nullptr, K_RUNTIME_ERROR,
                             "ZmqAuthHandler WorkerEntry failed, handler is null");
    while (true) {
        zmq_pollitem_t items[]{ { static_cast<void *>(*handler_), 0, ZMQ_POLLIN, 0 } };
        if (globalInterrupt_) {
            VLOG(RPC_LOG_LEVEL) << "ZmqAuthHandler shutdown";
            break;
        }
        auto n = zmq_poll(items, 1, RPC_POLL_TIME);
        // Check for interrupt.
        if (globalInterrupt_) {
            VLOG(RPC_KEY_LOG_LEVEL) << "ZmqAuthHandler shutdown";
            break;
        }
        if (n == 0) {
            continue;
        }
        if (items[0].revents & ZMQ_POLLIN) {
            LOG_IF_ERROR(ProcessAuthHelper(), "Error in processing authentication request");
        }
    }
    if (handler_ != nullptr && handler_->IsValid()) {
        handler_->Close();
    }
    return Status::OK();
}
}  // namespace datasystem
