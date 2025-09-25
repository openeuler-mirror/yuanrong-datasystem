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

/**
 * Description: Exclusive connection manager.
 */

#include "datasystem/common/rpc/zmq/exclusive_conn_mgr.h"

#include <sstream>
#include <sys/types.h>
#include <sys/syscall.h>
#include <unistd.h>

#include "datasystem/common/log/log.h"

// A glibc work-around for gettid(). Newer glibc has gettid() wrapper rather than direct system call.
#define gettid() syscall(SYS_gettid)

namespace datasystem {

// global variable in datasystem thread scope
// The instantiation only happens on first access from the given thread to this global var.
thread_local ExclusiveConnMgr gExclusiveConnMgr;

ExclusiveConnMgr::ExclusiveConnMgr() : sockTable_(TABLE_SIZE)
{
    pid_ = getpid();
    tid_ = gettid();
    LOG(INFO) << "A user thread " << tid_ << " in process " << pid_ << " created an exclusive connection manager.";
}

Status ExclusiveConnMgr::CreateExclusiveConnection(int32_t exclusiveId, int64_t timeoutMs, const std::string &sockPath)
{
    CHECK_FAIL_RETURN_STATUS(IsExclusiveIdInRange(exclusiveId), K_RUNTIME_ERROR, "Exclusive id out of range");

    // All slots in the table are pre-allocated (fixed length table). However, they might contain nullptr, which means
    // the connection has not been created yet (it needs to be created).
    if (!sockTable_[exclusiveId]) {
        // Create connection entry with a uds socket and connect it to the service
        std::unique_ptr<ExclusiveConn> conn;
        RETURN_IF_NOT_OK(CreateExclusiveConn(conn, sockPath));
        VLOG(RPC_LOG_LEVEL) << FormatString("Exclusive connection created. exclusiveId: %d, fd: %d, timeout: %d",
                                            exclusiveId, conn->sockFd_.GetFd(), timeoutMs);
        sockTable_[exclusiveId] = std::move(conn);
    }

    // A previous usage of the socket fd changes the remaining time allowed for sends/recv.
    // Update this timeout now so that it starts with a fresh value
    sockTable_[exclusiveId]->sockFd_.SetTimeoutEnforced(timeoutMs);
    VLOG(RPC_LOG_LEVEL) << "Exclusive connection initialized. Timeout: " << timeoutMs;
    return Status::OK();
}

Status ExclusiveConnMgr::GetExclusiveConnDecoder(int32_t exclusiveId, ZmqMsgDecoder *&decoder)
{
    CHECK_FAIL_RETURN_STATUS(IsExclusiveIdInRange(exclusiveId), K_RUNTIME_ERROR, "Exclusive id out of range");

    CHECK_FAIL_RETURN_STATUS(sockTable_[exclusiveId] != nullptr, K_RUNTIME_ERROR,
                             "Missing exclusive socket connection: " + std::to_string(exclusiveId));

    decoder = sockTable_[exclusiveId]->decoder_.get();
    return Status::OK();
}

Status ExclusiveConnMgr::GetExclusiveConnEncoder(int32_t exclusiveId, ZmqMsgEncoder *&encoder)
{
    CHECK_FAIL_RETURN_STATUS(IsExclusiveIdInRange(exclusiveId), K_RUNTIME_ERROR, "Exclusive id out of range");

    CHECK_FAIL_RETURN_STATUS(sockTable_[exclusiveId] != nullptr, K_RUNTIME_ERROR,
                             "Missing exclusive socket connection: " + std::to_string(exclusiveId));

    encoder = sockTable_[exclusiveId]->encoder_.get();
    return Status::OK();
}

Status ExclusiveConnMgr::CreateExclusiveConn(std::unique_ptr<ExclusiveConn> &conn, const std::string &sockPath)
{
    VLOG(RPC_LOG_LEVEL) << "Creaing an exclusive UDS connection and associated encoders/decoders";
    conn = std::make_unique<ExclusiveConn>();
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(conn->sockFd_.Connect(FormatString("ipc://%s", sockPath)),
        FormatString("Failed to create exclusive UDS connection with sockPath %s", sockPath));

    // The sock fd is owned here in the exclusive connection manager.
    // These encoder/decoder constructors will create encoder/decoders that directly reference this sockFd,
    // via pointer.
    conn->encoder_ = std::make_unique<ZmqMsgEncoder>(&conn->sockFd_);
    conn->decoder_ = std::make_unique<ZmqMsgDecoder>(&conn->sockFd_);

    return Status::OK();
}

std::string ExclusiveConnMgr::GetExclusiveConnMgrName() const
{
    std::ostringstream ss;
    ss << pid_ << ":" << tid_;
    return ss.str();
}

Status ExclusiveConnMgr::CloseExclusiveConn(int32_t exclusiveId)
{
    if (IsExclusiveIdInRange(exclusiveId)) {
        sockTable_[exclusiveId].reset();
    }
    return Status::OK();
}
}  // namespace datasystem
