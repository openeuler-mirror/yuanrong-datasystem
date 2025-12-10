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

ExclusiveConnMgr::ExclusiveConnMgr()
{
    std::ostringstream ss;
    auto pid = getpid();
    auto tid = gettid();
    ss << pid << ":" << tid;
    name_ = ss.str();
    LOG(INFO) << "A user thread " << tid << " in process " << pid << " created an exclusive connection manager.";
}

Status ExclusiveConnMgr::CreateExclusiveConnection(int32_t exclusiveId, int64_t timeoutMs, const std::string &sockPath)
{
    // Create the connection if it has not been created yet.
    auto sockTableIter = sockMap_.find(exclusiveId);
    if (sockTableIter == sockMap_.end()) {
        // Create connection entry with a uds socket and connect it to the service
        std::unique_ptr<ExclusiveConn> conn;
        RETURN_IF_NOT_OK(CreateExclusiveConn(conn, sockPath));
        VLOG(RPC_LOG_LEVEL) << FormatString("Exclusive connection created. exclusiveId: %d, fd: %d, timeout: %d",
                                            exclusiveId, conn->sockFd_.GetFd(), timeoutMs);
        sockTableIter = sockMap_.emplace(exclusiveId, std::move(conn)).first;
    }

    // A previous usage of the socket fd changes the remaining time allowed for sends/recv.
    // Update this timeout now so that it starts with a fresh value
    sockTableIter->second->sockFd_.SetTimeoutEnforced(timeoutMs);
    VLOG(RPC_LOG_LEVEL) << "Exclusive connection initialized. Timeout: " << timeoutMs;
    return Status::OK();
}

Status ExclusiveConnMgr::GetExclusiveConnDecoder(int32_t exclusiveId, ZmqMsgDecoder *&decoder)
{
    auto sockTableIter = sockMap_.find(exclusiveId);
    CHECK_FAIL_RETURN_STATUS(sockTableIter != sockMap_.end(), K_RUNTIME_ERROR,
                             "Missing exclusive socket connection: " + std::to_string(exclusiveId));

    decoder = sockTableIter->second->decoder_.get();
    return Status::OK();
}

Status ExclusiveConnMgr::GetExclusiveConnEncoder(int32_t exclusiveId, ZmqMsgEncoder *&encoder)
{
    auto sockTableIter = sockMap_.find(exclusiveId);
    CHECK_FAIL_RETURN_STATUS(sockTableIter != sockMap_.end(), K_RUNTIME_ERROR,
                             "Missing exclusive socket connection: " + std::to_string(exclusiveId));

    encoder = sockTableIter->second->encoder_.get();
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
    return name_;
}

Status ExclusiveConnMgr::CloseExclusiveConn(int32_t exclusiveId)
{
    VLOG(RPC_LOG_LEVEL) << "Close exclusive connection " << std::to_string(exclusiveId);
    // Remove the exclusive connection from socket map.
    auto sockTableIter = sockMap_.find(exclusiveId);
    if (sockTableIter != sockMap_.end()) {
        sockMap_.erase(sockTableIter);
    }
    return Status::OK();
}
}  // namespace datasystem
