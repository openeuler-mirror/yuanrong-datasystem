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
 * Description: UCX-UCP manager for ucp context, ucp worker, ucp endpoint, etc.
 */
#include "datasystem/common/rdma/ucp_manager.h"

namespace datasystem {

UcpManager &UcpManager::Instance()
{
    static UcpManager manager;
    return manager;
}

UcpManager::UcpManager()
{
}

UcpManager::~UcpManager()
{
}

Status UcpManager::Init()
{
    return Status::OK();
}

Status UcpManager::RemoveEndpoint(const HostPort &remoteAddress)
{
    (void)remoteAddress;
    return Status::OK();
}

Status UcpManager::UcpPutPayload(const UcpRemoteInfoPb &ucpInfo, const uint64_t &localObjectAddress,
                                 const uint64_t &readOffset, const uint64_t &readSize, const uint64_t &metaDataSize,
                                 bool blocking, std::vector<uint64_t> &keys)
{
    // ucp_put_nbx(ep, msg_addr, strlen(msg)+1, remote_buf, rkey, &param);
    // msg_addr = localObjectAddress + readOffset + metaDataSize + writtenSize
    // 当无需切块时，writtenSize为0
    (void)ucpInfo;
    (void)localObjectAddress;
    (void)readOffset;
    (void)readSize;
    (void)metaDataSize;
    (void)blocking;
    (void)keys;
    return Status::OK();
}

Status UcpManager::RegisterSegment(const uint64_t &segAddress, const uint64_t &segSize)
{
    (void)segAddress;
    (void)segSize;
    return Status::OK();
}

// remote_buf = segAddress + dataOffset
// rkey可以通过segAddress从UcpManager的local segment map中查到
// pb的remote_ip_addr(外面已经填过了，此处不需要)
// srcIpAddr -> remote_worker_addr(调用manager的GetOrSelectRecvWorkerAddress(srcIpAddr))
Status UcpManager::FillUcpInfoImpl(uint64_t segAddress, uint64_t dataOffset, const std::string &srcIpAddr,
                                   UcpRemoteInfoPb &ucpInfo)
{
    (void)segAddress;
    (void)dataOffset;
    (void)srcIpAddr;
    (void)ucpInfo;
    return Status::OK();
}

Status UcpManager::WaitToFinish(uint64_t requestId, int64_t timeoutMs)
{
    (void)requestId;
    (void)timeoutMs;
    return Status::OK();
}
}  // namespace datasystem
