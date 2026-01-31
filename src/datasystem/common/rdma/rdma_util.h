/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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

#ifndef DATASYSTEM_COMMON_RDMA_RDMA_UTIL_H
#define DATASYSTEM_COMMON_RDMA_RDMA_UTIL_H

#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>

#include "datasystem/common/log/log.h"
#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"

DS_DECLARE_bool(urma_register_whole_arena);
DS_DECLARE_bool(rdma_register_whole_arena);

namespace datasystem {

template <typename T>
using custom_unique_ptr = std::unique_ptr<T, std::function<void(T *)>>;

template <typename T>
inline custom_unique_ptr<T> MakeCustomUnique(T *p, std::function<void(T *)> custom_delete)
{
    if (p) {
        return custom_unique_ptr<T>(p, custom_delete);
    }

    LOG(WARNING) << "Input pointer is null";
    return nullptr;
}

class Event {
public:
    explicit Event(uint64_t requestId) : requestId_(requestId) {}
    ~Event() = default;

    Status WaitFor(std::chrono::milliseconds timeout)
    {
        std::unique_lock<std::mutex> lock(eventMutex_);
        bool gotNotification = cv_.wait_for(lock, timeout, [this] { return ready_; });
        if (!gotNotification && !ready_) {
            RETURN_STATUS_LOG_ERROR(K_RPC_DEADLINE_EXCEEDED,
                                    FormatString("timedout waiting for request: %d", requestId_));
        }
        return Status::OK();
    }

    void NotifyAll()
    {
        std::unique_lock<std::mutex> lock(eventMutex_);
        ready_ = true;
        cv_.notify_all();
    }

    void SetFailed()
    {
        failed_ = true;
    }

    bool IsFailed()
    {
        return failed_;
    }

private:
    std::condition_variable cv_;
    mutable std::mutex eventMutex_;
    uint64_t requestId_;
    bool ready_{ false };
    bool failed_{ false };
};

struct LocalSgeInfo {
    uint64_t segAddr;       // local seg address
    uint64_t segSize;       // local seg total size
    uint64_t sgeAddr;       // object address
    uint64_t readOffset;    // read offset
    uint64_t writeSize;     // data size
    uint64_t metaDataSize;  // meta data size
};

struct RemoteSegInfo {
    uint64_t segAddr;    // remote destination seg address
    uint64_t segOffset;  // the seg offset of segAdress
    std::string host;    // the host of remote urma endpoint
    int32_t port;        // the host of remote urma endpoint
};
/**
 * @brief Get the Ethernet device name from the destination ip.
 * @param[in] ipAddr The destination ip address.
 * @param[out] devName The ethernet device name.
 * @return Status of the call.
 */
Status GetDevNameFromDestIp(const std::string &ipAddr, std::string &devName);

/**
 * @brief Get the Ethernet device name from the local ip.
 * @param[in] ipAddr The local ip address.
 * @param[out] devName The ethernet device name.
 * @return Status of the call.
 */
int GetDevNameFromLocalIp(const std::string &ipAddr, std::string &devName);

/**
 * @brief Get the RDMA device name from the Ethernet device name.
 * @note If there is no RDMA device for the input device name, it will search and return other RDMA device if possible.
 * @param[in] ethDevName The Ethernet device name.
 * @param[out] rdmaDevName The RDMA device name.
 * @return Status of the call.
 */
Status EthToRdmaDevName(std::string ethDevName, std::string &rdmaDevName);

enum class UrmaMode { IB = 0, UB = 1, UNKNOWN };
/**
 * @brief Check the URMA mode.
 * @return The urma mode, valid options are IB or UB.
 */
UrmaMode GetUrmaMode();

/**
 * @brief Fill the urma_info fields in the request protobuf.
 * @note The request protobuf needs to contain urma_info fields.
 * @param[in] localAddress The local address.
 * @param[in] pointer The pointer to shm unit.
 * @param[in] offset The offset of the pointer.
 * @param[in] metaSz The metadata size of shared memory.
 * @param[out] reqPb The request protobuf.
 * @return Status of the call.
 */
template <typename Req>
Status FillRequestUrmaInfo(const HostPort &localAddress, const void *pointer, uint64_t offset, uint64_t metaSz,
                           Req &reqPb)
{
    uint64_t segAddress;
    uint64_t dataOffset;
    if (FLAGS_urma_register_whole_arena) {
        segAddress = reinterpret_cast<uint64_t>(pointer) - offset;
        dataOffset = offset + metaSz;
    } else {
        segAddress = reinterpret_cast<uint64_t>(pointer);
        dataOffset = metaSz;
    }
    auto *urmaInfo = reqPb.mutable_urma_info();
    urmaInfo->set_seg_va(segAddress);
    urmaInfo->set_seg_data_offset(dataOffset);
    auto *remoteAddr = urmaInfo->mutable_request_address();
    remoteAddr->set_host(localAddress.Host());
    remoteAddr->set_port(localAddress.Port());
    return Status::OK();
}

/**
 * @brief Fill the ucp_info fields in the request protobuf.
 * @note The request protobuf needs to contain ucp_info fields.
 * @param[in] localAddress The local address.
 * @param[in] shmUnit The shared memory unit.
 * @param[in] metaSz The metadata size of shared memory.
 * @param[out] reqPb The request protobuf.
 * @return Status of the call.
 */
template <typename Req>
Status FillRequestUcpInfo(const HostPort &localAddress, const std::string &srcIpAddr, std::shared_ptr<ShmUnit> &shmUnit,
                          uint64_t metaSz, Req &reqPb)
{
    uint64_t segAddress;
    uint64_t dataOffset;
    if (FLAGS_rdma_register_whole_arena) {
        segAddress = reinterpret_cast<uint64_t>(shmUnit->GetPointer()) - shmUnit->GetOffset();
        dataOffset = shmUnit->GetOffset() + metaSz;
    } else {
        segAddress = reinterpret_cast<uint64_t>(shmUnit->GetPointer());
        dataOffset = metaSz;
    }
    auto *ucpInfo = reqPb.mutable_ucp_info();
    auto *destIpAddr = ucpInfo->mutable_remote_ip_addr();
    destIpAddr->set_host(localAddress.Host());
    destIpAddr->set_port(localAddress.Port());
    return FillUcpInfo(segAddress, dataOffset, srcIpAddr, *ucpInfo);
}

}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RDMA_RDMA_UTIL_H