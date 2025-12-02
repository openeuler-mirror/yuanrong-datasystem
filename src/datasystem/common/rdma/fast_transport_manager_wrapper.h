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
 * Description: Wrappers for Fast Transport Manager (UrmaManager & UcpManager) logic.
 */

#ifndef DATASYSTEM_COMMON_FAST_TRANSPORT_MANAGER_WRAPPER_H
#define DATASYSTEM_COMMON_FAST_TRANSPORT_MANAGER_WRAPPER_H

#ifdef USE_URMA
#include "datasystem/common/rdma/urma_manager.h"
#endif
#ifdef USE_RDMA
#include "datasystem/common/rdma/ucp_manager.h"
#endif
#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/protos/utils.pb.h"

namespace datasystem {
/**
 * @brief Check if fast transport is enabled.
 * @return True if fast transport logic is compiled and the flag is set, else false.
 */
bool IsFastTransportEnabled();

/**
 * @brief Check if URMA is enabled.
 * @return True if URMA logic is compiled and the flag is set, else false.
 */
bool IsUrmaEnabled();

/**
 * @brief Check if Ucp is enabled.
 * @return True if Ucp logic is compiled and the flag is set, else false.
 */
bool IsUcpEnabled();

/**
 * @brief Initialize Fast Transport Manager.
 * @param[in] hostport Local ds worker ip address.
 * @return Status of the call.
 */
Status InitializeFastTransportManager(const HostPort &hostport);

/**
 * @brief Remove remote fast transport node in urma and ucp
 * @param[in] remoteAddress Remote Worker Address
 * @return Status of the call.
 */
Status RemoveRemoteFastTransportNode(const HostPort &remoteAddress);

/**
 * @brief Register fast transport memory (as segment).
 * @param[in] segAddress Starting address of the segment.
 * @param[in] segSize Size of the segment.
 * @return Status of the call.
 */
Status RegisterFastTransportMemory(void *segAddress, const uint64_t &segSize);

/**
 * @brief Wait for the event of urma_write/ucp_put_nbx finish.
 * @param[in] keys The request ids to wait for events.
 * @param[in] remainingTime The callback to calculate remaining time.
 * @param[in] errorHandler The error handling callback.
 * @return Status of the call.
 */
Status WaitFastTransportEvent(std::vector<uint64_t> &keys, std::function<int64_t(void)> remainingTime,
                              std::function<Status(Status &)> errorHandler);

/**
 * @brief Calculate the segment info (address and size) from shared memory unit.
 * @param[in] shmUnit The shared memory unit.
 * @param[in] memoryAddress The actual address of the memory.
 * @param[out] segAddress The segment address.
 * @param[out] segSize The segment size.
 * @return Status of the call.
 */
void GetSegmentInfoFromShmUnit(std::shared_ptr<ShmUnit> shmUnit, uint64_t memoryAddress, uint64_t &segAddress,
                               uint64_t &segSize);

/**
 * @brief Trigger UrmaManager logic to import segment and write payload.
 * @param[in] UrmaRemoteAddrPb  Protobuf contians remote host address, remote urma segment address and data offset
 * @param[in] localSegAddress Starting address of the segment (e.g. Arena start address).
 * @param[in] localSegSize Total size of the segment (e.g. Arena size).
 * @param[in] localObjectAddress Object address.
 * @param[in] readOffset Offset in the object to read.
 * @param[in] readSize Size of the object.
 * @param[in] metaDataSize Size of metadata (SHM metadata stored as part of object).
 * @param[in] blocking Whether to blocking wait for the urma_write to finish.
 * @param[out] keys The new request id to wait for if not blocking.
 * @return Status of the call.
 */
Status UrmaWritePayload(const UrmaRemoteAddrPb &urmaInfo, const uint64_t &localSegAddress, const uint64_t &localSegSize,
                        const uint64_t &localObjectAddress, const uint64_t &readOffset, const uint64_t &readSize,
                        const uint64_t &metaDataSize, bool blocking, std::vector<uint64_t> &keys);

/**
 * @brief Fill in ucp_info pb for UCP RDMA.
 * @param[in] segAddress The virtual address of the segment.
 * @param[in] dataOffset The data offset of the segment.
 * @param[in] srcIpAddr The ip address of data owner.
 * @param[out] ucpInfo Protobuf contians remote worker UCP info.
 * @return Status of the call.
 */
Status FillUcpInfo(uint64_t segAddress, uint64_t dataOffset, const std::string &srcIpAddr, UcpRemoteInfoPb &ucpInfo);

/**
 * @brief Trigger UcpManager logic to import segment and write payload.
 * @param[in] ucpInfo Protobuf contians remote worker UCP info.
 * @param[in] localObjectAddress Object address.
 * @param[in] readOffset Offset in the object to read.
 * @param[in] readSize Size of the object.
 * @param[in] metaDataSize Size of metadata (SHM metadata stored as part of object).
 * @param[in] blocking Whether to blocking wait for the ucp_put_nbx to finish.
 * @param[out] keys The new request id to wait for if not blocking.
 * @return Status of the call.
 */
Status UcpPutPayload(const UcpRemoteInfoPb &ucpInfo, const uint64_t &localObjectAddress, const uint64_t &readOffset,
                     const uint64_t &readSize, const uint64_t &metaDataSize, bool blocking,
                     std::vector<uint64_t> &keys);
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_FAST_TRANSPORT_MANAGER_WRAPPER_H