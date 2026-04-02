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

#include "datasystem/common/rdma/rdma_util.h"
#ifdef USE_URMA
#include "datasystem/common/rdma/urma_manager.h"
#endif
#ifdef USE_RDMA
#include "datasystem/common/rdma/ucp_manager.h"
#endif
#include "datasystem/common/rdma/fast_transport_base.h"
#include "datasystem/common/shared_memory/shm_unit.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/protos/meta_transport.pb.h"

namespace datasystem {
/**
 * @brief Get an unique identifier for connection between device and the remote host.
 * @param[out] commId The uuid in string.
 * @return Status of the call.
 */
Status GetClientCommUuid(std::string &commId);

/**
 * @brief Client sets global remote h2d configurations according to connect options.
 * @param[in] enableRemoteH2D Whether to enable remote host to device data transfer.
 * @param[in] devId The NPU device id.
 */
void SetClientRemoteH2DConfig(bool enableRemoteH2D, uint32_t devId);

/**
 * @brief Set the fast transport mode for the client.
 * @param[in] fastTransportMode The fast transport mode, e.g. UB.
 * @param[in] transportSize The size of the client transport.
 */
void SetClientFastTransportMode(FastTransportMode fastTransportMode, uint64_t transportSize);

/**
 * @brief Initialize Fast Transport Manager.
 * @param[in] hostport Local ds worker ip address.
 * @return Status of the call.
 */
Status InitializeFastTransportManager(const HostPort &hostport);

/**
 * @brief Remove remote fast transport node in urma and ucp.
 * @param[in] remoteAddress Remote Worker Address.
 * @return Status of the call.
 */
Status RemoveRemoteFastTransportNode(const HostPort &remoteAddress);

/**
 * @brief Remove remote fast transport client in urma.
 * @param[in] clientId The client id.
 * @return Status of the call.
 */
Status RemoveRemoteFastTransportClient(const ClientKey &clientId);

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
 * @param[out] eventKeys The new request id to wait for if not blocking.
 * @param[in] waiter The optional event waiter.
 * @return Status of the call.
 */
Status UrmaWritePayload(const UrmaRemoteAddrPb &urmaInfo, const uint64_t &localSegAddress, const uint64_t &localSegSize,
                        const uint64_t &localObjectAddress, const uint64_t &readOffset, const uint64_t &readSize,
                        const uint64_t &metaDataSize, bool blocking, std::vector<uint64_t> &eventKeys,
                        std::shared_ptr<EventWaiter> waiter = nullptr);

/**
 * @brief Trigger UrmaManager logic to import segment and read payload.
 * @param[in] UrmaRemoteAddrPb  Protobuf contians remote host address, remote urma segment address and data offset
 * @param[in] localSegAddress Starting address of the segment (e.g. Arena start address).
 * @param[in] localSegSize Total size of the segment (e.g. Arena size).
 * @param[in] localObjectAddress Object address.
 * @param[in] dataSize Size of the object.
 * @param[in] metaSize Size of metadata (SHM metadata stored as part of object).
 * @param[out] keys The new request id to wait for.
 * @return Status of the call.
 */
Status UrmaRead(const UrmaRemoteAddrPb &urmaInfo, const uint64_t &localSegAddress, const uint64_t &localSegSize,
                const uint64_t &localObjectAddress, const uint64_t &dataSize, const uint64_t &metaSize,
                std::vector<uint64_t> &keys);

/**
 * @brief Trigger UrmaManager logic to import segment and write payload, without memcopy.
 * @param[in] RemoteSegInfo  The remote destination address info of data.
 * @param[in] LocalSgeInfo   The local source address info of data.
 * @param[in] readOffset Offset in the object to read.
 * @param[in] blocking Whether to blocking wait for the urma_write to finish.
 * @param[out] eventKeys The new request id to wait for if not blocking.
 * @return Status of the call.
 */
Status UrmaGatherWrite(const RemoteSegInfo &remoteInfo, const std::vector<LocalSgeInfo> &objInfos, bool blocking,
                       std::vector<uint64_t> &eventKeys);

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
 * @param[out] eventKeys The new request id to wait for if not blocking.
 * @return Status of the call.
 */
Status UcpPutPayload(const UcpRemoteInfoPb &ucpInfo, const uint64_t &localObjectAddress, const uint64_t &readOffset,
                     const uint64_t &readSize, const uint64_t &metaDataSize, bool blocking,
                     std::vector<uint64_t> &eventKeys);

/**
 * @brief Trigger UcpManager logic to import segment and write payload, without memcopy.
 * @param[in] ucpInfo Protobuf contains remote worker UCP info.
 * @param[in] metaDataSize Size of metadata (SHM metadata stored as part of object).
 * @param[in] objInfos The local source address info of data.
 * @param[in] blocking Whether to blocking wait for the ucp_put_nbx to finish.
 * @param[out] eventKeys The new request id to wait for if not blocking.
 * @return Status of the call.
 */
Status UcpGatherPut(const UcpRemoteInfoPb &ucpInfo, uint64_t metaDataSize, const std::vector<LocalSgeInfo> &objInfos,
                    bool blocking, std::vector<uint64_t> &eventKeys);

/**
 * @brief Trigger UcpManager logic to import segment and write payload.
 * @param[in] req Urma handshake request.
 * @param[out] rsp Urma handshake response.
 * @return Status of the call.
 */
Status ExchangeJfr(const UrmaHandshakeReqPb &req, UrmaHandshakeRspPb &rsp);

/**
 * @brief Shared logic for Urma handshake: ExchangeJfr then fill rsp if not already filled (Client–Worker or
 * Worker–Worker). Used by WorkerOCService::ExchangeUrmaConnectInfo and
 * WorkerWorkerTransportService::WorkerWorkerExchangeUrmaConnectInfo.
 */
Status DoExchangeUrmaConnectInfo(const UrmaHandshakeReqPb &req, UrmaHandshakeRspPb &rsp);

/**
 * @brief Check if the connection is stable.
 * @param[in] hostAddress The dst port address.
 * @param[in] instanceId The unqiue instance uuid from dst port.
 * @return Status of the connection.
 */
Status CheckTransportConnectionStable(const std::string &hostAddress, const std::string &instanceId);

/**
 * @brief Get local transport unique instance id.
 * @param[out] instanceId The local instance uuid.
 * @return Status of the call.
 */
Status GetLocalTransportInstanceId(std::string &instanceId);

/**
 * @brief Construct local transport request info.
 * @param[in] sendAddr The sender address.
 * @param[out] req The handshake request.
 * @param[in] clientEntityId The client entity id.
 * @return Status of the call.
 */
Status ConstructHandshakePb(const std::string &senderAddr, UrmaHandshakeReqPb &req,
                            const std::string &clientEntityId = "");

uint64_t GenerateReqId();
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_FAST_TRANSPORT_MANAGER_WRAPPER_H
