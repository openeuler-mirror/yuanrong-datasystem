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
 * Description: Defines the worker client class to communicate with the worker service.
 */
#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_CLIENT_WORKER_BASE_API_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_CLIENT_WORKER_BASE_API_H

#include <cstdint>
#include <memory>
#include <unordered_map>

#include "datasystem/client/object_cache/client_worker_api/iclient_worker_api.h"
#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/string_intern/string_ref.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/hetero/device_common.h"
#include "datasystem/object/buffer.h"
#include "datasystem/protos/p2p_subscribe.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {
namespace object_cache {
class ClientWorkerBaseApi : public IClientWorkerApi {
public:
    explicit ClientWorkerBaseApi(HostPort hostPort, HeartbeatType heartbeatType, bool enableCrossNodeConnection,
                                 Signature *signature)
        : IClientWorkerApi(std::move(hostPort), heartbeatType, enableCrossNodeConnection, signature)
    {
    }

    ~ClientWorkerBaseApi() = default;

    bool EnableDecreaseShmRefByShmQueue()
    {
        return IsShmEnable() && decShmUnit_->fd > 0;
    }

protected:
    /**
     * @brief Prepare the put request.
     * @param[in] bufferInfo Buffer information.
     * @param[in] isSeal Is seal or not.
     * @param[in] nestedKeys Nested keys.
     * @param[in] ttlSecond Used by state api, means how many seconds the key will be delete automatically.
     * @param[in] existence Used by state api, to determine whether to set or not set the key if it does already
     * exist.
     * @param[out] req The protobuf req.
     * @return K_OK on success; the error code otherwise.
     */
    Status PreparePublishReq(const std::shared_ptr<ObjectBufferInfo> &bufferInfo, bool isSeal,
                             const std::unordered_set<std::string> &nestedKeys, uint32_t ttlSecond, int existence,
                             PublishReqPb &req);

    Status PreGet(const GetParam &getParam, int64_t subTimeoutMs, GetReqPb &req);

    void ParseGlbRefPb(QueryGlobalRefNumRspCollectionPb &rsp,
                       std::unordered_map<std::string, std::vector<std::unordered_set<std::string>>> &gRefMap);

    /**
     * @brief Fill device object meta to Pb.
     * @param[in] bufferInfo The info of device buffer.
     * @param[in] blobs The list of blob info.
     * @param[out] metaPb The device object meta pb.
     */
    void FillDevObjMeta(const std::shared_ptr<DeviceBufferInfo> &bufferInfo, const std::vector<Blob> &blobs,
                        DeviceObjectMetaPb *metaPb);

    bool CheckUseTransferForMultiCreateRsp(const MultiCreateRspPb &rsp, bool skipCheckExistence) const;
    void FillCreateParamsFromMultiCreateRsp(const MultiCreateRspPb &rsp, bool skipCheckExistence,
                                            std::vector<MultiCreateParam> &createParams,
                                            const std::vector<bool> &exists);
    void PostMultiCreate(bool skipCheckExistence, const MultiCreateRspPb &rsp,
                         std::vector<MultiCreateParam> &createParams, bool &useShmTransfer, PerfPoint &point,
                         uint32_t &version, std::vector<bool> &exists);

    Status SendBufferViaUb(const std::shared_ptr<ObjectBufferInfo> &bufferInfo, const void *data,
                           uint64_t length) override;

#ifdef USE_URMA
    Status PipelineDataTransferHelper(const std::shared_ptr<ObjectBufferInfo> &bufferInfo, const void *data,
                                      uint64_t totalSize, std::shared_ptr<UrmaManager::BufferHandle> &bufHandle,
                                      uint64_t realSize);

    void PrepareUrmaBuffer(GetReqPb &req, std::shared_ptr<UrmaManager::BufferHandle> &ubBufferHandle,
                           uint8_t *&ubBufferPtr, uint64_t &ubBufferSize, uint64_t requiredSize);
    Status FillUrmaBuffer(std::shared_ptr<UrmaManager::BufferHandle> &ubBufferHandle, GetRspPb &rsp,
                          std::vector<RpcMessage> &payloads, uint8_t *ubBufferPtr, uint64_t ubBufferSize);
#endif
};
}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_CLIENT_WORKER_API_H
