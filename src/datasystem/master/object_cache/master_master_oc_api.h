/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Module responsible for master master oc api.
 */

#ifndef MASTER_MASTER_OC_API_H
#define MASTER_MASTER_OC_API_H

#include <memory>
#include <shared_mutex>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/rpc/rpc_constants.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/master_object.stub.rpc.pb.h"
#include "datasystem/worker/object_cache/worker_worker_oc_api.h"

namespace datasystem {
namespace master {

class MasterMasterOCApi {
public:
    /**
     * @brief Constructor for the remote version of the api
     * @param[in] hostPort The host port of the target master
     * @param[in] localHostPort The local worker rpc service host port.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     */
    MasterMasterOCApi(const HostPort &hostPort, const HostPort &localHostPort,
                      std::shared_ptr<AkSkManager> akSkManager);

    ~MasterMasterOCApi() = default;

    /**
     * @brief Initialization.
     * @return Status of the call.
     */
    Status Init();

    /**
     * @brief Migrate the metadata
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc request protobuf.
     */
    Status MigrateMetadata(MigrateMetadataReqPb &req, MigrateMetadataRspPb &rsp);

    /**
     * @brief GIncrease master remoteClientId ref
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc request protobuf.
     */
    Status GIncreaseMasterAppRef(const GIncreaseReqPb &req, GIncreaseRspPb &resp);

    /**
     * @brief Release master object ref of remoteClientId
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc request protobuf.
     */
    Status ReleaseGRefsOfRemoteClientId(const ReleaseGRefsReqPb &req, ReleaseGRefsRspPb &resp);

    /**
     * @brief Remove object meta in cache and rocksdb.
     * @param[in] request Req of call.
     * @param[out] response Rsp of call.
     * @return Status of the call.
     */
    Status RemoveMeta(const RemoveMetaReqPb &req, RemoveMetaRspPb &rsp);

    /**
     * @brief A factory method to instantiate the correct derived version of the api. Remote masters will use an
     * rpc-based api, whereas local masters can be optimized for in-process pointer based api.
     * @param[in] hostPort The host port of the target master
     * @param[in] localHostPort The local master host port.
     * @param[in] akSkManager Used to do AK/SK authenticate.
     * @return A base class pointer to the correct derived type of api.
     */
    static std::shared_ptr<MasterMasterOCApi> CreateMasterMasterOCApi(const HostPort &hostPort,
                                                                      const HostPort &localHostPort,
                                                                      std::shared_ptr<AkSkManager> akSkManager)
    {
        auto api = std::make_shared<MasterMasterOCApi>(hostPort, localHostPort, akSkManager);
        auto rc = api->Init();
        if (rc.IsError()) {
            LOG(ERROR) << FormatString("Init MasterMasterOCApi[%s to %s] failed: %s", localHostPort.ToString(),
                                       hostPort.ToString(), rc.ToString());
            return nullptr;
        }
        return api;
    }

    /**
     * @brief Send request to master to delete meta and get response.
     *        Master will notify other workers to delete these objects asynchronously.
     * @param[in] request The rpc request protobuf.
     * @param[out] response The rpc response protobuf.
     * @return Status of the call.
     */
    Status DeleteAllCopyMeta(DeleteAllCopyMetaReqPb &request, DeleteAllCopyMetaRspPb &response);

private:
    HostPort destHostPort_;   // The HostPort of the destination node
    HostPort localHostPort_;  // The HostPort of the local node
    std::shared_ptr<AkSkManager> akSkManager_;
    std::shared_ptr<master::MasterOCService_Stub> rpcSession_{ nullptr };  // session to the master rpc service
};
}  // namespace master
}  // namespace datasystem
#endif  // MASTER_MASTER_OC_API_H