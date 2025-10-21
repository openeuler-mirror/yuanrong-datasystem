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
 * Description: Defines the service to process requests for unit test
 */
#ifndef DATASYSTEM_ST_OBJECT_CACHE_ST_SERVICE_IMPL_H
#define DATASYSTEM_ST_OBJECT_CACHE_ST_SERVICE_IMPL_H

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/master/replica_manager.h"
#include "datasystem/protos/ut_object.service.rpc.pb.h"
#include "datasystem/worker/object_cache/worker_oc_service_impl.h"

namespace datasystem {
namespace st {
class StOCServiceImpl : public UtOCService {
public:
    explicit StOCServiceImpl(object_cache::WorkerOCServiceImpl *workerOc, EtcdClusterManager *etcdCM,
                             ReplicaManager *replicaManager, std::shared_ptr<AkSkManager> akSkManager)
        : workerOc_(workerOc), etcdCM_(etcdCM), replicaManager_(replicaManager), akSkManager_(std::move(akSkManager))
    {
    }

    /**
     * @brief Get a copy of global reference table in master (only used in test cases).
     * @param[in] req The rpc req protobuf.
     * @param[out] rsp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status GetMasterGRefTable(const GRefTableReqPb &req, GRefTableRspPb &rsp);

    /**
     * @brief Get a copy of global reference table in worker (only used in test cases).
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status GetWorkerGRefTable(const GRefTableReqPb &req, GRefTableRspPb &rsp);

    /**
     * @brief Get the node table in cluster manager.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status GetCmNodeTable(const CmNodeTableReqPb &req, CmNodeTableRspPb &rsp);

    Status TestUnaryCompatibilityV1(const TestReqPbV1 &, TestRspPbV1 &);
    Status TestStreamCompatibilityV1(std::shared_ptr<::datasystem::ServerWriterReader<TestRspPbV1, TestReqPbV1>>);
    Status TestStreamCompatibility1V1(std::shared_ptr<::datasystem::ServerReader<TestReqPbV1>>, TestRspPbV1 &);
    Status TestUnaryCompatibilityV2(const TestReqPbV2 &, TestRspPbV1 &);
    Status TestStreamCompatibilityV2(std::shared_ptr<::datasystem::ServerWriterReader<TestRspPbV1, TestReqPbV2>>);
    Status TestStreamCompatibility1V2(std::shared_ptr<::datasystem::ServerReader<TestReqPbV2>>, TestRspPbV1 &);

private:
    object_cache::WorkerOCServiceImpl *workerOc_;
    EtcdClusterManager *etcdCM_;
    ReplicaManager *replicaManager_;
    std::shared_ptr<AkSkManager> akSkManager_;
};
}  // namespace st
}  // namespace datasystem
#endif  // DATASYSTEM_ST_OBJECT_CACHE_ST_SERVICE_IMPL_H