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

#include "st_oc_service_impl.h"

#include <memory>

#include "datasystem/master/object_cache/oc_metadata_manager.h"
#include "datasystem/master/replica_manager.h"
#include "datasystem/worker/authenticate.h"

namespace datasystem {
namespace st {
Status StOCServiceImpl::GetMasterGRefTable(const GRefTableReqPb &req, GRefTableRspPb &rsp)
{
    (void)req;
    std::unordered_map<std::string, std::vector<std::string>> refTable;
    auto dbName = replicaManager_->GetCurrentWorkerUuid();
    std::shared_ptr<master::OCMetadataManager> ocMetadataManager;
    Status rc = replicaManager_->GetOcMetadataManager(dbName, ocMetadataManager);
    RETURN_OK_IF_TRUE(rc.IsError());
    ocMetadataManager->GetGlobalRefTable()->GetAllClientRef(refTable);
    for (const auto &singleWorker : refTable) {
        auto singleWorkerGRef = rsp.add_single_client_gref();
        singleWorkerGRef->set_hostport(singleWorker.first);
        *singleWorkerGRef->mutable_object_keys() = { singleWorker.second.begin(), singleWorker.second.end() };
    }
    return Status::OK();
}

Status StOCServiceImpl::GetWorkerGRefTable(const GRefTableReqPb &req, GRefTableRspPb &rsp)
{
    (void)req;
    std::unordered_map<std::string, std::vector<std::string>> refTable;
    workerOc_->GetGlobalRefTable()->GetAllClientRef(refTable);
    for (const auto &singleWorker : refTable) {
        auto singleWorkerGRef = rsp.add_single_client_gref();
        singleWorkerGRef->set_hostport(singleWorker.first);
        *singleWorkerGRef->mutable_object_keys() = { singleWorker.second.begin(), singleWorker.second.end() };
    }
    return Status::OK();
}

Status StOCServiceImpl::GetCmNodeTable(const CmNodeTableReqPb &req, CmNodeTableRspPb &rsp)
{
    (void)req;
    const int ZERO = 0;
    const int ONE = 1;
    const int TWO = 2;
    const int THREE = 3;
    const int NUM_TOKENS = 4;

    const auto &tableStr = etcdCM_->ClusterNodeTableToString();
    for (const std::string &nodeStr : tableStr) {
        std::vector<std::string> tokens;
        std::stringstream ss(nodeStr);
        for (std::string token; std::getline(ss, token, ';');) {
            tokens.emplace_back(token);
        }
        CHECK_FAIL_RETURN_STATUS(tokens.size() == NUM_TOKENS, StatusCode::K_INVALID,
                                 "String should be of format \"hostport:timeEpoch:stateStr:additionEventType\"");
        auto nodePb = rsp.add_cm_node_table();
        nodePb->set_hostport(tokens[ZERO]);
        nodePb->set_time_epoch(tokens[ONE]);
        nodePb->set_state(tokens[TWO]);
        nodePb->set_addition_event_type(tokens[THREE]);
    }
    return Status::OK();
}

Status StOCServiceImpl::TestUnaryCompatibilityV1(const TestReqPbV1 &req, TestRspPbV1 &rsp)
{
    std::string tenantId;
    (void)rsp;
    const uint32_t ONE = 1;
    if (req.auth_req() == 0) {
        return worker::Authenticate(akSkManager_, req, tenantId);
    } else if (req.auth_req() == ONE) {
        return worker::AuthenticateRequest(akSkManager_, req, req.tenant_id(), tenantId);
    } else {
        return akSkManager_->VerifySignatureAndTimestamp(req);
    }
}

Status StOCServiceImpl::TestStreamCompatibilityV1(
    std::shared_ptr<::datasystem::ServerWriterReader<TestRspPbV1, TestReqPbV1>> stream)
{
    TestReqPbV1 req;
    TestRspPbV1 rsp;
    RETURN_IF_NOT_OK(stream->Read(req));
    std::string tenantId;
    const uint32_t ONE = 1;
    if (req.auth_req() == 0) {
        RETURN_IF_NOT_OK(worker::Authenticate(akSkManager_, req, tenantId));
    } else if (req.auth_req() == ONE) {
        RETURN_IF_NOT_OK(worker::AuthenticateRequest(akSkManager_, req, req.tenant_id(), tenantId));
    } else {
        RETURN_IF_NOT_OK(akSkManager_->VerifySignatureAndTimestamp(req));
    }
    RETURN_IF_NOT_OK(stream->Write(rsp));
    return stream->Finish();
}

Status StOCServiceImpl::TestStreamCompatibility1V1(std::shared_ptr<::datasystem::ServerReader<TestReqPbV1>> stream,
                                                   TestRspPbV1 &rsp)
{
    TestReqPbV1 req;
    (void)rsp;
    RETURN_IF_NOT_OK(stream->Read(req));
    std::string tenantId;
    const uint32_t ONE = 1;
    if (req.auth_req() == 0) {
        RETURN_IF_NOT_OK(worker::Authenticate(akSkManager_, req, tenantId));
    } else if (req.auth_req() == ONE) {
        RETURN_IF_NOT_OK(worker::AuthenticateRequest(akSkManager_, req, req.tenant_id(), tenantId));
    } else {
        RETURN_IF_NOT_OK(akSkManager_->VerifySignatureAndTimestamp(req));
    }
    return Status::OK();
}

Status StOCServiceImpl::TestUnaryCompatibilityV2(const TestReqPbV2 &req, TestRspPbV1 &rsp)
{
    std::string tenantId;
    (void)rsp;
    TestReqPbV1 v1Req;
    v1Req.ParseFromString(req.SerializeAsString());
    TestRspPbV1 v1Rsp;
    return TestUnaryCompatibilityV1(v1Req, v1Rsp);
}

Status StOCServiceImpl::TestStreamCompatibilityV2(
    std::shared_ptr<::datasystem::ServerWriterReader<TestRspPbV1, TestReqPbV2>> stream)
{
    auto streamV2 = std::reinterpret_pointer_cast<ServerWriterReader<TestRspPbV1, TestReqPbV1>>(stream);
    return TestStreamCompatibilityV1(streamV2);
}

Status StOCServiceImpl::TestStreamCompatibility1V2(std::shared_ptr<::datasystem::ServerReader<TestReqPbV2>> stream,
                                                   TestRspPbV1 &rsp)
{
    auto streamV2 = std::reinterpret_pointer_cast<ServerReader<TestReqPbV1>>(stream);
    (void)rsp;
    TestRspPbV1 v1Rsp;
    return TestStreamCompatibility1V1(streamV2, v1Rsp);
}
}  // namespace st
}  // namespace datasystem