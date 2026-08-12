/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#include <gtest/gtest.h>

#include <list>
#include <optional>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "datasystem/common/object_cache/peer_ub_admission.h"
#include "datasystem/common/object_cache/provider_ub_failure_detail.h"
#include "datasystem/common/object_cache/urma_fallback_tcp_limiter.h"
#include "datasystem/common/rpc/brpc_status_util.h"
#define private public
#include "datasystem/worker/object_cache/service/worker_oc_service_get_impl.h"
#include "datasystem/worker/object_cache/worker_worker_oc_service_impl.h"
#undef private

namespace datasystem {
namespace object_cache {
namespace {
const HostPort LOCAL_WORKER("192.0.2.20", 18480);
const HostPort DATA_WORKER("192.0.2.21", 18481);
const HostPort CLIENT_WRITEBACK_ENDPOINT("192.0.2.22", 18481);
const HostPort REMOTE_GET_ENDPOINT("192.0.2.23", 18481);

WorkerOcServiceCrudParam BuildGetParam(WorkerRequestManager &requestManager, std::shared_ptr<ObjectTable> objectTable)
{
    return WorkerOcServiceCrudParam{
        .workerMasterApiManager = nullptr,
        .workerRequestManager = requestManager,
        .memoryRefTable = nullptr,
        .objectTable = std::move(objectTable),
        .evictionManager = nullptr,
        .workerDevOcManager = nullptr,
        .asyncPersistenceDelManager = nullptr,
        .asyncSendManager = nullptr,
        .metadataSize = 0,
        .persistenceApi = nullptr,
        .metadataRouteResolver = nullptr,
        .endpointPolicy = nullptr,
        .exitRequested = nullptr,
        .allowDirectoryLag = false,
    };
}
}  // namespace

TEST(WorkerOcServiceGetUbAdmissionTest, UnavailableDataWorkerReadSourceFailsFast)
{
    auto admission = std::make_shared<PeerUbAdmission>();
    UbOpOutcome outcome{ DATA_WORKER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                         Status(K_URMA_ERROR, "remote get writeback failed") };
    outcome.providerStatus = 4;
    admission->ReportOutcome(outcome);

    WorkerRequestManager requestManager;
    auto objectTable = std::make_shared<ObjectTable>();
    auto param = BuildGetParam(requestManager, objectTable);
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, LOCAL_WORKER, nullptr, admission);

    auto rc = getImpl.CheckRemoteReadAdmission(DATA_WORKER.ToString());

    ASSERT_TRUE(rc.IsError());
    EXPECT_EQ(rc.GetCode(), StatusCode::K_URMA_DATA_WORKER_UNAVAILABLE);
}

TEST(WorkerOcServiceGetUbAdmissionTest, EmptyBatchResponsePreservesRequestError)
{
    WorkerRequestManager requestManager;
    auto param = BuildGetParam(requestManager, std::make_shared<ObjectTable>());
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, LOCAL_WORKER, nullptr, nullptr);
    Status checkConnectStatus;
    std::list<WorkerOcServiceGetImpl::GetObjectInfo> infos;
    BatchGetObjectRemoteRspPb rsp;
    std::vector<RpcMessage> payloads;
    std::vector<std::string> successIds;
    std::vector<ReadKey> needRetryIds;
    std::unordered_set<std::string> failedIds;
    std::list<WorkerOcServiceGetImpl::GetObjectInfo> failedInfos;
    bool dataSizeChange = false;
    Status requestError(K_RPC_UNAVAILABLE, "batch request failed before per-object handling");

    auto rc = getImpl.ProcessBatchResponse(DATA_WORKER.ToString(), checkConnectStatus, infos, nullptr, requestError,
                                           rsp, payloads, successIds, needRetryIds, failedIds, failedInfos,
                                           dataSizeChange);

    EXPECT_EQ(rc, requestError);
}

TEST(WorkerOcServiceGetUbAdmissionTest, LegacyRemoteReadFailureDoesNotHardQuarantineDataWorker)
{
    auto admission = std::make_shared<PeerUbAdmission>();

    WorkerRequestManager requestManager;
    auto objectTable = std::make_shared<ObjectTable>();
    auto param = BuildGetParam(requestManager, objectTable);
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, LOCAL_WORKER, nullptr, admission);

    getImpl.ReportRemoteReadOutcome(DATA_WORKER.ToString(), Status(K_URMA_ERROR, "remote get failed"), "unit_test");

    auto rc = getImpl.CheckRemoteReadAdmission(DATA_WORKER.ToString());
    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
    EXPECT_FALSE(admission->GetState(DATA_WORKER).has_value());
}

TEST(WorkerOcServiceGetUbAdmissionTest, RemoteReadRpcTimeoutDoesNotHardQuarantineDataWorker)
{
    auto admission = std::make_shared<PeerUbAdmission>();

    WorkerRequestManager requestManager;
    auto objectTable = std::make_shared<ObjectTable>();
    auto param = BuildGetParam(requestManager, objectTable);
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, LOCAL_WORKER, nullptr, admission);

    getImpl.ReportRemoteReadOutcome(DATA_WORKER.ToString(), Status(K_RPC_DEADLINE_EXCEEDED, "remote get rpc timeout"),
                                    "rpc_timeout");

    auto rc = getImpl.CheckRemoteReadAdmission(DATA_WORKER.ToString());
    ASSERT_TRUE(rc.IsOk()) << rc.ToString();
    auto state = admission->GetState(DATA_WORKER);
    ASSERT_TRUE(state.has_value());
    EXPECT_EQ(state->state, UbAdmissionState::SUSPECT);
}

TEST(WorkerOcServiceGetUbAdmissionTest, RequesterConnectFailureDoesNotHardBlockWithoutProviderDetail)
{
    auto admission = std::make_shared<PeerUbAdmission>();
    WorkerRequestManager requestManager;
    auto param = BuildGetParam(requestManager, std::make_shared<ObjectTable>());
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, LOCAL_WORKER, nullptr, admission);

    getImpl.ReportRemoteReadOutcome(DATA_WORKER.ToString(), Status(K_URMA_CONNECT_FAILED, "requester connect failed"),
                                    "requester_connect");

    EXPECT_TRUE(getImpl.CheckRemoteReadAdmission(DATA_WORKER.ToString()).IsOk());
    EXPECT_FALSE(admission->GetState(DATA_WORKER).has_value());
}

TEST(WorkerOcServiceGetUbAdmissionTest, ClientGetResponseCarriesStructuredProviderUbFailure)
{
    GetRspPb rsp;
    auto *detail = rsp.mutable_provider_ub_failure_detail();
    detail->set_status_code(K_URMA_ERROR);
    detail->set_provider_status(4);
    detail->set_cqe_status(4);
    detail->set_has_provider_status(true);
    detail->set_has_cqe_status(true);
    detail->set_failed_endpoint(CLIENT_WRITEBACK_ENDPOINT.ToString());
    detail->set_failure_side("provider_local_ub_write");
    detail->set_operator_worker(DATA_WORKER.ToString());

    EXPECT_EQ(rsp.provider_ub_failure_detail().status_code(), K_URMA_ERROR);
    EXPECT_EQ(rsp.provider_ub_failure_detail().provider_status(), 4);
    EXPECT_EQ(rsp.provider_ub_failure_detail().cqe_status(), 4);
    EXPECT_TRUE(rsp.provider_ub_failure_detail().has_provider_status());
    EXPECT_TRUE(rsp.provider_ub_failure_detail().has_cqe_status());
    EXPECT_EQ(rsp.provider_ub_failure_detail().failed_endpoint(), CLIENT_WRITEBACK_ENDPOINT.ToString());
    EXPECT_EQ(rsp.provider_ub_failure_detail().failure_side(), "provider_local_ub_write");
    EXPECT_EQ(rsp.provider_ub_failure_detail().operator_worker(), DATA_WORKER.ToString());
}

TEST(WorkerOcServiceGetUbAdmissionTest, RemoteGetResponseCarriesStructuredProviderUbFailure)
{
    GetObjectRemoteRspPb rsp;
    auto *detail = rsp.mutable_provider_ub_failure_detail();
    detail->set_status_code(K_URMA_ERROR);
    detail->set_provider_status(4);
    detail->set_cqe_status(4);
    detail->set_has_provider_status(true);
    detail->set_has_cqe_status(true);
    detail->set_failed_endpoint(REMOTE_GET_ENDPOINT.ToString());
    detail->set_failure_side("provider_local_ub_write");
    detail->set_operator_worker(DATA_WORKER.ToString());

    EXPECT_EQ(rsp.provider_ub_failure_detail().status_code(), K_URMA_ERROR);
    EXPECT_EQ(rsp.provider_ub_failure_detail().provider_status(), 4);
    EXPECT_EQ(rsp.provider_ub_failure_detail().cqe_status(), 4);
    EXPECT_TRUE(rsp.provider_ub_failure_detail().has_provider_status());
    EXPECT_TRUE(rsp.provider_ub_failure_detail().has_cqe_status());
    EXPECT_EQ(rsp.provider_ub_failure_detail().failed_endpoint(), REMOTE_GET_ENDPOINT.ToString());
    EXPECT_EQ(rsp.provider_ub_failure_detail().failure_side(), "provider_local_ub_write");
    EXPECT_EQ(rsp.provider_ub_failure_detail().operator_worker(), DATA_WORKER.ToString());
}

TEST(WorkerOcServiceGetUbAdmissionTest, ProviderFailureHelperEncodesAvailableRawStatus)
{
    ProviderUbFailureDetailPb detail;

    FillProviderUbFailureDetail(Status(K_URMA_ERROR, "provider write failed"), REMOTE_GET_ENDPOINT.ToString(),
                                DATA_WORKER.ToString(), 4, 4, detail);

    EXPECT_EQ(detail.status_code(), K_URMA_ERROR);
    EXPECT_EQ(detail.message(), "provider write failed");
    EXPECT_TRUE(detail.has_provider_status());
    EXPECT_TRUE(detail.has_cqe_status());
    EXPECT_EQ(detail.provider_status(), 4);
    EXPECT_EQ(detail.cqe_status(), 4);
    EXPECT_EQ(detail.failed_endpoint(), REMOTE_GET_ENDPOINT.ToString());
    EXPECT_EQ(detail.failure_side(), PROVIDER_LOCAL_UB_WRITE_FAILURE_SIDE);
    EXPECT_EQ(detail.operator_worker(), DATA_WORKER.ToString());
}

TEST(WorkerOcServiceGetUbAdmissionTest, ProviderFailureDetailTracksFallbackLimiterWrappedStatus)
{
    const Status providerFailure(K_URMA_ERROR, "provider write failed");
    ProviderUbFailureDetailPb detail;
    FillProviderUbFailureDetail(providerFailure, REMOTE_GET_ENDPOINT.ToString(), DATA_WORKER.ToString(), 4, 4, detail);
    std::atomic<uint64_t> pendingBytes{ 0 };
    UrmaFallbackTcpLimiter::Ticket ticket;
    auto wrappedStatus =
        UrmaFallbackTcpLimiter::TryAcquire(pendingBytes, UrmaFallbackTcpLimiter::kMaxSinglePayloadBytes,
                                           providerFailure, "worker->client", ticket);

    ASSERT_TRUE(wrappedStatus.IsError());
    UpdateProviderUbFailureDetailForWrappedStatus(providerFailure, wrappedStatus, detail);

    EXPECT_EQ(detail.status_code(), wrappedStatus.GetCode());
    EXPECT_EQ(detail.message(), wrappedStatus.GetMsg());
    EXPECT_EQ(detail.failed_endpoint(), REMOTE_GET_ENDPOINT.ToString());
    EXPECT_EQ(detail.operator_worker(), DATA_WORKER.ToString());
    EXPECT_TRUE(detail.has_provider_status());
    EXPECT_TRUE(detail.has_cqe_status());
    EXPECT_EQ(detail.provider_status(), 4);
    EXPECT_EQ(detail.cqe_status(), 4);
}

TEST(WorkerOcServiceGetUbAdmissionTest, WrappedStatusDoesNotRewriteMismatchedProviderFailureDetail)
{
    const Status providerFailure(K_URMA_ERROR, "provider write failed");
    ProviderUbFailureDetailPb detail;
    FillProviderUbFailureDetail(Status(K_URMA_ERROR, "another provider failure"), REMOTE_GET_ENDPOINT.ToString(),
                                DATA_WORKER.ToString(), 4, 4, detail);
    const Status wrappedStatus(K_URMA_ERROR, "provider write failed, fallback rejected");

    UpdateProviderUbFailureDetailForWrappedStatus(providerFailure, wrappedStatus, detail);

    EXPECT_EQ(detail.message(), "another provider failure");
}

TEST(WorkerOcServiceGetUbAdmissionTest, DecoderRejectsLegacyOrUntrustedFailureDetail)
{
    ProviderUbFailureDetailPb empty;
    EXPECT_FALSE(DecodeProviderUbFailureDetail(empty, DATA_WORKER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                                               "legacy_response")
                     .has_value());

    ProviderUbFailureDetailPb untrusted;
    FillProviderUbFailureDetail(Status(K_URMA_ERROR, "provider write failed"), REMOTE_GET_ENDPOINT.ToString(),
                                DATA_WORKER.ToString(), 4, 4, untrusted);
    untrusted.set_failure_side("requester_local_failure");
    EXPECT_FALSE(DecodeProviderUbFailureDetail(untrusted, DATA_WORKER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                                               "untrusted_response")
                     .has_value());

    FillProviderUbFailureDetail(Status(K_URMA_ERROR, "provider write failed"), REMOTE_GET_ENDPOINT.ToString(),
                                LOCAL_WORKER.ToString(), 4, 4, untrusted);
    EXPECT_FALSE(DecodeProviderUbFailureDetail(untrusted, DATA_WORKER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                                               "mismatched_operator")
                     .has_value());
}

TEST(WorkerOcServiceGetUbAdmissionTest, ExplicitRemoteGetDetailMarksProviderUnavailable)
{
    auto admission = std::make_shared<PeerUbAdmission>();
    WorkerRequestManager requestManager;
    auto param = BuildGetParam(requestManager, std::make_shared<ObjectTable>());
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, LOCAL_WORKER, nullptr, admission);
    GetObjectRemoteRspPb rsp;
    FillProviderUbFailureDetail(Status(K_URMA_ERROR, "provider write failed"), LOCAL_WORKER.ToString(),
                                DATA_WORKER.ToString(), 4, 4, *rsp.mutable_provider_ub_failure_detail());

    getImpl.ReportRemoteReadOutcome(DATA_WORKER.ToString(), rsp, "remote_get_response");

    EXPECT_EQ(getImpl.CheckRemoteReadAdmission(DATA_WORKER.ToString()).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);
}

TEST(WorkerOcServiceGetUbAdmissionTest, SingleRemoteGetErrorResponseFormsObservationBeforeReturningFailure)
{
    auto admission = std::make_shared<PeerUbAdmission>();
    WorkerRequestManager requestManager;
    auto param = BuildGetParam(requestManager, std::make_shared<ObjectTable>());
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, LOCAL_WORKER, nullptr, admission);
    GetObjectRemoteRspPb rsp;
    const Status providerRc(K_URMA_ERROR, "provider write failed");
    FillProviderUbFailureDetail(providerRc, LOCAL_WORKER.ToString(), DATA_WORKER.ToString(), 5, 4,
                                *rsp.mutable_provider_ub_failure_detail());

    ASSERT_TRUE(WorkerWorkerOCServiceImpl::TryEncodeProviderUbFailureResponse(providerRc, rsp));
    auto rc = getImpl.ProcessRemoteReadResponse(DATA_WORKER.ToString(), rsp, "remote_get_response");

    EXPECT_EQ(rc.GetCode(), K_URMA_ERROR);
    EXPECT_EQ(rsp.error().error_code(), K_URMA_ERROR);
    EXPECT_EQ(rsp.error().error_msg(), providerRc.GetMsg());
    EXPECT_EQ(getImpl.CheckRemoteReadAdmission(DATA_WORKER.ToString()).GetCode(),
              K_URMA_DATA_WORKER_UNAVAILABLE);
}

TEST(WorkerOcServiceGetUbAdmissionTest, BatchRemoteGetUsesPerResponseFailureDetail)
{
    auto admission = std::make_shared<PeerUbAdmission>();
    WorkerRequestManager requestManager;
    auto param = BuildGetParam(requestManager, std::make_shared<ObjectTable>());
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, LOCAL_WORKER, nullptr, admission);
    BatchGetObjectRemoteRspPb rsp;
    rsp.add_responses();
    auto *failed = rsp.add_responses();
    FillProviderUbFailureDetail(Status(K_URMA_ERROR, "provider write failed"), LOCAL_WORKER.ToString(),
                                DATA_WORKER.ToString(), 4, 4, *failed->mutable_provider_ub_failure_detail());

    getImpl.ReportRemoteReadOutcome(DATA_WORKER.ToString(), rsp, "batch_remote_get_response");

    EXPECT_EQ(getImpl.CheckRemoteReadAdmission(DATA_WORKER.ToString()).GetCode(), K_URMA_DATA_WORKER_UNAVAILABLE);
}

TEST(WorkerOcServiceGetUbAdmissionTest, BatchTransportFailureMarksEveryCoveredResponse)
{
    BatchGetObjectRemoteRspPb rsp;
    rsp.add_responses();
    rsp.add_responses();
    rsp.add_responses();
    const Status failure(K_URMA_ERROR, "shared batch write failed");

    WorkerWorkerOCServiceImpl::SetBatchResponseError(rsp, 1, 2, failure);

    EXPECT_EQ(rsp.responses(0).error().error_code(), K_OK);
    EXPECT_EQ(rsp.responses(1).error().error_code(), K_URMA_ERROR);
    EXPECT_EQ(rsp.responses(1).error().error_msg(), failure.GetMsg());
    EXPECT_EQ(rsp.responses(2).error().error_code(), K_URMA_ERROR);
    EXPECT_EQ(rsp.responses(2).error().error_msg(), failure.GetMsg());
}

TEST(WorkerOcServiceGetUbAdmissionTest, ClientWritebackDetailDecodesAsClientGetOutcome)
{
    GetRspPb rsp;
    FillProviderUbFailureDetail(Status(K_URMA_ERROR, "provider write failed"), CLIENT_WRITEBACK_ENDPOINT.ToString(),
                                DATA_WORKER.ToString(), 4, 4, *rsp.mutable_provider_ub_failure_detail());

    auto outcome = DecodeProviderUbFailureDetail(rsp.provider_ub_failure_detail(), DATA_WORKER,
                                                 UbOperationKind::CLIENT_GET_WRITEBACK, "client_get_response");

    ASSERT_TRUE(outcome.has_value());
    EXPECT_EQ(outcome->peer, DATA_WORKER);
    EXPECT_EQ(outcome->op, UbOperationKind::CLIENT_GET_WRITEBACK);
    EXPECT_EQ(outcome->status.GetCode(), K_URMA_ERROR);
    ASSERT_TRUE(outcome->providerStatus.has_value());
    ASSERT_TRUE(outcome->cqeStatus.has_value());
    EXPECT_EQ(*outcome->providerStatus, 4);
    EXPECT_EQ(*outcome->cqeStatus, 4);
}

TEST(WorkerOcServiceGetUbAdmissionTest, GetRequestRecordsWorkerToClientProviderFailure)
{
    GetRequest request(AccessRecorderKey::DS_POSIX_GET, DATA_WORKER.ToString());
    request.ubUrmaInfo_.mutable_request_address()->set_host(CLIENT_WRITEBACK_ENDPOINT.Host());
    request.ubUrmaInfo_.mutable_request_address()->set_port(CLIENT_WRITEBACK_ENDPOINT.Port());
    GetRspPb rsp;
    UrmaWriteFailure failure{ .providerStatus = 4, .cqeStatus = 9 };

    request.RecordProviderUbWriteFailure(Status(K_URMA_ERROR, "client writeback failed"), rsp, &failure);

    ASSERT_TRUE(rsp.has_provider_ub_failure_detail());
    EXPECT_EQ(rsp.provider_ub_failure_detail().failed_endpoint(), CLIENT_WRITEBACK_ENDPOINT.ToString());
    EXPECT_EQ(rsp.provider_ub_failure_detail().operator_worker(), DATA_WORKER.ToString());
    EXPECT_TRUE(rsp.provider_ub_failure_detail().has_provider_status());
    EXPECT_TRUE(rsp.provider_ub_failure_detail().has_cqe_status());
    EXPECT_EQ(rsp.provider_ub_failure_detail().provider_status(), 4);
    EXPECT_EQ(rsp.provider_ub_failure_detail().cqe_status(), 9);
}

TEST(WorkerOcServiceGetUbAdmissionTest, ClientWritebackDetailUsesClientIdentityWhenUrmaAddressIsEmpty)
{
    GetRequest request(AccessRecorderKey::DS_POSIX_GET, DATA_WORKER.ToString());
    request.clientId_ = ClientKey::Intern("client-without-urma-host-port");
    GetRspPb rsp;
    UrmaWriteFailure failure{ .providerStatus = 4, .cqeStatus = 4 };

    request.RecordProviderUbWriteFailure(Status(K_URMA_ERROR, "client writeback failed"), rsp, &failure);

    ASSERT_TRUE(rsp.has_provider_ub_failure_detail());
    EXPECT_EQ(rsp.provider_ub_failure_detail().failed_endpoint(), "client_id=client-without-urma-host-port");
    auto outcome = DecodeProviderUbFailureDetail(rsp.provider_ub_failure_detail(), DATA_WORKER,
                                                 UbOperationKind::CLIENT_GET_WRITEBACK, "client_get_response");
    ASSERT_TRUE(outcome.has_value());
    ASSERT_TRUE(outcome->cqeStatus.has_value());
    EXPECT_EQ(*outcome->cqeStatus, 4);
}

TEST(WorkerOcServiceGetUbAdmissionTest, GetRequestAttachesOnlyMatchingObjectProviderFailure)
{
    GetRequest request(AccessRecorderKey::DS_POSIX_GET, DATA_WORKER.ToString());
    request.rawObjectKeys_ = { "first", "second" };
    request.objects_.emplace("first", GetObjInfo{});
    request.objects_.emplace("second", GetObjInfo{});
    ProviderUbFailureDetailPb first;
    FillProviderUbFailureDetail(Status(K_URMA_ERROR, "first provider failure"), CLIENT_WRITEBACK_ENDPOINT.ToString(),
                                DATA_WORKER.ToString(), std::nullopt, 4, first);
    ProviderUbFailureDetailPb second;
    FillProviderUbFailureDetail(Status(K_URMA_CONNECT_FAILED, "second provider failure"),
                                REMOTE_GET_ENDPOINT.ToString(), DATA_WORKER.ToString(), 5, 5, second);

    request.RecordRemoteProviderUbFailure("first", Status(K_URMA_ERROR, "first provider failure"), first);
    request.RecordRemoteProviderUbFailure("second", Status(K_URMA_CONNECT_FAILED, "second provider failure"), second);

    GetRspPb rsp;
    request.AttachRemoteProviderUbFailure(Status(K_URMA_CONNECT_FAILED, "second provider failure"), rsp);

    ASSERT_TRUE(rsp.has_provider_ub_failure_detail());
    EXPECT_EQ(rsp.provider_ub_failure_detail().failed_endpoint(), REMOTE_GET_ENDPOINT.ToString());
    GetRspPb unrelatedRsp;
    request.AttachRemoteProviderUbFailure(Status(K_NOT_FOUND, "unrelated failure"), unrelatedRsp);
    EXPECT_FALSE(unrelatedRsp.has_provider_ub_failure_detail());
}

TEST(WorkerOcServiceGetUbAdmissionTest, ClientWritebackFailureQuarantinesProviderSelfAdmission)
{
    auto admission = std::make_shared<PeerUbAdmission>();
    GetRequest request(AccessRecorderKey::DS_POSIX_GET, DATA_WORKER.ToString(), admission);
    request.ubUrmaInfo_.mutable_request_address()->set_host(CLIENT_WRITEBACK_ENDPOINT.Host());
    request.ubUrmaInfo_.mutable_request_address()->set_port(CLIENT_WRITEBACK_ENDPOINT.Port());
    GetRspPb rsp;
    UrmaWriteFailure failure{ .providerStatus = 4, .cqeStatus = 4 };

    request.RecordProviderUbWriteFailure(Status(K_URMA_ERROR, "client writeback failed"), rsp, &failure);

    auto self = admission->GetState(DATA_WORKER);
    ASSERT_TRUE(self.has_value());
    EXPECT_EQ(self->state, UbAdmissionState::UNAVAILABLE);
    EXPECT_EQ(self->lastFailureClass, UbFailureClass::PORT_UNAVAILABLE_ERROR4);
}

TEST(WorkerOcServiceGetUbAdmissionTest, RemoteGetProviderRecordsWorkerToWorkerFailure)
{
    GetObjectRemoteReqPb req;
    req.mutable_urma_info()->mutable_request_address()->set_host(REMOTE_GET_ENDPOINT.Host());
    req.mutable_urma_info()->mutable_request_address()->set_port(REMOTE_GET_ENDPOINT.Port());
    GetObjectRemoteRspPb rsp;
    UrmaWriteFailure failure{ .providerStatus = 4, .cqeStatus = 9 };

    WorkerWorkerOCServiceImpl::RecordProviderUbWriteFailure(req, Status(K_URMA_ERROR, "remote get writeback failed"),
                                                            DATA_WORKER, rsp, &failure);

    ASSERT_TRUE(rsp.has_provider_ub_failure_detail());
    EXPECT_EQ(rsp.provider_ub_failure_detail().failed_endpoint(), REMOTE_GET_ENDPOINT.ToString());
    EXPECT_EQ(rsp.provider_ub_failure_detail().operator_worker(), DATA_WORKER.ToString());
    EXPECT_TRUE(rsp.provider_ub_failure_detail().has_provider_status());
    EXPECT_TRUE(rsp.provider_ub_failure_detail().has_cqe_status());
    EXPECT_EQ(rsp.provider_ub_failure_detail().provider_status(), 4);
    EXPECT_EQ(rsp.provider_ub_failure_detail().cqe_status(), 9);
}

TEST(WorkerOcServiceGetUbAdmissionTest, RemoteGetProviderUsesClientIdWhenRequesterAddressIsEmpty)
{
    constexpr char CLIENT_ID[] = "test-client-id";
    GetObjectRemoteReqPb req;
    req.mutable_urma_info()->set_client_id(CLIENT_ID);
    GetObjectRemoteRspPb rsp;
    UrmaWriteFailure failure{ .cqeStatus = 4 };

    WorkerWorkerOCServiceImpl::RecordProviderUbWriteFailure(req, Status(K_URMA_ERROR, "client writeback failed"),
                                                            DATA_WORKER, rsp, &failure);

    ASSERT_TRUE(rsp.has_provider_ub_failure_detail());
    EXPECT_EQ(rsp.provider_ub_failure_detail().failed_endpoint(), std::string("client_id=") + CLIENT_ID);
    EXPECT_EQ(rsp.provider_ub_failure_detail().operator_worker(), DATA_WORKER.ToString());
    EXPECT_TRUE(rsp.provider_ub_failure_detail().has_cqe_status());
    EXPECT_EQ(rsp.provider_ub_failure_detail().cqe_status(), 4);
}

TEST(WorkerOcServiceGetUbAdmissionTest, RemoteGetWritebackFailureQuarantinesProviderSelfAdmission)
{
    auto admission = std::make_shared<PeerUbAdmission>();
    GetObjectRemoteReqPb req;
    req.mutable_urma_info()->mutable_request_address()->set_host(REMOTE_GET_ENDPOINT.Host());
    req.mutable_urma_info()->mutable_request_address()->set_port(REMOTE_GET_ENDPOINT.Port());
    GetObjectRemoteRspPb rsp;
    UrmaWriteFailure failure{ .providerStatus = 4, .cqeStatus = 4 };

    WorkerWorkerOCServiceImpl::RecordProviderUbWriteFailure(req, Status(K_URMA_ERROR, "writeback failed"), DATA_WORKER,
                                                            rsp, &failure, admission.get());

    auto self = admission->GetState(DATA_WORKER);
    ASSERT_TRUE(self.has_value());
    EXPECT_EQ(self->state, UbAdmissionState::UNAVAILABLE);
    EXPECT_EQ(self->lastFailureClass, UbFailureClass::PORT_UNAVAILABLE_ERROR4);
}

TEST(WorkerOcServiceGetUbAdmissionTest, NotifyRemoteGetPropagatesNewSourceProviderError4)
{
    auto admission = std::make_shared<PeerUbAdmission>();
    WorkerRequestManager requestManager;
    auto param = BuildGetParam(requestManager, std::make_shared<ObjectTable>());
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, LOCAL_WORKER, nullptr, admission);
    std::unordered_map<std::string, uint64_t> epochsBefore{ { DATA_WORKER.ToString(), 0 } };
    UbOpOutcome failure(DATA_WORKER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                        Status(K_URMA_ERROR, "source provider CQE status 4"));
    failure.providerStatus = 4;
    failure.cqeStatus = 4;
    admission->ReportOutcome(failure);
    NotifyRemoteGetRspPb rsp;

    getImpl.AttachNotifyRemoteGetUbFailure(epochsBefore, rsp);

    ASSERT_TRUE(rsp.has_provider_ub_failure_detail());
    EXPECT_EQ(rsp.provider_ub_failure_detail().failed_endpoint(), LOCAL_WORKER.ToString());
    EXPECT_EQ(rsp.provider_ub_failure_detail().operator_worker(), DATA_WORKER.ToString());
    EXPECT_EQ(rsp.provider_ub_failure_detail().provider_status(), 4);
    EXPECT_EQ(rsp.provider_ub_failure_detail().cqe_status(), 4);
}

TEST(WorkerOcServiceGetUbAdmissionTest, NotifyRemoteGetPreservesDirectProviderError4)
{
    auto admission = std::make_shared<PeerUbAdmission>();
    WorkerRequestManager requestManager;
    auto param = BuildGetParam(requestManager, std::make_shared<ObjectTable>());
    WorkerOcServiceGetImpl getImpl(param, nullptr, nullptr, nullptr, nullptr, LOCAL_WORKER, nullptr, admission);
    UbOpOutcome failure(DATA_WORKER, UbOperationKind::WORKER_REMOTE_GET_WRITEBACK,
                        Status(K_URMA_ERROR, "inferred provider failure"));
    failure.cqeStatus = 4;
    admission->ReportOutcome(failure);

    ProviderUbFailureDetailPb directDetail;
    FillProviderUbFailureDetail(Status(K_URMA_ERROR, "direct provider failure"), LOCAL_WORKER.ToString(),
                                DATA_WORKER.ToString(), std::nullopt, 4, directDetail);
    std::vector<std::optional<ProviderUbFailureDetailPb>> failureDetails{ std::nullopt, directDetail };
    NotifyRemoteGetRspPb rsp;
    getImpl.CopyFirstNotifyRemoteGetUbFailure(failureDetails, rsp);
    getImpl.AttachNotifyRemoteGetUbFailure({ { DATA_WORKER.ToString(), 0 } }, rsp);

    ASSERT_TRUE(rsp.has_provider_ub_failure_detail());
    EXPECT_EQ(rsp.provider_ub_failure_detail().message(), "direct provider failure");
    EXPECT_EQ(rsp.provider_ub_failure_detail().operator_worker(), DATA_WORKER.ToString());
    EXPECT_EQ(rsp.provider_ub_failure_detail().cqe_status(), 4);
}

TEST(WorkerOcServiceGetUbAdmissionTest, LaterSuccessDoesNotClearBatchError)
{
    Status lastError;
    WorkerOcServiceGetImpl::UpdateLastBatchError(Status(K_URMA_ERROR, "first object failed"), lastError);
    WorkerOcServiceGetImpl::UpdateLastBatchError(Status::OK(), lastError);

    EXPECT_EQ(lastError.GetCode(), K_URMA_ERROR);
    EXPECT_EQ(lastError.GetMsg(), "first object failed");
}

TEST(WorkerOcServiceGetUbAdmissionTest, DelayReleaseOnlyForAmbiguousRemoteWrites)
{
    auto notSent = TryExtractStatusFromControllerError("[E111]Connection refused", ECONNREFUSED);
    EXPECT_FALSE(WorkerOcServiceGetImpl::NeedDelayReleaseRemoteGetShm(Status::OK()));
    EXPECT_FALSE(WorkerOcServiceGetImpl::NeedDelayReleaseRemoteGetShm(Status(K_NOT_FOUND, "not found")));
    EXPECT_TRUE(IsBrpcRequestDefinitelyNotSent(notSent));
    EXPECT_FALSE(WorkerOcServiceGetImpl::NeedDelayReleaseRemoteGetShm(notSent));
    EXPECT_TRUE(WorkerOcServiceGetImpl::NeedDelayReleaseRemoteGetShm(
        Status(K_RPC_DEADLINE_EXCEEDED, "response timeout")));
    EXPECT_TRUE(WorkerOcServiceGetImpl::NeedDelayReleaseRemoteGetShm(Status(K_URMA_ERROR, "write failed")));
}

TEST(WorkerOcServiceGetUbAdmissionTest, RemoteGetRetryCodesDependOnTransport)
{
    const auto &fastTransportRetryCodes = WorkerOcServiceGetImpl::GetRemoteGetRetryCodes(true);
    EXPECT_EQ(fastTransportRetryCodes.count(K_TRY_AGAIN), 1);
    EXPECT_EQ(fastTransportRetryCodes.count(K_URMA_CONNECT_FAILED), 1);
    EXPECT_EQ(fastTransportRetryCodes.count(K_RPC_DEADLINE_EXCEEDED), 0);

    const auto &rpcPayloadRetryCodes = WorkerOcServiceGetImpl::GetRemoteGetRetryCodes(false);
    EXPECT_EQ(rpcPayloadRetryCodes.count(K_TRY_AGAIN), 1);
    EXPECT_EQ(rpcPayloadRetryCodes.count(K_URMA_CONNECT_FAILED), 1);
    EXPECT_EQ(rpcPayloadRetryCodes.count(K_RPC_DEADLINE_EXCEEDED), 1);
    EXPECT_EQ(rpcPayloadRetryCodes.count(K_RPC_UNAVAILABLE), 1);
}
}  // namespace object_cache
}  // namespace datasystem
