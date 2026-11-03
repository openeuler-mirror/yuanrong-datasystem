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
 * Description: The authenticate implement.
 */

#include <sstream>

#include "datasystem/worker/authenticate.h"

DS_DEFINE_bool(skip_authenticate, false, "hack to skip authenticate");


namespace datasystem {
namespace worker {
namespace {
enum class RpcAuthPolicy { UNSET, NONE, DIRECT, SYSTEM, TENANT, HEALTH, HEARTBEAT };
struct PolicySpec {
    const char *service;
    RpcAuthPolicy policy;
    const char *methods;
};

const PolicySpec POLICY_SPECS[] = {
    { "WorkerService", RpcAuthPolicy::TENANT,
      "GetClientFd GetSocketPath RegisterClient DisconnectClient" },
    { "WorkerService", RpcAuthPolicy::HEARTBEAT,
      "Heartbeat" },
    { "MasterSCService", RpcAuthPolicy::DIRECT,
      "CreateProducer CloseProducer Subscribe CloseConsumer DeleteStream QueryGlobalProducersNum "
      "QueryGlobalConsumersNum MigrateSCMetadata" },
    { "MasterOCService", RpcAuthPolicy::DIRECT,
      "CreateMeta CreateCopyMeta CreateMultiCopyMeta QueryMeta RemoveMeta DeleteAllCopyMeta UpdateMeta "
      "GetObjectLocations GIncreaseRef GDecreaseRef QueryGlobalRefNum PushMetaToMaster GIncNestedRef "
      "GDecNestedRef ReconcileMembershipChange MigrateMetadata GIncreaseMasterAppRef ReleaseGRefs "
      "ReleaseGRefsOfRemoteClientId CreateMultiMeta ReportResource ReportRebalanceResult "
      "StartEvictionPolicyUpdate GetEvictionPolicyUpdateProgress LivenessCheck PutP2PMeta "
      "SubscribeReceiveEvent GetP2PMeta SendRootInfo RecvRootInfo GetDataInfo AckRecvFinish "
      "RemoveP2PLocation ReleaseMetaData ReplacePrimary PureQueryMeta RollbackMultiMeta Expire "
      "GetMetaInfo CheckObjectDataLocation BatchExists" },
    { "MasterOCService", RpcAuthPolicy::NONE,
      "IncrSeqNoMeta GetSeqNoMeta DelSeqNoMeta" },
    { "ClientWorkerSCService", RpcAuthPolicy::TENANT,
      "CreateProducer CloseProducer Subscribe CloseConsumer GetDataPage CreateShmPage DeleteStream "
      "QueryGlobalProducersNum QueryGlobalConsumersNum GetLastAppendCursor ResetStreams ResumeStreams "
      "AllocBigShmMemory ReleaseBigShmMemory" },
    { "ClientWorkerSCService", RpcAuthPolicy::DIRECT,
      "UnblockProducer BlockProducer" },
    { "MasterWorkerSCService", RpcAuthPolicy::DIRECT,
      "UpdateTopoNotification DelStreamContext SyncPubNode SyncConsumerNode ClearAllRemotePub "
      "QueryMetadata" },
    { "MasterWorkerSCService", RpcAuthPolicy::NONE,
      "ClearAllRemoteConsumer" },
    { "WorkerWorkerSCService", RpcAuthPolicy::DIRECT,
      "PushElementsCursors PushSharedPageCursors" },
    { "PerfService", RpcAuthPolicy::SYSTEM,
      "GetPerfLog ResetPerfLog" },
    { "MasterWorkerOCService", RpcAuthPolicy::DIRECT,
      "PublishMeta UpdateNotification DeleteNotification DeletePersistenceObject "
      "QueryGlobalRefNumOnWorker PushMetaToWorker RequestMetaFromWorker ChangePrimaryCopy "
      "NotifyMasterIncNestedRefs NotifyMasterDecNestedRefs" },
    { "MasterWorkerOCService", RpcAuthPolicy::NONE,
      "ClearData MetaChange" },
    { "WorkerWorkerOCService", RpcAuthPolicy::DIRECT,
      "GetObjectRemote CheckCoordinatorState MigrateData GetClusterState BatchGetObjectRemote "
      "GetPeerHashRing QueryUbPortHealth" },
    { "WorkerWorkerOCService", RpcAuthPolicy::NONE,
      "NotifyRemoteGet" },
    { "WorkerWorkerTransportService", RpcAuthPolicy::NONE,
      "WorkerWorkerExchangeUrmaConnectInfo ProbeProviderUbRecovery" },
    { "WorkerWorkerTransportService", RpcAuthPolicy::DIRECT,
      "QueryUbPortHealth" },
    { "WorkerOCService", RpcAuthPolicy::HEALTH,
      "HealthCheck" },
    { "WorkerOCService", RpcAuthPolicy::TENANT,
      "Create Publish Get DecreaseReference ReconcileShmRef GIncreaseRef GDecreaseRef DeleteAllCopy "
      "InvalidateBuffer ReleaseGRefs QueryGlobalRefNum PublishDeviceObject GetDeviceObject MultiPublish "
      "PutP2PMeta SubscribeReceiveEvent GetP2PMeta SendRootInfo RecvRootInfo GetDataInfo AckRecvFinish "
      "MultiCreate QuerySize Exist Expire GetMetaInfo" },
    { "WorkerOCService", RpcAuthPolicy::DIRECT,
      "QueryAndGet GetHashRing" },
    { "WorkerOCService", RpcAuthPolicy::NONE,
      "IncrSeqNo GetSeqNo DelSeqNo Lpush Lpop Lindex Llen Hget Hset Hdel Hgetall RemoveP2PLocation" },
    { "WorkerOCService", RpcAuthPolicy::SYSTEM,
      "GetObjMetaInfo" },
};

struct RpcAuthMethod {
    RpcAuthPolicy policy = RpcAuthPolicy::UNSET;
    const google::protobuf::FieldDescriptor *token = nullptr;
    const google::protobuf::FieldDescriptor *accessKey = nullptr;
    const google::protobuf::FieldDescriptor *signature = nullptr;
    const google::protobuf::FieldDescriptor *clientId = nullptr;
    const google::protobuf::FieldDescriptor *tenantId = nullptr;
    const google::protobuf::FieldDescriptor *routed = nullptr;
    bool sessionTenantOnly = false;
    bool requestTenantOnly = false;
};

std::string StringField(const google::protobuf::Message &request, const google::protobuf::FieldDescriptor *field)
{
    return field == nullptr ? std::string{} : request.GetReflection()->GetString(request, field);
}

Status AuthenticateTenantKey(const RpcAuthMethod &method, const google::protobuf::Message &request,
                             const RpcRequestProof &proof)
{
    const auto *reflection = request.GetReflection();
    std::string requestedTenant = StringField(request, method.tenantId);
    const bool routed = method.routed && reflection->GetBool(request, method.routed);
    if (method.sessionTenantOnly || (requestedTenant.empty() && !method.requestTenantOnly && !routed)) {
        bool exists = false;
        requestedTenant = ClientManager::Instance().GetAuthTenantIdByClientId(
            ClientKey::Intern(StringField(request, method.clientId)), exists);
    }
    std::string tenant;
    return TenantAuthManager::Instance()->TenantAkAuth(proof.accessKey, requestedTenant, tenant);
}

Status AuthenticateRpc(const RpcAuthMethod &method, AkSkManager &manager, brpc::Controller &controller,
                       google::protobuf::Message &request)
{
    if (method.policy == RpcAuthPolicy::NONE) {
        return Status::OK();
    }
    const auto *proof = GetRpcRequestCapture(controller);
    if (method.policy == RpcAuthPolicy::DIRECT || method.policy == RpcAuthPolicy::SYSTEM) {
        const bool authEnabled = method.policy == RpcAuthPolicy::DIRECT ? manager.ServerAuthEnabled()
                                                                       : manager.SystemAuthEnabled();
        return authEnabled ? VerifyRpcRequest(manager, proof) : Status::OK();
    }
    const auto *reflection = request.GetReflection();
    // Body credentials are untrusted. Only the receiving RPC entry can publish a verified identity to business code.
    reflection->ClearField(&request, method.accessKey);
    reflection->ClearField(&request, method.signature);
    if (FLAGS_skip_authenticate) {
        return Status::OK();
    }
    if (method.policy == RpcAuthPolicy::HEALTH && StringField(request, method.clientId).empty()) {
        return Status::OK();
    }
    if (method.policy == RpcAuthPolicy::HEARTBEAT) {
        bool exists = false;
        ClientManager::Instance().GetAuthTenantIdByClientId(ClientKey::Intern(StringField(request, method.clientId)),
            exists);
        if (!exists) {
            // If this client reappears before business admission, the original exist branch must reject this identity.
            reflection->SetString(&request, method.signature, "unverified");
            return Status::OK();
        }
    }
    const bool tenantEnabled = TenantAuthManager::Instance()->AuthEnabled();
    if (tenantEnabled) {
        std::string scratch;
        if (!reflection->GetStringReference(request, method.token, &scratch).empty()) {
            return Status::OK();
        }
    }
    if (tenantEnabled && proof != nullptr && proof->captured && !proof->signature.empty()
        && manager.IsTenantAk(proof->accessKey)) {
        RETURN_IF_NOT_OK(AuthenticateTenantKey(method, request, *proof));
    } else if (!manager.SystemAuthEnabled()) {
        CHECK_FAIL_RETURN_STATUS(!tenantEnabled, K_RUNTIME_ERROR, "AK/SK or token not provide");
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS(proof && proof->captured && !proof->signature.empty(), K_NOT_AUTHORIZED,
                             "The AK/SK authorized failed.");
    RETURN_IF_NOT_OK(VerifyRpcRequest(manager, proof));
    reflection->SetString(&request, method.accessKey, proof->accessKey);
    reflection->SetString(&request, method.signature, proof->signature);
    return Status::OK();
}
}  // namespace

Status CreateRpcAuthenticator(const google::protobuf::ServiceDescriptor &service,
                              std::shared_ptr<AkSkManager> manager, BrpcAuthService::Authenticate &authenticate)
{
    CHECK_FAIL_RETURN_STATUS(manager != nullptr, K_INVALID, "AK/SK manager has not been initialized.");
    std::vector<RpcAuthMethod> methods(service.method_count());
    for (const auto &spec : POLICY_SPECS) {
        if (service.name() != spec.service) {
            continue;
        }
        std::istringstream names(spec.methods);
        std::string name;
        while (names >> name) {
            const auto *method = service.FindMethodByName(name);
            CHECK_FAIL_RETURN_STATUS(method != nullptr, K_INVALID, "Unknown RPC authentication method: " + name);
            auto &entry = methods[method->index()];
            CHECK_FAIL_RETURN_STATUS(entry.policy == RpcAuthPolicy::UNSET, K_INVALID,
                                     "Duplicate RPC authentication policy: " + method->full_name());
            entry.policy = spec.policy;
            const auto *type = method->input_type();
            entry.token = type->FindFieldByName("token");
            entry.accessKey = type->FindFieldByName("access_key");
            entry.signature = type->FindFieldByName("signature");
            entry.clientId = type->FindFieldByName("client_id");
            entry.tenantId = type->FindFieldByName("tenant_id");
            entry.routed = type->FindFieldByName("is_routed");
            entry.sessionTenantOnly = name == "Heartbeat" || name == "DisconnectClient" || name == "ReconcileShmRef";
            entry.requestTenantOnly = service.name() == "WorkerService"
                                      && (name == "RegisterClient" || name == "GetSocketPath");
            if (entry.policy == RpcAuthPolicy::TENANT || entry.policy == RpcAuthPolicy::HEALTH
                || entry.policy == RpcAuthPolicy::HEARTBEAT) {
                CHECK_FAIL_RETURN_STATUS(entry.token && entry.accessKey && entry.signature, K_INVALID,
                                         "Missing RPC tenant identity fields: " + method->full_name());
            }
        }
    }
    for (int i = 0; i < service.method_count(); ++i) {
        CHECK_FAIL_RETURN_STATUS(methods[i].policy != RpcAuthPolicy::UNSET, K_INVALID,
                                 "Missing RPC authentication policy: " + service.method(i)->full_name());
    }
    authenticate = [manager = std::move(manager), methods = std::move(methods)](
        const google::protobuf::MethodDescriptor &method, brpc::Controller &controller,
        google::protobuf::Message &request) {
        return AuthenticateRpc(methods[method.index()], *manager, controller, request);
    };
    return Status::OK();
}
}  // namespace worker
}  // namespace datasystem
