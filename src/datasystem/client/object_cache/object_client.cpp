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
 * Description: Data system Object Cache Client management.
 */

#include "datasystem/object_client.h"

#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/utils/status.h"

namespace datasystem {
ObjectClient::ObjectClient(const ConnectOptions &connectOptions)
{
    impl_ = std::make_shared<object_cache::ObjectClientImpl>(connectOptions);
}

ObjectClient::~ObjectClient()
{
    if (impl_) {
        impl_.reset();
    }
}

Status ObjectClient::ShutDown()
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    if (impl_) {
        bool needRollbackState;
        auto rc = impl_->ShutDown(needRollbackState);
        impl_->CompleteHandler(rc.IsError(), needRollbackState);
        return rc;
    }
    return Status::OK();
}

Status ObjectClient::Init()
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    (void)metrics::InitKvMetrics();
    bool needRollbackState;
    auto rc = impl_->Init(needRollbackState, true);
    impl_->CompleteHandler(rc.IsError(), needRollbackState);
    return rc;
}

Status ObjectClient::Create(const std::string &objectKey, uint64_t size, const CreateParam &param,
                            std::shared_ptr<Buffer> &buffer)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    object_cache::FullParam innerParam;
    innerParam.writeMode = WriteMode::NONE_L2_CACHE;
    innerParam.consistencyType = param.consistencyType;
    innerParam.cacheType = param.cacheType;
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_OBJECT_CLIENT_CREATE);
    Status rc = impl_->Create(objectKey, size, innerParam, buffer);
    access.ObjectKeyRef(objectKey).WriteMode(static_cast<int>(innerParam.writeMode))
        .ConsistencyType(static_cast<int>(innerParam.consistencyType)).CacheType(static_cast<int>(innerParam.cacheType))
        .DataSize(size).Result(rc).Record();
    return rc;
}

Status ObjectClient::GIncreaseRef(const std::vector<std::string> &objectKeys,
                                  std::vector<std::string> &failedObjectKeys, const std::string &remoteClientId)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_OBJECT_CLIENT_GINCREASEREF);
    Status rc = impl_->GIncreaseRef(objectKeys, failedObjectKeys, remoteClientId);
    access.ObjectKeysRef(objectKeys).RemoteClientId(remoteClientId).Result(rc).Record();
    return rc;
}

Status ObjectClient::ReleaseGRefs(const std::string &remoteClientId)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_OBJECT_CLIENT_RELEASEGREFS);
    Status rc = impl_->ReleaseGRefs(remoteClientId);
    access.RemoteClientId(remoteClientId).Result(rc).Record();
    return rc;
}

Status ObjectClient::GDecreaseRef(const std::vector<std::string> &objectKeys,
                                  std::vector<std::string> &failedObjectKeys, const std::string &remoteClientId)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_OBJECT_CLIENT_GDECREASEREF);
    Status rc = impl_->GDecreaseRef(objectKeys, failedObjectKeys, remoteClientId);
    access.ObjectKeysRef(objectKeys).RemoteClientId(remoteClientId).Result(rc).Record();
    return rc;
}

Status ObjectClient::UpdateToken(SensitiveValue token)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    return impl_->UpdateToken(token);
}

Status ObjectClient::UpdateAkSk(const std::string accesskey, SensitiveValue secretkey)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    return impl_->UpdateAkSk(accesskey, secretkey);
}

int ObjectClient::QueryGlobalRefNum(const std::string &objectKey)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_OBJECT_CLIENT_QUERY_GLOBAL_REF_NUM);
    int num = impl_->QueryGlobalRefNum(objectKey);
    access.ObjectKeyRef(objectKey).Result(Status::OK()).Record();
    return num;
}

Status ObjectClient::Put(const std::string &objectKey, const uint8_t *data, uint64_t size, const CreateParam &param,
                         const std::unordered_set<std::string> &nestedObjectKeys)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    object_cache::FullParam innerParam;
    innerParam.writeMode = WriteMode::NONE_L2_CACHE;
    innerParam.consistencyType = param.consistencyType;
    innerParam.cacheType = param.cacheType;
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_OBJECT_CLIENT_PUT);
    Status rc = impl_->Put(objectKey, data, size, innerParam, nestedObjectKeys);
    access.ObjectKeyRef(objectKey).WriteMode(static_cast<int>(innerParam.writeMode))
        .ConsistencyType(static_cast<int>(innerParam.consistencyType)).CacheType(static_cast<int>(innerParam.cacheType))
        .DataSize(size).Result(rc).Record();
    return rc;
}

Status ObjectClient::Get(const std::vector<std::string> &objectKeys, int32_t subTimeoutMs,
                         std::vector<Optional<Buffer>> &buffers)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    auto access = AccessRecorder::Object(AccessRecorderKey::DS_OBJECT_CLIENT_GET);
    Status rc = impl_->Get(objectKeys, subTimeoutMs, buffers);
    access.ObjectKeysRef(objectKeys).TimeoutMs(subTimeoutMs).Result(rc).Record();
    return rc;
}

Status ObjectClient::GetObjMetaInfo(const std::string &tenantId, const std::vector<std::string> &objectKeys,
                                    std::vector<ObjMetaInfo> &objMetas)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    return impl_->GetObjMetaInfo(tenantId, objectKeys, objMetas);
}

Status ObjectClient::GenerateKey(const std::string &prefix, std::string &key)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    return impl_->GenerateKey(key, prefix);
}

Status ObjectClient::GenerateObjectKey(const std::string &prefix, std::string &key)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    return impl_->GenerateKey(key, prefix);
}

Status ObjectClient::GetPrefix(const std::string &key, std::string &prefix)
{
    TraceGuard traceGuard = Trace::Instance().SetRequestTraceUUID();
    return impl_->GetPrefix(key, prefix);
}

Status ObjectClient::HealthCheck()
{
    ServerState state;
    return impl_->HealthCheck(state);
}
}  // namespace datasystem