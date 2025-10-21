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
 * Description: Data system Object Cache Client management.
 */

#include "datasystem/hetero_cache/hetero_client.h"

#include "datasystem/client/hetero_cache/device_util.h"
#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/utils/status.h"

namespace datasystem {
HeteroClient::HeteroClient(const ConnectOptions &connectOptions)
{
    impl_ = std::make_shared<object_cache::ObjectClientImpl>(connectOptions);
}

HeteroClient::~HeteroClient()
{
    if (impl_) {
        impl_.reset();
    }
}

Status HeteroClient::ShutDown()
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    if (impl_) {
        bool needRollbackState;
        auto rc = impl_->ShutDown(needRollbackState);
        impl_->CompleteHandler(rc.IsError(), needRollbackState);
        return rc;
    }
    return Status::OK();
}

Status HeteroClient::Init()
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    bool needRollbackState;
    auto rc = impl_->Init(needRollbackState, true);
    impl_->CompleteHandler(rc.IsError(), needRollbackState);
    return rc;
}

Status HeteroClient::MGet(const std::vector<std::string> &objectKeys, const std::vector<DeviceBlobList> &devBlobList,
                          uint64_t timeoutMs, std::vector<std::string> &failKeys)
{
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    PerfPoint point(PerfKey::CLIENT_MGET_H2D_ALL);
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    std::shared_future<AsyncResult> future = impl_->MGetH2D(objectKeys, devBlobList, timeoutMs);
    auto result = future.get();
    failKeys = std::move(result.failedList);
    return result.status;
}

Status HeteroClient::MGetH2D(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList,
                             std::vector<std::string> &failedKeys, int32_t subTimeoutMs)
{
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    PerfPoint point(PerfKey::CLIENT_MGET_H2D_ALL);
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    std::shared_future<AsyncResult> future = impl_->MGetH2D(keys, devBlobList, subTimeoutMs);
    auto result = future.get();
    failedKeys = std::move(result.failedList);
    return result.status;
}

std::shared_future<AsyncResult> HeteroClient::MGetAsync(const std::vector<std::string> &objectKeys,
                                                        const std::vector<DeviceBlobList> &devBlobList,
                                                        uint64_t timeoutMs)
{
#ifndef BUILD_HETERO
#ifndef WITH_TESTS
    std::promise<AsyncResult> promise;
    promise.set_value(
        AsyncResult{ .status = Status(K_RUNTIME_ERROR, "Hetero client is not supported. compile with -X on please!"),
                     .failedList = {} });
    return promise.get_future().share();
#endif
#endif
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    std::shared_future<AsyncResult> future = impl_->MGetH2D(objectKeys, devBlobList, timeoutMs);
    return future;
}

Status HeteroClient::MSet(const std::vector<std::string> &objectKeys, const std::vector<DeviceBlobList> &devBlobList)
{
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    PerfPoint point(PerfKey::CLIENT_MSET_D2H_ALL);
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    SetParam setParam{ .writeMode = WriteMode::NONE_L2_CACHE_EVICT };
    std::shared_future<AsyncResult> future = impl_->MSet(objectKeys, devBlobList, setParam);
    return future.get().status;
}

Status HeteroClient::MSetD2H(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList,
                             const SetParam &setParam)
{
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    PerfPoint point(PerfKey::CLIENT_MSET_D2H_ALL);
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    std::shared_future<AsyncResult> future = impl_->MSet(keys, devBlobList, setParam);
    return future.get().status;
}

std::shared_future<AsyncResult> HeteroClient::MSetAsync(const std::vector<std::string> &objectKeys,
                                                        const std::vector<DeviceBlobList> &devBlobList)
{
#ifndef BUILD_HETERO
#ifndef WITH_TESTS
    std::promise<AsyncResult> promise;
    promise.set_value(
        AsyncResult{ .status = Status(K_RUNTIME_ERROR, "Hetero client is not supported. compile with -X on please!"),
                     .failedList = {} });
    return promise.get_future().share();
#endif
#endif
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();

    return impl_->MSet(objectKeys, devBlobList, { .writeMode = WriteMode::NONE_L2_CACHE_EVICT });
}

Status HeteroClient::Delete(const std::vector<std::string> &keys, std::vector<std::string> &failedKeys)
{
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    return impl_->Delete(keys, failedKeys);
}

std::shared_future<AsyncResult> HeteroClient::AsyncMSetD2H(const std::vector<std::string> &keys,
                                                           const std::vector<DeviceBlobList> &devBlobList,
                                                           const SetParam &setParam)
{
    auto rc = HeteroClient::IsCompileWithHetero();
    if (rc.IsError()) {
        std::promise<AsyncResult> promise;
        std::shared_future<AsyncResult> future = promise.get_future().share();
        promise.set_value({ rc, keys });
        return future;
    }
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    return impl_->MSet(keys, devBlobList, setParam);
}

std::shared_future<AsyncResult> HeteroClient::AsyncMGetH2D(const std::vector<std::string> &keys,
                                                           const std::vector<DeviceBlobList> &devBlobList,
                                                           uint64_t subTimeoutMs)
{
    auto rc = HeteroClient::IsCompileWithHetero();
    if (rc.IsError()) {
        std::promise<AsyncResult> promise;
        std::shared_future<AsyncResult> future = promise.get_future().share();
        promise.set_value({ rc, keys });
        return future;
    }
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    return impl_->MGetH2D(keys, devBlobList, subTimeoutMs);
}

Status HeteroClient::GenerateKey(const std::string &prefix, std::string &key)
{
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    return impl_->GenerateKey(key, prefix);
}

Status HeteroClient::DevPublish(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList,
                                std::vector<Future> &futureVec)
{
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    return impl_->DevPublish(keys, devBlobList, futureVec);
}

Status HeteroClient::DevSubscribe(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList,
                                  std::vector<Future> &futureVec)
{
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    return impl_->DevSubscribe(keys, devBlobList, futureVec);
}

Status HeteroClient::LocalDelete(const std::vector<std::string> &objectKeys, std::vector<std::string> &failedKeys)
{
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    return impl_->DevLocalDelete(objectKeys, failedKeys);
}

Status HeteroClient::DevDelete(const std::vector<std::string> &keys, std::vector<std::string> &failedKeys)
{
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    return impl_->DeleteDevObjects(keys, failedKeys);
}

Status HeteroClient::DevLocalDelete(const std::vector<std::string> &keys, std::vector<std::string> &failedKeys)
{
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    return impl_->DevLocalDelete(keys, failedKeys);
}

std::shared_future<AsyncResult> HeteroClient::AsyncDevDelete(const std::vector<std::string> &keys)
{
    AccessRecorder accessPoint(AccessRecorderKey::DS_HETERO_CLIENT_ASUNC_DEVDELETE);
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    std::shared_future<AsyncResult> future = impl_->AsyncDeleteDevObjects(keys);
    accessPoint.Record(
        StatusCode::K_OK, keys.empty() ? "0" : std::to_string(keys[0].size()),
        RequestParam{ .objectKey =
                          FormatString("%s+count:%s", keys.empty() ? "" : keys[0].substr(0, LOG_OBJECT_KEY_SIZE_LIMIT),
                                       keys.size()) },
        "Async call success");
    return future;
}

Status HeteroClient::DevMSet(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList,
                             std::vector<std::string> &failedKeys)
{
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    return impl_->DevMSet(keys, devBlobList, failedKeys);
}

Status HeteroClient::DevPreFetch(const std::vector<std::string> &keys, std::vector<DeviceBlobList> &blob2dList)
{
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    (void)keys;
    (void)blob2dList;
    return Status::OK();
}

Status HeteroClient::DevMGet(const std::vector<std::string> &keys, std::vector<DeviceBlobList> &devBlobList,
                             std::vector<std::string> &failedKeys, int32_t subTimeoutMs)
{
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    return impl_->DevMGet(keys, devBlobList, failedKeys, subTimeoutMs);
}

Status HeteroClient::HealthCheck(ServerState &state)
{
    return impl_->HealthCheck(state);
}

Status HeteroClient::Exist(const std::vector<std::string> &keys, std::vector<bool> &exists)
{
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    return impl_->Exist(keys, exists, false, false);
}

Status HeteroClient::GetMetaInfo(const std::vector<std::string> &keys, bool isDevKey, std::vector<MetaInfo> &metaInfos,
                                 std::vector<std::string> &failKeys)
{
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    PerfPoint point(PerfKey::CLIENT_GET_META_INFO);
    return impl_->GetMetaInfo(keys, isDevKey, metaInfos, failKeys);
}

Status HeteroClient::IsCompileWithHetero()
{
#ifndef BUILD_HETERO
#ifndef WITH_TESTS
    return Status(K_RUNTIME_ERROR, "Hetero client is not supported. compile with -X on please!");
#endif
#endif
    return Status::OK();
}
}  // namespace datasystem
