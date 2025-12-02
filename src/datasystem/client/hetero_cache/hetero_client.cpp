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

#include "datasystem/hetero_client.h"

#include "datasystem/client/hetero_cache/device_util.h"
#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/utils/status.h"

namespace datasystem {

namespace {
/**
* \brief Calculate the total data size of devBlobList in request
* \param[in] devBlobList List of device blobs. Constraint: blobs under the same DeviceBlobList.
* \return Total data size in bytes
*/
uint64_t CalculateDataSize(const std::vector<DeviceBlobList> &devBlobList)
{
    uint64_t totalSize = 0;
    const uint64_t max_val = std::numeric_limits<uint64_t>::max();
    for (const auto &deviceBlobList : devBlobList) {
        for (const auto &blob : deviceBlobList.blobs) {
            if (blob.size > 0 && max_val - totalSize < blob.size) {
                // maybe overflow？
                totalSize = max_val;
            } else {
                totalSize += blob.size;
            }
        }
    }
    return totalSize;
}
} // namespace

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
        AccessRecorder accessPoint(AccessRecorderKey::DS_HETERO_CLIENT_SHUTDOWN);
        auto rc = impl_->ShutDown(needRollbackState);
        impl_->CompleteHandler(rc.IsError(), needRollbackState);
        accessPoint.Record(rc.GetCode(), "0", {}, "ShutDown call success");
        return rc;
    }
    return Status::OK();
}

Status HeteroClient::Init()
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    AccessRecorder accessPoint(AccessRecorderKey::DS_HETERO_CLIENT_INIT);
    bool needRollbackState;
    auto rc = impl_->Init(needRollbackState, true);
    impl_->CompleteHandler(rc.IsError(), needRollbackState);
    accessPoint.Record(rc.GetCode(), "0", {}, "Init call success");
    return rc;
}

Status HeteroClient::MGetH2D(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList,
                             std::vector<std::string> &failedKeys, int32_t subTimeoutMs)
{
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    PerfPoint point(PerfKey::CLIENT_MGET_H2D_ALL);
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    AccessRecorder accessPoint(AccessRecorderKey::DS_HETERO_CLIENT_MGETH2D);
    std::shared_future<AsyncResult> future = impl_->MGetH2D(keys, devBlobList, subTimeoutMs);
    auto result = future.get();
    failedKeys = std::move(result.failedList);
    accessPoint.Record(
        result.status.GetCode(), std::to_string(CalculateDataSize(devBlobList)),
        RequestParam{
            .objectKey = FormatString("%s+count:%s", keys.empty() ? "" : keys[0].substr(0, LOG_OBJECT_KEY_SIZE_LIMIT),
            keys.size()) },
        "MGetH2D call success");
    return result.status;
}

Status HeteroClient::MSetD2H(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList,
                             const SetParam &setParam)
{
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    PerfPoint point(PerfKey::CLIENT_MSET_D2H_ALL);
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    AccessRecorder accessPoint(AccessRecorderKey::DS_HETERO_CLIENT_MSETD2H);
    std::shared_future<AsyncResult> future = impl_->MSet(keys, devBlobList, setParam);
    auto rc = future.get().status;
    accessPoint.Record(
        rc.GetCode(), std::to_string(CalculateDataSize(devBlobList)),
        RequestParam{
            .objectKey = FormatString("%s+count:%s", keys.empty() ? "" : keys[0].substr(0, LOG_OBJECT_KEY_SIZE_LIMIT),
            keys.size()) },
        "MSetD2H call success");
    return rc;
}

Status HeteroClient::Delete(const std::vector<std::string> &keys, std::vector<std::string> &failedKeys)
{
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    AccessRecorder accessPoint(AccessRecorderKey::DS_HETERO_CLIENT_DELETE);
    auto rc = impl_->Delete(keys, failedKeys);
    accessPoint.Record(
        rc.GetCode(), "0",
        RequestParam{
            .objectKey = FormatString("%s+count:%s", keys.empty() ? "" : keys[0].substr(0, LOG_OBJECT_KEY_SIZE_LIMIT),
            keys.size()) },
        "Delete call success");
    return rc;
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
    AccessRecorder accessPoint(AccessRecorderKey::DS_HETERO_CLIENT_GENERATEKEY);
    auto ret = impl_->GenerateKey(key, prefix);
    accessPoint.Record(ret.GetCode(), "0", RequestParam{ .objectKey = key }, "GenerateKey call success");
    return ret;
}

Status HeteroClient::DevPublish(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList,
                                std::vector<Future> &futureVec)
{
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    AccessRecorder accessPoint(AccessRecorderKey::DS_HETERO_CLIENT_DEVPUBLISH);
    auto ret = impl_->DevPublish(keys, devBlobList, futureVec);
    accessPoint.Record(
        ret.GetCode(), std::to_string(CalculateDataSize(devBlobList)),
        RequestParam{
            .objectKey = FormatString("%s+count:%s", keys.empty() ? "" : keys[0].substr(0, LOG_OBJECT_KEY_SIZE_LIMIT),
            keys.size()) },
        "DevPublish call success");
    return ret;
}

Status HeteroClient::DevSubscribe(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList,
                                  std::vector<Future> &futureVec)
{
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    AccessRecorder accessPoint(AccessRecorderKey::DS_HETERO_CLIENT_DEVSUBSCRIBE);
    auto ret = impl_->DevSubscribe(keys, devBlobList, futureVec);
    accessPoint.Record(
        ret.GetCode(), std::to_string(CalculateDataSize(devBlobList)),
        RequestParam{
            .objectKey = FormatString("%s+count:%s", keys.empty() ? "" : keys[0].substr(0, LOG_OBJECT_KEY_SIZE_LIMIT),
            keys.size()) },
        "DevSubscribe call success");
    return ret;
}

Status HeteroClient::DevDelete(const std::vector<std::string> &keys, std::vector<std::string> &failedKeys)
{
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    AccessRecorder accessPoint(AccessRecorderKey::DS_HETERO_CLIENT_DEVDELETE);
    auto ret = impl_->DeleteDevObjects(keys, failedKeys);
    accessPoint.Record(
        ret.GetCode(), "0",
        RequestParam{
            .objectKey = FormatString("%s+count:%s", keys.empty() ? "" : keys[0].substr(0, LOG_OBJECT_KEY_SIZE_LIMIT),
            keys.size()) },
        "DevDelete call success");
    return ret;
}

Status HeteroClient::DevLocalDelete(const std::vector<std::string> &keys, std::vector<std::string> &failedKeys)
{
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    AccessRecorder accessPoint(AccessRecorderKey::DS_HETERO_CLIENT_DEVLOCALDELETE);
    auto ret = impl_->DevLocalDelete(keys, failedKeys);
    accessPoint.Record(
        ret.GetCode(), "0",
        RequestParam{
            .objectKey = FormatString("%s+count:%s", keys.empty() ? "" : keys[0].substr(0, LOG_OBJECT_KEY_SIZE_LIMIT),
            keys.size()) },
        "DevLocalDelete call success");
    return ret;
}

std::shared_future<AsyncResult> HeteroClient::AsyncDevDelete(const std::vector<std::string> &keys)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    std::shared_future<AsyncResult> future = impl_->AsyncDeleteDevObjects(keys);
    return future;
}

Status HeteroClient::DevMSet(const std::vector<std::string> &keys, const std::vector<DeviceBlobList> &devBlobList,
                             std::vector<std::string> &failedKeys)
{
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    AccessRecorder accessPoint(AccessRecorderKey::DS_HETERO_CLIENT_DEVMSET);
    auto ret = impl_->DevMSet(keys, devBlobList, failedKeys);
    accessPoint.Record(
        ret.GetCode(), std::to_string(CalculateDataSize(devBlobList)),
        RequestParam{
            .objectKey = FormatString("%s+count:%s", keys.empty() ? "" : keys[0].substr(0, LOG_OBJECT_KEY_SIZE_LIMIT),
            keys.size()) },
        "DevMSet call success");
    return ret;
}

Status HeteroClient::DevMGet(const std::vector<std::string> &keys, std::vector<DeviceBlobList> &devBlobList,
                             std::vector<std::string> &failedKeys, int32_t subTimeoutMs)
{
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    AccessRecorder accessPoint(AccessRecorderKey::DS_HETERO_CLIENT_DEVMGET);
    auto ret = impl_->DevMGet(keys, devBlobList, failedKeys, subTimeoutMs);
    accessPoint.Record(
        ret.GetCode(), std::to_string(CalculateDataSize(devBlobList)),
        RequestParam{
            .objectKey = FormatString("%s+count:%s", keys.empty() ? "" : keys[0].substr(0, LOG_OBJECT_KEY_SIZE_LIMIT),
            keys.size()) },
        "DevMGet call success");
    return ret;
}

Status HeteroClient::HealthCheck(ServerState &state)
{
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    AccessRecorder accessPoint(AccessRecorderKey::DS_HETERO_CLIENT_HEALTHCHECK);
    auto rc = impl_->HealthCheck(state);
    accessPoint.Record(rc.GetCode(), "0", {}, "HealthCheck call success");
    return rc;
}

Status HeteroClient::Exist(const std::vector<std::string> &keys, std::vector<bool> &exists)
{
    AccessRecorder accessPoint(AccessRecorderKey::DS_HETERO_CLIENT_EXIST);
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    auto rc = impl_->Exist(keys, exists, false, false);
    accessPoint.Record(
        rc.GetCode(), "0",
        RequestParam{
            .objectKey = FormatString("%s+count:%s", keys.empty() ? "" : keys[0].substr(0, LOG_OBJECT_KEY_SIZE_LIMIT),
            keys.size()) },
        "Exist call success");
    return rc;
}

Status HeteroClient::GetMetaInfo(const std::vector<std::string> &keys, bool isDevKey, std::vector<MetaInfo> &metaInfos,
                                 std::vector<std::string> &failKeys)
{
    AccessRecorder accessPoint(AccessRecorderKey::DS_HETERO_CLIENT_GETMETAINFO);
    RETURN_IF_NOT_OK(HeteroClient::IsCompileWithHetero());
    TraceGuard traceGuard = Trace::Instance().SetTraceUUID();
    PerfPoint point(PerfKey::CLIENT_GET_META_INFO);
    auto rc = impl_->GetMetaInfo(keys, isDevKey, metaInfos, failKeys);
    accessPoint.Record(
        rc.GetCode(), "0",
        RequestParam{
            .objectKey = FormatString("%s+count:%s", keys.empty() ? "" : keys[0].substr(0, LOG_OBJECT_KEY_SIZE_LIMIT),
            keys.size()) },
        "GetMetaInfo call success");
    return rc;
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
