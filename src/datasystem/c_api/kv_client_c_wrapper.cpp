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
 * Description: Client c wrapper for state cache client.
 */
#include "datasystem/c_api/kv_client_c_wrapper.h"

#include <cstddef>
#include <cstring>
#include <functional>

#include <securec.h>

#include "datasystem/c_api/util.h"
#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/object/buffer.h"
#include "datasystem/utils/optional.h"
#include "datasystem/utils/status.h"
#include "datasystem/utils/string_view.h"
#include "kv_client_c_wrapper.h"
#include "status_definition.h"

KVClient_p KVCreateClient(const char *cWorkerHost, const int workerPort, const int timeOut, const char *token,
                          size_t tokenLen, const char *clientPublicKey, size_t cClientPublicKeyLen,
                          const char *clientPrivateKey, size_t clientPrivateKeyLen, const char *serverPublicKey,
                          size_t cServerPublicKeyLen, const char *accessKey, size_t cAccessKeyLen,
                          const char *secretKey, size_t secretKeyLen, const char *tenantId, size_t cTenantIdLen,
                          const char *enableCrossNodeConnection)
{
    datasystem::TraceGuard traceGuard = datasystem::Trace::Instance().SetTraceUUID();
    return CreateObjectClient(cWorkerHost, workerPort, timeOut, token, tokenLen, clientPublicKey, cClientPublicKeyLen,
                              clientPrivateKey, clientPrivateKeyLen, serverPublicKey, cServerPublicKeyLen, accessKey,
                              cAccessKeyLen, secretKey, secretKeyLen, tenantId, cTenantIdLen,
                              enableCrossNodeConnection);
}

struct StatusC SCConnectWorker(KVClient_p clientPtr)
{
    datasystem::TraceGuard traceGuard = datasystem::Trace::Instance().SetTraceUUID();
    return ConnectWorker(clientPtr);
}

struct StatusC SCUpdateAkSk(KVClient_p clientPtr, const char *cAccessKey, size_t cAccessKeyLen, const char *cSecretKey,
                            size_t cSecretKeyLen)
{
    auto client = reinterpret_cast<std::shared_ptr<datasystem::object_cache::ObjectClientImpl> *>(clientPtr);
    std::string accessKey(cAccessKey, cAccessKeyLen);
    accessKey.assign(cAccessKey, cAccessKeyLen);
    datasystem::SensitiveValue secretKey(cSecretKey, cSecretKeyLen);
    datasystem::Status rc = (*client)->UpdateAkSk(accessKey, secretKey);
    if (rc.IsError()) {
        return ToStatusC(rc);
    }
    return StatusC{ datasystem::K_OK, {} };
}

void SCFreeClient(KVClient_p clientPtr)
{
    FreeClient(clientPtr);
}

struct StatusC SCExecuteSet(KVClient_p clientPtr, const char *cKey, size_t keyLen, const char *cVal, size_t valLen,
                            const char *cWriteMode, uint32_t ttlSecond, const char *cExistenceOpt)
{
    std::string errorMsg;
    CheckNullptr(clientPtr, "clientPtr", errorMsg);
    CheckNullptr(cKey, "cKey", errorMsg);
    CheckNullptr(cVal, "cVal", errorMsg);
    CheckNullptr(cWriteMode, "cWriteMode", errorMsg);
    CheckNullptr(cExistenceOpt, "cExistenceOpt", errorMsg);
    if (!errorMsg.empty()) {
        return MakeStatusC(datasystem::K_INVALID, errorMsg);
    }
    auto client = reinterpret_cast<std::shared_ptr<datasystem::object_cache::ObjectClientImpl> *>(clientPtr);
    std::string key(cKey, keyLen);
    datasystem::StringView val(cVal, valLen);
    std::string writeMode(cWriteMode);
    std::string existenceOpt(cExistenceOpt);
    datasystem::SetParam setParam;
    StatusC s = InitSetParam(writeMode, ttlSecond, existenceOpt, setParam);
    if (s.code != datasystem::K_OK) {
        return s;
    }
    datasystem::Status rc = (*client)->Set(key, val, setParam);
    if (rc.IsError()) {
        return ToStatusC(rc);
    }
    return StatusC{ datasystem::K_OK, {} };
}

struct StatusC SCSet(KVClient_p clientPtr, const char *cKey, size_t keyLen, const char *cVal, size_t valLen,
                     const char *cWriteMode, uint32_t ttlSecond, const char *cExistenceOpt)
{
    datasystem::TraceGuard traceGuard = datasystem::Trace::Instance().SetTraceUUID();
    datasystem::AccessRecorder accessPoint(datasystem::AccessRecorderKey::DS_KV_CLIENT_SET);
    StatusC rc = SCExecuteSet(clientPtr, cKey, keyLen, cVal, valLen, cWriteMode, ttlSecond, cExistenceOpt);
    std::string s(cKey, keyLen);
    datasystem::RequestParam reqParam;
    if (cKey != nullptr) {
        std::string s(cKey);
        reqParam.objectKey = s.substr(0, datasystem::LOG_OBJECT_KEY_SIZE_LIMIT);
    }
    reqParam.writeMode = cWriteMode;
    reqParam.ttlSecond = std::to_string(ttlSecond);
    accessPoint.Record(rc.code, std::to_string(valLen), reqParam, rc.errMsg);
    return rc;
}

struct StatusC SCExecuteSetValue(KVClient_p clientPtr, char **cKey, size_t *keyLen, const char *cVal, size_t valLen,
                                 const char *cWriteMode, uint32_t ttlSecond, const char *cExistenceOpt)
{
    std::string errorMsg;
    CheckNullptr(clientPtr, "clientPtr", errorMsg);
    CheckNullptr(cKey, "cKey", errorMsg);
    CheckNullptr(cVal, "cVal", errorMsg);
    CheckNullptr(cWriteMode, "cWriteMode", errorMsg);
    if (!errorMsg.empty()) {
        return MakeStatusC(datasystem::K_INVALID, errorMsg);
    }
    auto client = reinterpret_cast<std::shared_ptr<datasystem::object_cache::ObjectClientImpl> *>(clientPtr);
    datasystem::StringView val(cVal, valLen);
    std::string writeMode(cWriteMode);
    datasystem::SetParam setParam;
    StatusC s = InitSetParam(writeMode, ttlSecond, cExistenceOpt, setParam);
    if (s.code != datasystem::K_OK) {
        return s;
    }
    std::string key;
    datasystem::Status rc = (*client)->Set(val, setParam, key);
    if (rc.IsError()) {
        return ToStatusC(rc);
    }
    *cKey = StringToCString(key);
    if (*cKey == nullptr) {
        return MakeStatusC(datasystem::K_RUNTIME_ERROR, "Failed to construct the c_style character string.");
    }
    *keyLen = key.size();
    return StatusC{ datasystem::K_OK, {} };
}

struct StatusC SCSetValue(KVClient_p clientPtr, char **cKey, size_t *keyLen, const char *cVal, size_t valLen,
                          const char *cWriteMode, uint32_t ttlSecond, const char *cExistenceOpt)
{
    datasystem::TraceGuard traceGuard = datasystem::Trace::Instance().SetTraceUUID();
    datasystem::AccessRecorder accessPoint(datasystem::AccessRecorderKey::DS_KV_CLIENT_SET);
    StatusC rc = SCExecuteSetValue(clientPtr, cKey, keyLen, cVal, valLen, cWriteMode, ttlSecond, cExistenceOpt);
    datasystem::RequestParam reqParam;
    reqParam.objectKey = "";
    if (cKey != nullptr && *cKey != nullptr) {
        std::string s(*cKey);
        reqParam.objectKey = s.substr(0, datasystem::LOG_OBJECT_KEY_SIZE_LIMIT);
    }
    reqParam.writeMode = cWriteMode;
    reqParam.ttlSecond = std::to_string(ttlSecond);
    accessPoint.Record(rc.code, std::to_string(valLen), reqParam, rc.errMsg);
    return rc;
}

struct StatusC SCExecuteGet(KVClient_p clientPtr, const char *cKey, const size_t keyLen, uint32_t ctimeoutms,
                            char **cVal, size_t *valLen)
{
    std::string errorMsg;
    CheckNullptr(clientPtr, "clientPtr", errorMsg);
    CheckNullptr(cKey, "cKey", errorMsg);
    CheckNullptr(cVal, "cVal", errorMsg);
    if (!errorMsg.empty()) {
        return MakeStatusC(datasystem::K_INVALID, errorMsg);
    }
    auto client = reinterpret_cast<std::shared_ptr<datasystem::object_cache::ObjectClientImpl> *>(clientPtr);
    std::string key(cKey, keyLen);
    std::vector<datasystem::Optional<datasystem::Buffer>> buffers;
    datasystem::Status rc = (*client)->Get({ key }, ctimeoutms, buffers);
    if (rc.IsError()) {
        return ToStatusC(rc);
    }
    StatusC s = BufferToCString(std::move(buffers[0]), (*client)->GetMemoryCopyThreadPool(), cVal, valLen);
    if (s.code != datasystem::K_OK) {
        return s;
    }
    return StatusC{ datasystem::K_OK, {} };
}

struct StatusC SCGet(KVClient_p clientPtr, const char *cKey, const size_t keyLen, uint32_t ctimeoutms, char **cVal,
                     size_t *valLen)
{
    datasystem::TraceGuard traceGuard = datasystem::Trace::Instance().SetTraceUUID();
    datasystem::AccessRecorder accessPoint(datasystem::AccessRecorderKey::DS_KV_CLIENT_GET);
    StatusC rc = SCExecuteGet(clientPtr, cKey, keyLen, ctimeoutms, cVal, valLen);
    std::string s(cKey, keyLen);
    datasystem::RequestParam reqParam;
    reqParam.objectKey = s.substr(0, datasystem::LOG_OBJECT_KEY_SIZE_LIMIT);
    reqParam.timeout = std::to_string(ctimeoutms);
    if (rc.code == datasystem::K_NOT_FOUND) {
        accessPoint.Record(datasystem::K_OK, std::to_string(*valLen), reqParam, rc.errMsg);
    } else {
        accessPoint.Record(rc.code, std::to_string(*valLen), reqParam, rc.errMsg);
    }
    return rc;
}

struct StatusC SCExecuteDel(KVClient_p clientPtr, const char *cKey, const size_t keyLen)
{
    std::string errorMsg;
    CheckNullptr(clientPtr, "clientPtr", errorMsg);
    CheckNullptr(cKey, "cKey", errorMsg);
    if (!errorMsg.empty()) {
        return MakeStatusC(datasystem::K_INVALID, errorMsg);
    }
    auto client = reinterpret_cast<std::shared_ptr<datasystem::object_cache::ObjectClientImpl> *>(clientPtr);
    std::vector<std::string> failedKeys;
    std::string key(cKey, keyLen);
    datasystem::Status rc = (*client)->Delete({ key }, failedKeys);
    if (rc.IsError()) {
        return ToStatusC(rc);
    }
    if (!failedKeys.empty()) {
        return MakeStatusC(datasystem::K_RUNTIME_ERROR, "The failed key is not empty, delete key failed!");
    }
    return StatusC{ datasystem::K_OK, {} };
}

struct StatusC SCDel(KVClient_p clientPtr, const char *cKey, const size_t keyLen)
{
    datasystem::TraceGuard traceGuard = datasystem::Trace::Instance().SetTraceUUID();
    datasystem::AccessRecorder accessPoint(datasystem::AccessRecorderKey::DS_KV_CLIENT_DELETE);
    StatusC rc = SCExecuteDel(clientPtr, cKey, keyLen);
    std::string s(cKey, keyLen);
    datasystem::RequestParam reqParam;
    reqParam.objectKey = s.substr(0, datasystem::LOG_OBJECT_KEY_SIZE_LIMIT);
    accessPoint.Record(rc.code, "0", reqParam, rc.errMsg);
    return rc;
}

struct StatusC SCGetArray(KVClient_p clientPtr, const char **cKeys, const size_t *keysLen, uint64_t keysNum,
                          uint32_t ctimeoutms, char **cVals, size_t *valsLen)
{
    datasystem::TraceGuard traceGuard = datasystem::Trace::Instance().SetTraceUUID();
    datasystem::AccessRecorder accessPoint(datasystem::AccessRecorderKey::DS_KV_CLIENT_GET);
    size_t totalSize = 0;
    datasystem::RequestParam reqParam;
    StatusC rc = ExecuteGetArray(clientPtr, cKeys, keysLen, keysNum, ctimeoutms, cVals, valsLen, &totalSize, &reqParam);
    if (rc.code == datasystem::K_NOT_FOUND) {
        accessPoint.Record(datasystem::K_OK, std::to_string(totalSize), reqParam, rc.errMsg);
    } else {
        accessPoint.Record(rc.code, std::to_string(totalSize), reqParam, rc.errMsg);
    }
    return rc;
}

struct StatusC SCExecuteDelArray(KVClient_p clientPtr, const char **cKeys, uint64_t numObjs, char **cFailedKeys,
                                 uint64_t *failedCount)
{
    std::string errorMsg;
    CheckNullptr(clientPtr, "clientPtr", errorMsg);
    CheckNullptr(cKeys, "cKeys", errorMsg);
    CheckNullptr(cFailedKeys, "cFailedKeys", errorMsg);
    CheckNullptr(failedCount, "failedCount", errorMsg);
    if (!errorMsg.empty()) {
        return MakeStatusC(datasystem::K_INVALID, errorMsg);
    }
    auto client = reinterpret_cast<std::shared_ptr<datasystem::object_cache::ObjectClientImpl> *>(clientPtr);
    std::vector<std::string> keysVec(cKeys, cKeys + numObjs);
    std::vector<std::string> failedKeys;
    datasystem::Status rc = (*client)->Delete(keysVec, failedKeys);
    // Even with errors we can have partial results
    // send the results back as const char
    *failedCount = failedKeys.size();
    for (size_t i = 0; i < failedKeys.size(); ++i) {
        cFailedKeys[i] = StringToCString(failedKeys[i]);
    }
    if (rc.IsError()) {
        LOG(ERROR) << datasystem::FormatString("SCDelArray with error:%s", rc.GetMsg());
    }
    return ToStatusC(rc);
}

struct StatusC SCDelArray(KVClient_p clientPtr, const char **cKeys, uint64_t numObjs, char **cFailedKeys,
                          uint64_t *failedCount)
{
    datasystem::TraceGuard traceGuard = datasystem::Trace::Instance().SetTraceUUID();
    datasystem::AccessRecorder accessPoint(datasystem::AccessRecorderKey::DS_KV_CLIENT_DELETE);
    StatusC rc = SCExecuteDelArray(clientPtr, cKeys, numObjs, cFailedKeys, failedCount);
    datasystem::RequestParam reqParam;
    if (cKeys != nullptr && *cKeys != nullptr) {
        reqParam.objectKey = datasystem::objectKeysToString(cKeys, numObjs);
    }
    accessPoint.Record(rc.code, "0", reqParam, rc.errMsg);
    return rc;
}

size_t SCGenerateKey(KVClient_p clientPtr, char **key)
{
    if (clientPtr == nullptr) {
        return 0;
    }
    auto client = reinterpret_cast<std::shared_ptr<datasystem::object_cache::ObjectClientImpl> *>(clientPtr);
    std::string strKey;
    (void)(*client)->GenerateKey(strKey);
    *key = StringToCString(strKey);
    return strKey.size();
}
