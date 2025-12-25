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
 * Description: Client c wrapper for object cache client.
 */
#include "datasystem/c_api/object_client_c_wrapper.h"

#include <cstddef>
#include <cstring>
#include <functional>

#include <securec.h>

#include "datasystem/c_api/util.h"
#include "datasystem/c_api/utilC.h"
#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/trace.h"
#include "datasystem/object/buffer.h"
#include "datasystem/utils/status.h"

ObjectClient_p OCCreateClient(const char *cWorkerHost, const int workerPort, const int timeOut,
                              const char *clientPublicKey, size_t cClientPublicKeyLen, const char *clientPrivateKey,
                              size_t clientPrivateKeyLen, const char *serverPublicKey, size_t cServerPublicKeyLen,
                              const char *accessKey, size_t cAccessKeyLen, const char *secretKey, size_t secretKeyLen,
                              const char *tenantId, size_t cTenantIdLen, const char *enableCrossNodeConnection)
{
    datasystem::TraceGuard traceGuard = datasystem::Trace::Instance().SetTraceUUID();
    return CreateObjectClient(cWorkerHost, workerPort, timeOut, clientPublicKey, cClientPublicKeyLen, clientPrivateKey,
                              clientPrivateKeyLen, serverPublicKey, cServerPublicKeyLen, accessKey, cAccessKeyLen,
                              secretKey, secretKeyLen, tenantId, cTenantIdLen, enableCrossNodeConnection);
}

struct StatusC OCConnectWorker(ObjectClient_p clientPtr)
{
    datasystem::TraceGuard traceGuard = datasystem::Trace::Instance().SetTraceUUID();
    return ConnectWorker(clientPtr);
}

void OCFreeClient(ObjectClient_p clientPtr)
{
    FreeClient(clientPtr);
}

struct StatusC ObjectExecutePut(ObjectClient_p clientPtr, const char *cObjKey, size_t cObjKeyLen, const char *cVal,
                                size_t cValLen, const char **cNestedObjectKeys, const size_t *cNestedObjKeyLenArray,
                                const size_t cNestedObjectKeysNum, const char *cConsistencyType,
                                datasystem::RequestParam *reqParam)
{
    std::string errorMsg;
    CheckNullptr(clientPtr, "clientPtr", errorMsg);
    CheckNullptr(cObjKey, "cObjKey", errorMsg);
    CheckNullptr(cVal, "cVal", errorMsg);
    CheckNullptr(cConsistencyType, "cConsistencyType", errorMsg);
    if (!errorMsg.empty()) {
        return MakeStatusC(datasystem::K_INVALID, errorMsg);
    }
    // cNestedObjectKeys is an optional parameter and may be nullptr.
    std::unordered_set<std::string> nestedObjectKeysSet;
    if (cNestedObjectKeys != nullptr) {
        for (size_t i = 0; i < cNestedObjectKeysNum; i++) {
            std::string objectKey(cNestedObjectKeys[i], cNestedObjKeyLenArray[i]);
            (void)nestedObjectKeysSet.insert(objectKey);
        }
        (*reqParam).nestedKey = objectKeysToString(cNestedObjectKeys, cNestedObjectKeysNum);
    }
    auto client = reinterpret_cast<std::shared_ptr<datasystem::object_cache::ObjectClientImpl> *>(clientPtr);
    std::string consistencyType(cConsistencyType);
    datasystem::object_cache::FullParam createParam;
    StatusC s = InitCreateParam(consistencyType, createParam);
    if (s.code != datasystem::K_OK) {
        return s;
    }
    std::string objKey(cObjKey, cObjKeyLen);
    Status rc =
        (*client)->Put(objKey, reinterpret_cast<const uint8_t *>(cVal), cValLen, createParam, nestedObjectKeysSet);
    if (rc.IsError()) {
        return ToStatusC(rc);
    }
    (*reqParam).writeMode = std::to_string(static_cast<int>(createParam.writeMode));
    (*reqParam).consistencyType = consistencyType;
    return StatusC{ datasystem::K_OK, {} };
}

struct StatusC OCPut(ObjectClient_p clientPtr, const char *cObjKey, size_t cObjKeyLen, const char *cVal, size_t cValLen,
                     const char **cNestedObjectKeys, const size_t *cNestedObjKeyLenArray,
                     const size_t cNestedObjectKeysNum, const char *cConsistencyType)
{
    datasystem::TraceGuard traceGuard = datasystem::Trace::Instance().SetTraceUUID();
    datasystem::AccessRecorder accessPoint(datasystem::AccessRecorderKey::DS_OBJECT_CLIENT_PUT);
    datasystem::RequestParam reqParam;
    StatusC rc = ObjectExecutePut(clientPtr, cObjKey, cObjKeyLen, cVal, cValLen, cNestedObjectKeys,
                                  cNestedObjKeyLenArray, cNestedObjectKeysNum, cConsistencyType, &reqParam);
    accessPoint.Record(rc.code, std::to_string(cValLen), reqParam, rc.errMsg);
    return rc;
}

struct StatusC OCGet(ObjectClient_p clientPtr, const char **cObjKeys, const size_t *cObjKeysLen, uint64_t objsNum,
                     uint32_t cTimeoutMs, char **cVals, size_t *valsLen)
{
    datasystem::TraceGuard traceGuard = datasystem::Trace::Instance().SetTraceUUID();
    datasystem::AccessRecorder accessPoint(datasystem::AccessRecorderKey::DS_OBJECT_CLIENT_GET);
    size_t totalSize = 0;
    datasystem::RequestParam reqParam;
    StatusC rc =
        ExecuteGetArray(clientPtr, cObjKeys, cObjKeysLen, objsNum, cTimeoutMs, cVals, valsLen, &totalSize, &reqParam);
    accessPoint.Record(rc.code, std::to_string(totalSize), reqParam, rc.errMsg);
    return rc;
}

struct StatusC OCExecuteGIncreaseRef(ObjectClient_p clientPtr, const char **cObjKeys, const size_t *cObjKeysLen,
                                     uint64_t cObjKeysNum, char *cRemoteClientId, size_t cRemoteClientIdLen,
                                     char **cFailedObjKeys, size_t *failedObjKeysCount,
                                     datasystem::RequestParam *reqParam)
{
    std::string errorMsg;
    CheckNullptr(clientPtr, "clientPtr", errorMsg);
    CheckNullptr(cObjKeys, "cObjKeys", errorMsg);
    CheckNullptr(reqParam, "reqParam", errorMsg);
    CheckNullptr(cObjKeys, "cObjKeys", errorMsg);
    if (!errorMsg.empty()) {
        return MakeStatusC(datasystem::K_INVALID, errorMsg);
    }
    // cNestedObjectKeys is an optional parameter and may be nullptr.
    std::string remoteClientId;
    if (cRemoteClientId != nullptr) {
        remoteClientId = std::string(cRemoteClientId, cRemoteClientIdLen);
    }
    (*reqParam).objectKey = datasystem::objectKeysToString(cObjKeys, cObjKeysNum);
    auto client = reinterpret_cast<std::shared_ptr<datasystem::object_cache::ObjectClientImpl> *>(clientPtr);
    std::vector<std::string> objKeysVec = GetObjKeysVector(cObjKeys, cObjKeysLen, cObjKeysNum);
    std::vector<std::string> failedObjectKeys;
    datasystem::Status rc = (*client)->GIncreaseRef(objKeysVec, failedObjectKeys, remoteClientId);
    // Even with errors we can have partial results
    // send the results back as const char
    *failedObjKeysCount = failedObjectKeys.size();
    for (size_t i = 0; i < failedObjectKeys.size(); ++i) {
        cFailedObjKeys[i] = StringToCString(failedObjectKeys[i]);
    }
    if (rc.IsError()) {
        return ToStatusC(rc);
    }
    return StatusC{ datasystem::K_OK, {} };
}

struct StatusC OCGIncreaseRef(ObjectClient_p clientPtr, const char **cObjKeys, const size_t *cObjKeysLen,
                              uint64_t cObjKeysNum, char *cRemoteClientId, size_t cRemoteClientIdLen,
                              char **cFailedObjKeys, size_t *failedObjKeysCount)
{
    datasystem::TraceGuard traceGuard = datasystem::Trace::Instance().SetTraceUUID();
    datasystem::AccessRecorder accessPoint(datasystem::AccessRecorderKey::DS_OBJECT_CLIENT_GINCREASEREF);
    datasystem::RequestParam reqParam;
    StatusC rc = OCExecuteGIncreaseRef(clientPtr, cObjKeys, cObjKeysLen, cObjKeysNum, cRemoteClientId,
                                       cRemoteClientIdLen, cFailedObjKeys, failedObjKeysCount, &reqParam);
    accessPoint.Record(rc.code, "0", reqParam, rc.errMsg);
    return rc;
}

struct StatusC OCExecuteGDecreaseRef(ObjectClient_p clientPtr, const char **cObjKeys, const size_t *cObjKeysLen,
                                     uint64_t cObjKeysNum, char *cRemoteClientId, size_t cRemoteClientIdLen,
                                     char **cFailedObjKeys, size_t *failedObjKeysCount,
                                     datasystem::RequestParam *reqParam)
{
    std::string errorMsg;
    CheckNullptr(clientPtr, "clientPtr", errorMsg);
    CheckNullptr(cObjKeys, "cObjKeys", errorMsg);
    CheckNullptr(reqParam, "reqParam", errorMsg);
    CheckNullptr(cObjKeys, "cObjKeys", errorMsg);
    if (!errorMsg.empty()) {
        return MakeStatusC(datasystem::K_INVALID, errorMsg);
    }
    // cRemoteClientId is an optional parameter and may be nullptr.
    std::string remoteClientId;
    if (cRemoteClientId != nullptr) {
        remoteClientId = std::string(cRemoteClientId, cRemoteClientIdLen);
    }
    (*reqParam).objectKey = datasystem::objectKeysToString(cObjKeys, cObjKeysNum);
    auto client = reinterpret_cast<std::shared_ptr<datasystem::object_cache::ObjectClientImpl> *>(clientPtr);
    std::vector<std::string> failedObjectKeys;
    std::vector<std::string> objKeysVec = GetObjKeysVector(cObjKeys, cObjKeysLen, cObjKeysNum);
    datasystem::Status rc = (*client)->GDecreaseRef(objKeysVec, failedObjectKeys, remoteClientId);
    // Even with errors we can have partial results
    // send the results back as const char
    *failedObjKeysCount = failedObjectKeys.size();
    for (size_t i = 0; i < failedObjectKeys.size(); ++i) {
        cFailedObjKeys[i] = StringToCString(failedObjectKeys[i]);
    }
    if (rc.IsError()) {
        return ToStatusC(rc);
    }
    return StatusC{ datasystem::K_OK, {} };
}

struct StatusC OCDeccreaseRef(ObjectClient_p clientPtr, const char **cObjKeys, const size_t *cObjKeysLen,
                              uint64_t cObjKeysNum, char *cRemoteClientId, size_t cRemoteClientIdLen,
                              char **cFailedObjKeys, size_t *failedObjKeysCount)
{
    datasystem::TraceGuard traceGuard = datasystem::Trace::Instance().SetTraceUUID();
    datasystem::AccessRecorder accessPoint(datasystem::AccessRecorderKey::DS_OBJECT_CLIENT_GDECREASEREF);
    datasystem::RequestParam reqParam;
    StatusC rc = OCExecuteGDecreaseRef(clientPtr, cObjKeys, cObjKeysLen, cObjKeysNum, cRemoteClientId,
                                       cRemoteClientIdLen, cFailedObjKeys, failedObjKeysCount, &reqParam);
    accessPoint.Record(rc.code, "0", reqParam, rc.errMsg);
    return rc;
}

struct StatusC OCExecuteReleaseGRefs(ObjectClient_p clientPtr, char *cRemoteClientId, size_t cRemoteClientIdLen,
                                     datasystem::RequestParam *reqParam)
{
    std::string errorMsg;
    CheckNullptr(clientPtr, "clientPtr", errorMsg);
    CheckNullptr(cRemoteClientId, "cRemoteClientId", errorMsg);
    CheckNullptr(reqParam, "reqParam", errorMsg);
    if (!errorMsg.empty()) {
        return MakeStatusC(datasystem::K_INVALID, errorMsg);
    }
    auto client = reinterpret_cast<std::shared_ptr<datasystem::object_cache::ObjectClientImpl> *>(clientPtr);
    std::string remoteClientId(cRemoteClientId, cRemoteClientIdLen);
    datasystem::Status rc = (*client)->ReleaseGRefs(remoteClientId);
    if (rc.IsError()) {
        return ToStatusC(rc);
    }
    return StatusC{ datasystem::K_OK, {} };
}

struct StatusC OCReleaseGRefs(ObjectClient_p clientPtr, char *cRemoteClientId, size_t cRemoteClientIdLen)
{
    datasystem::TraceGuard traceGuard = datasystem::Trace::Instance().SetTraceUUID();
    datasystem::AccessRecorder accessPoint(datasystem::AccessRecorderKey::DS_OBJECT_CLIENT_RELEASEGREFS);
    datasystem::RequestParam reqParam;
    StatusC rc = OCExecuteReleaseGRefs(clientPtr, cRemoteClientId, cRemoteClientIdLen, &reqParam);
    accessPoint.Record(rc.code, "0", reqParam, rc.errMsg);
    return rc;
}

struct StatusC GetObjMetaInfo(ObjectClient_p clientPtr, const char *cTenantId, const size_t cTenantIdLen,
                              const char **cObjKeys, const size_t *cObjKeysLen, const size_t cObjNum, size_t *cObjSizes,
                              char ***cLocations, size_t *cLocNumPerObj, int *cLocationNum)
{
    std::string errorMsg;
    CheckNullptr(clientPtr, "clientPtr", errorMsg);
    CheckNullptr(cTenantId, "cTenantId", errorMsg);
    CheckNullptr(cObjKeys, "cObjKeys", errorMsg);
    CheckNullptr(cObjKeysLen, "cObjKeysLen", errorMsg);
    CheckNullptr(cObjSizes, "cObjSizes", errorMsg);
    CheckNullptr(cLocations, "cLocations", errorMsg);
    CheckNullptr(cLocNumPerObj, "cLocNumPerObj", errorMsg);
    CheckNullptr(cLocationNum, "cLocationNum", errorMsg);
    if (!errorMsg.empty()) {
        return MakeStatusC(datasystem::K_INVALID, errorMsg);
    }
    std::string tenantId(cTenantId, cTenantIdLen);
    std::vector<std::string> objKeysVec = GetObjKeysVector(cObjKeys, cObjKeysLen, cObjNum);
    std::vector<datasystem::ObjMetaInfo> objMetas;
    auto client = reinterpret_cast<std::shared_ptr<datasystem::object_cache::ObjectClientImpl> *>(clientPtr);
    datasystem::Status rc = (*client)->GetObjMetaInfo(tenantId, objKeysVec, objMetas);
    if (rc.IsError()) {
        return ToStatusC(rc);
    }
    int totalNum = 0;
    for (const auto &objMeta : objMetas) {
        totalNum += objMeta.locations.size();
    }
    *cLocationNum = totalNum;
    *cLocations = MakeCharsArray(totalNum);
    if (*cLocations == nullptr) {
        return MakeStatusC(datasystem::K_RUNTIME_ERROR, "failed to allocate memory");
    }
    int pos = 0;
    for (size_t i = 0; i < objMetas.size(); i++) {
        cObjSizes[i] = objMetas[i].objSize;
        cLocNumPerObj[i] = objMetas[i].locations.size();
        for (auto &loc : objMetas[i].locations) {
            (*cLocations)[pos++] = StringToCString(loc);
        }
    }

    return StatusC{ datasystem::K_OK, {} };
}