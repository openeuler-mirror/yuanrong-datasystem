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
 * Description: Common functions for both wrappers
 */

#include <cstdlib>

#include <securec.h>

#include "datasystem/c_api/util.h"
#include "datasystem/c_api/utilC.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/context/context.h"

void CheckNullptr(const void *ptr, const std::string &paramName, std::string &errorMsg)
{
    if (ptr == nullptr) {
        errorMsg += paramName + " is nullptr. ";
    }
}

void ExecHandlerIfParamValid(const char *str, std::function<void()> handler)
{
    if (str != nullptr && strlen(str) > 0) {
        handler();
    }
}

void ConstructConnectOptions(const int timeOut, const char *token, size_t tokenLen, const char *clientPublicKey,
                             size_t cClientPublicKeyLen, const char *clientPrivateKey, size_t clientPrivateKeyLen,
                             const char *serverPublicKey, size_t cServerPublicKeyLen, const char *accessKey,
                             size_t cAccessKeyLen, const char *secretKey, size_t secretKeyLen, const char *tenantId,
                             size_t cTenantIdLen, const char *enableCrossNodeConnection,
                             datasystem::ConnectOptions &opts)
{
    if (timeOut > 0) {
        opts.connectTimeoutMs = timeOut;
    }
    ExecHandlerIfParamValid(
        token, [&opts, token, tokenLen]() { opts.token = datasystem::SensitiveValue(token, tokenLen); });
    ExecHandlerIfParamValid(clientPublicKey, [&opts, clientPublicKey, cClientPublicKeyLen]() {
        opts.clientPublicKey = std::string(clientPublicKey, cClientPublicKeyLen);
    });
    ExecHandlerIfParamValid(clientPrivateKey, [&opts, clientPrivateKey, clientPrivateKeyLen]() {
        opts.clientPrivateKey = datasystem::SensitiveValue(clientPrivateKey, clientPrivateKeyLen);
    });
    ExecHandlerIfParamValid(serverPublicKey, [&opts, serverPublicKey, cServerPublicKeyLen]() {
        opts.serverPublicKey = std::string(serverPublicKey, cServerPublicKeyLen);
    });
    ExecHandlerIfParamValid(
        accessKey, [&opts, accessKey, cAccessKeyLen]() { opts.accessKey = std::string(accessKey, cAccessKeyLen); });
    ExecHandlerIfParamValid(secretKey, [&opts, secretKey, secretKeyLen]() {
        opts.secretKey = datasystem::SensitiveValue(secretKey, secretKeyLen);
    });
    ExecHandlerIfParamValid(tenantId,
                            [&opts, tenantId, cTenantIdLen]() { opts.tenantId = std::string(tenantId, cTenantIdLen); });

    // The default value of enableCrossNodeConnection is "false".
    // If the value transferred from the Go is "true", assign a new value.
    if (enableCrossNodeConnection != nullptr && strcmp(enableCrossNodeConnection, "true") == 0) {
        opts.enableCrossNodeConnection = true;
    }
}

StatusC MakeStatusC(const uint32_t &code, const std::string &str)
{
    StatusC statusC{ .code = code, .errMsg = {} };
    int ret = strcpy_s(statusC.errMsg, sizeof(statusC.errMsg), str.c_str());
    if (ret != EOK) {
        LOG(ERROR) << "Error number of strcpy_s: " << ret;
    }
    return statusC;
}

StatusC ToStatusC(datasystem::Status &status)
{
    return MakeStatusC(status.GetCode(), status.GetMsg());
}

char *StringToCString(const std::string &input)
{
    char *ret = nullptr;
    if (input.empty()) {
        return ret;
    }
    size_t destSize = input.size() + 1;
    ret = (char *)malloc(destSize);
    if (ret != nullptr) {
        int err = memcpy_s(ret, destSize, input.data(), input.size());
        if (err == EOK) {
            ret[destSize - 1] = '\0';
        } else {
            free(ret);
            ret = nullptr;
            return nullptr;
        }
    }
    return ret;
}

char *CharToCString(const char *input, const int length)
{
    if (input == nullptr || length <= 0) {
        return nullptr;
    }

    char *ret = (char *)malloc(length + 1);
    if (ret != nullptr) {
        int err = memcpy_s(ret, length, input, length);
        if (err == 0) {
            ret[length] = '\0';
        } else {
            free(ret);
            ret = nullptr;
        }
    }
    return ret;
}

StatusC BufferToCString(datasystem::Optional<datasystem::Buffer> input,
                        const std::shared_ptr<datasystem::ThreadPool> &copyThreads, char **valPointer, size_t *valLen)
{
    size_t destSize = input->GetSize();
    *valPointer = (char *)malloc(destSize);
    if (*valPointer == nullptr) {
        return StatusC{ datasystem::K_RUNTIME_ERROR, "Memory allocation failed" };
    }
    datasystem::Status bufferRLatchRc = input->RLatch();
    if (bufferRLatchRc.IsError()) {
        free(*valPointer);
        *valPointer = nullptr;
        return ToStatusC(bufferRLatchRc);
    }
    *valLen = input->GetSize();

    datasystem::Status rc =
        datasystem::MemoryCopy(reinterpret_cast<uint8_t *>(*valPointer), destSize,
                               static_cast<const uint8_t *>(input->ImmutableData()), *valLen, copyThreads);

    datasystem::Status bufferUnRLatchRc = input->UnRLatch();
    if (bufferUnRLatchRc.IsError()) {
        free(*valPointer);
        *valPointer = nullptr;
        return ToStatusC(bufferUnRLatchRc);
    }
    if (rc.IsError()) {
        free(*valPointer);
        *valPointer = nullptr;
        return ToStatusC(rc);
    }
    return StatusC{ datasystem::K_OK, {} };
}

StatusC InitSetParam(const std::string &writeMode, uint32_t ttlSecond, const std::string &existenceOpt,
                     datasystem::SetParam &setParam)
{
    setParam.ttlSecond = ttlSecond;
    // WriteMode
    if (writeMode == "NONE_L2_CACHE") {
        setParam.writeMode = datasystem::WriteMode::NONE_L2_CACHE;
    } else if (writeMode == "WRITE_THROUGH_L2_CACHE") {
        setParam.writeMode = datasystem::WriteMode::WRITE_THROUGH_L2_CACHE;
    } else if (writeMode == "WRITE_BACK_L2_CACHE") {
        setParam.writeMode = datasystem::WriteMode::WRITE_BACK_L2_CACHE;
    } else if (writeMode == "NONE_L2_CACHE_EVICT") {
        setParam.writeMode = datasystem::WriteMode::NONE_L2_CACHE_EVICT;
    } else {
        return StatusC{ datasystem::K_INVALID,
                        "InvalidParam: The value provided to the cWriteMode parameter should be \"NONE_L2_CACHE\" or "
                        "\"WRITE_THROUGH_L2_CACHE\" or \"WRITE_BACK_L2_CACHE\" or \"NONE_L2_CACHE_EVICT\"." };
    }
    // ExistenceOpt
    if (existenceOpt == "NONE") {
        setParam.existence = datasystem::ExistenceOpt::NONE;
    } else if (existenceOpt == "NX") {
        setParam.existence = datasystem::ExistenceOpt::NX;
    } else {
        return StatusC{
            datasystem::K_INVALID,
            "InvalidParam: The value provided to the ExistenceOpt parameter should be \"NONE\" or \"NX\"."
        };
    }
    return StatusC{ datasystem::K_OK, {} };
}

StatusC InitCreateParam(const std::string &consistencyType, datasystem::object_cache::FullParam &createParam)
{
    if (consistencyType == "PRAM") {
        createParam.consistencyType = datasystem::ConsistencyType::PRAM;
    } else if (consistencyType == "CAUSAL") {
        createParam.consistencyType = datasystem::ConsistencyType::CAUSAL;
    } else {
        return StatusC{ datasystem::K_INVALID,
                        "InvalidParam: The value provided to the consistencyType parameter should be \"PRAM\" or "
                        "\"CAUSAL\"." };
    }
    return StatusC{ datasystem::K_OK, {} };
}

// If we have memory representing pointers, we utilize calloc to clear first.
char **MakeCharsArray(int arrLen)
{
    return (char **)calloc(arrLen, sizeof(char *));
}

size_t *MakeNumArray(int arrLen)
{
    return (size_t *)calloc(arrLen, sizeof(size_t));
}

char *GetCharsAtIdx(char **arr, int idx)
{
    if (arr == nullptr) {
        return nullptr;
    }
    return arr[idx];
}

size_t GetNumAtIdx(size_t *arr, int idx)
{
    if (arr == nullptr) {
        return 0;
    }
    return arr[idx];
}

void SetCharsAtIdx(char **arr, char *str, int idx)
{
    if (arr == nullptr) {
        return;
    }
    arr[idx] = str;
}

void FreeCharsArray(char **arr, int arrLen)
{
    if (arr == nullptr) {
        return;
    }
    for (int i = 0; i < arrLen; i++) {
        if (arr[i] != nullptr) {
            free(arr[i]);
            arr[i] = nullptr;
        }
    }
    free(arr);
}

void FreeNumArray(size_t *arr)
{
    if (arr == nullptr) {
        return;
    }
    free(arr);
}

struct StatusC ContextSetTraceId(const char *cTraceId, size_t traceIdLen)
{
    std::string traceId;
    if (cTraceId != nullptr && traceIdLen > 0) {
        traceId.assign(cTraceId, traceIdLen);
    }

    datasystem::Status rc = datasystem::Context::SetTraceId(traceId);
    if (rc.IsError()) {
        return ToStatusC(rc);
    }
    return StatusC{ datasystem::K_OK, {} };
}

void ContextSetTenantId(const char *cTenantId, size_t tenantIdLen)
{
    if (cTenantId != nullptr) {
        std::string tenantId;
        tenantId.assign(cTenantId, tenantIdLen);
        datasystem::Context::SetTenantId(tenantId);
    }
}

void *CreateObjectClient(const char *cWorkerHost, const int workerPort, const int timeOut, const char *token,
                         size_t tokenLen, const char *clientPublicKey, size_t cClientPublicKeyLen,
                         const char *clientPrivateKey, size_t clientPrivateKeyLen, const char *serverPublicKey,
                         size_t cServerPublicKeyLen, const char *accessKey, size_t cAccessKeyLen, const char *secretKey,
                         size_t secretKeyLen, const char *tenantId, size_t cTenantIdLen,
                         const char *enableCrossNodeConnection)
{
    if (cWorkerHost == nullptr || strlen(cWorkerHost) == 0 || workerPort < 0) {
        LOG(ERROR) << "Host or Port have not been provided correctly";
        return nullptr;
    }
    std::string workerHost(cWorkerHost);
    struct datasystem::ConnectOptions opts = {
        .host = workerHost,
        .port = workerPort,
    };
    ConstructConnectOptions(timeOut, token, tokenLen, clientPublicKey, cClientPublicKeyLen, clientPrivateKey,
                            clientPrivateKeyLen, serverPublicKey, cServerPublicKeyLen, accessKey, cAccessKeyLen,
                            secretKey, secretKeyLen, tenantId, cTenantIdLen, enableCrossNodeConnection, opts);
    auto clientSharedPtr = std::make_shared<datasystem::object_cache::ObjectClientImpl>(opts);
    auto clientUniquePtr =
        std::make_unique<std::shared_ptr<datasystem::object_cache::ObjectClientImpl>>(std::move(clientSharedPtr));
    return reinterpret_cast<void *>(clientUniquePtr.release());
}

struct StatusC ConnectWorker(void *clientPtr)
{
    if (clientPtr == nullptr) {
        return StatusC{ datasystem::K_RUNTIME_ERROR, "The client has not been initialized." };
    }
    auto client = reinterpret_cast<std::shared_ptr<datasystem::object_cache::ObjectClientImpl> *>(clientPtr);
    bool needRollbackState;
    datasystem::Status rc = (*client)->Init(needRollbackState);
    (*client)->CompleteHandler(rc.IsError(), needRollbackState);
    if (rc.IsError()) {
        return ToStatusC(rc);
    }
    return StatusC{ datasystem::K_OK, {} };
}

void FreeClient(void *clientPtr)
{
    auto client = reinterpret_cast<std::shared_ptr<datasystem::object_cache::ObjectClientImpl> *>(clientPtr);
    if (client != nullptr) {
        delete client;
        client = nullptr;
    }
}

struct StatusC ExecuteGetArray(void *clientPtr, const char **cObjKeys, const size_t *cObjKeysLen, uint64_t cObjKeysNum,
                               uint32_t ctimeoutms, char **cVals, size_t *valsLen, size_t *totalSize,
                               datasystem::RequestParam *reqParam)
{
    std::string errorMsg;
    CheckNullptr(clientPtr, "clientPtr", errorMsg);
    CheckNullptr(cObjKeys, "cObjKeys", errorMsg);
    CheckNullptr(cVals, "cVals", errorMsg);
    CheckNullptr(valsLen, "valsLen", errorMsg);
    CheckNullptr(totalSize, "totalSize", errorMsg);
    if (!errorMsg.empty()) {
        return MakeStatusC(datasystem::K_INVALID, errorMsg);
    }
    (*reqParam).objectKey = datasystem::objectKeysToString(cObjKeys, cObjKeysNum);
    (*reqParam).timeout = std::to_string(ctimeoutms);
    auto client = reinterpret_cast<std::shared_ptr<datasystem::object_cache::ObjectClientImpl> *>(clientPtr);
    std::vector<std::string> objsVec = GetObjKeysVector(cObjKeys, cObjKeysLen, cObjKeysNum);
    std::vector<datasystem::Optional<datasystem::Buffer>> buffers;
    datasystem::Status rc = (*client)->Get(objsVec, ctimeoutms, buffers);
    // Even with errors we can have partial results
    // send the results back as const char
    for (size_t i = 0; i < buffers.size(); ++i) {
        if (buffers[i]) {
            StatusC s =
                BufferToCString(std::move(buffers[i]), (*client)->GetMemoryCopyThreadPool(), &cVals[i], &valsLen[i]);
            if (s.code != datasystem::K_OK) {
                LOG(ERROR) << "Converts buffer to c style char * failed, " << s.errMsg;
            }
            *totalSize += valsLen[i];
        }
    }
    return ToStatusC(rc);
}

std::vector<std::string> GetObjKeysVector(const char **cObjKeys, const size_t *cObjKeysLen, uint64_t cObjKeysNum)
{
    std::vector<std::string> objsVec(cObjKeysNum);
    for (size_t i = 0; i < cObjKeysNum; i++) {
        objsVec[i] = std::string(cObjKeys[i], cObjKeysLen[i]);
    }
    return objsVec;
}