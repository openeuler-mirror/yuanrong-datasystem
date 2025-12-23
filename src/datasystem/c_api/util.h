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
 * Description: some common CPP functions for the wrapper
 */
#ifndef DATASYSTEM_UTIL_CPP_CLIENT_C_WRAPPER_H
#define DATASYSTEM_UTIL_CPP_CLIENT_C_WRAPPER_H

#include "datasystem/client/object_cache/object_client_impl.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/object/buffer.h"
#include "datasystem/object_client.h"
#include "datasystem/kv_client.h"
#include "datasystem/utils/optional.h"
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"
#include "status_definition.h"

/**
 * @brief Check whether the parameter is nullptr.
 * @param[in] ptr Parameter value to be verified.
 * @param[in] paramName Parameter Name.
 * @param[in/out] errorMsg Error information.
 */
void CheckNullptr(const void *ptr, const std::string &paramName, std::string &errorMsg);

/**
 * @brief Check the parameter. If the parameter is valid, execute the user-defined function.
 * @param[in] str Parameter value to be verified.
 * @param[in/out] handler The User-defined execution function.
 */
void ExecHandlerIfParamValid(const char *str, std::function<void()> handler);

/**
 * @brief Constructs the ConnectOptions object based on the parameter values transferred by the Go interface.
 * @param[in] timeOut Timeout in milliseconds and the range is [0, INT32_MAX].
 * @param[in] clientPublicKey The rpc public key of client
 * @param[in] cClientPublicKeyLen The length of cClientPublicKey
 * @param[in] clientPrivateKey The rpc private key of client
 * @param[in] clientPrivateKeyLen The rpc private key length
 * @param[in] serverPublicKey The rpc public key of worker
 * @param[in] cServerPublicKeyLen The length of cServerPublicKey
 * @param[in] accessKey The access key for AK/SK authorize.
 * @param[in] cAccessKeyLen The length of cAccessKey
 * @param[in] secretKey The secret key for AK/SK authorize.
 * @param[in] secretKeyLen The secret key length.
 * @param[in] tenantId The tenant ID.
 * @param[in] cTenantIdLen The length of cTenantId
 * @param[in] tenantId The tenant ID.
 * @param[in] enableCrossNodeConnection Indicates whether the client can connect to the standby node.
 * @param[out] opts The ConnectOptions object.
 */
void ConstructConnectOptions(const int timeOut, const char *token, size_t tokenLen, const char *clientPublicKey,
                             size_t cClientPublicKeyLen, const char *clientPrivateKey, size_t clientPrivateKeyLen,
                             const char *serverPublicKey, size_t cServerPublicKeyLen, const char *accessKey,
                             size_t cAccessKeyLen, const char *secretKey, size_t secretKeyLen, const char *tenantId,
                             size_t cTenantIdLen, const char *enableCrossNodeConnection,
                             datasystem::ConnectOptions &opts);

/**
 * @brief Make c struct Status
 * @param[in] code The status code.
 * @param[in] str The status message.
 * @return C style status contains an int for status code and char[] for error message
 */
StatusC MakeStatusC(const uint32_t &code, const std::string &str);

/**
 * @brief Converts C++ Status to c struct Status
 * @param[in] input status.
 * @return C style status contains an int for status code and char[] for error message
 */
StatusC ToStatusC(datasystem::Status &status);

/**
 * @brief Converts C++ string to c style char *(also allocates memory)
 * @param[in] input string.
 * @return Pointer to char array with null termination.
 */
char *StringToCString(const std::string &input);

/**
 * @brief Converts C++ string to c style char *(also allocates memory)
 * @param[in] input char[].
 * @param [in] length the length of char[].
 * @return Pointer to char array with null termination.
 */
char *CharToCString(const char *input, const int length);

/**
 * @brief Converts buffer to c style char *(also allocates memory)
 * @param[in] input buffer.
 * @param[in] copyThreads The memory copy threadPool.
 * @param[out] valPointer The value.
 * @param[out] valLen The value length.
 * @return C style status contains an int for status code and char[] for error message
 */
StatusC BufferToCString(datasystem::Optional<datasystem::Buffer> input,
                        const std::shared_ptr<datasystem::ThreadPool> &copyThreads, char **valPointer, size_t *valLen);

/**
 * @brief Set setParam.
 * @param[in] writeMode The write mode of SetParam.
 * @param[in] ttlSecond The ttl of SetParam.
 * @param[in] cExistenceOpt Whether to check whether the key exists.
 * @param[out] setParam The set parameters.
 * @return C style status contains an int for status code and char[] for error message
 */
StatusC InitSetParam(const std::string &writeMode, uint32_t ttlSecond, const std::string &existenceOpt,
                     datasystem::SetParam &setParam);

/**
 * @brief Set CreateParam.
 * @param[in] writeMode The write mode of CreateParam.
 * @param[in] consistencyType The consistency type of CreateParam.
 * @param[out] createParam The create parameters.
 * @return C style status contains an int for status code and char[] for error message
 */
StatusC InitCreateParam(const std::string &consistencyType, datasystem::object_cache::FullParam &createParam);

/**
 * @brief Creates and returns a handle for a Stateclient/ObjectClient.
 * @param[in] cWorkerHost A c string that contains the hostname for the worker
 * @param[in] workerPort The port number of the worker
 * @param[in] timeOut Timeout in milliseconds and the range is [0, INT32_MAX].
 * @param[in] clientPublicKey The rpc public key of client
 * @param[in] cClientPublicKeyLen The length of cClientPublicKey
 * @param[in] clientPrivateKey The rpc private key of client
 * @param[in] clientPrivateKeyLen The rpc private key length
 * @param[in] serverPublicKey The rpc public key of worker
 * @param[in] cServerPublicKeyLen The length of cServerPublicKey
 * @param[in] accessKey The access key for AK/SK authorize.
 * @param[in] cAccessKeyLen The length of cAccessKey
 * @param[in] secretKey The secret key for AK/SK authorize.
 * @param[in] secretKeyLen The secret key length.
 * @param[in] tenantId The tenant ID.
 * @param[in] cTenantIdLen The length of cTenantId
 * @param[in] enableCrossNodeConnection Indicates whether the client can connect to the standby node.
 * @return Return the pointer of StateClient/ObjectClient.
 */
void *CreateObjectClient(const char *cWorkerHost, const int workerPort, const int timeOut, const char *token,
                         size_t tokenLen, const char *clientPublicKey, size_t cClientPublicKeyLen,
                         const char *clientPrivateKey, size_t clientPrivateKeyLen, const char *serverPublicKey,
                         size_t cServerPublicKeyLen, const char *accessKey, size_t cAccessKeyLen, const char *secretKey,
                         size_t secretKeyLen, const char *tenantId, size_t cTenantIdLen,
                         const char *enableCrossNodeConnection);

/**
 * @brief Executes the initialization of the connection to the worker
 * @param[in] clientPtr The pointer of StateClient/ObjectClient to connect worker.
 * @return status of the call
 */
struct StatusC ConnectWorker(void *clientPtr);

/**
 * @brief Frees the StateClient/ObjectClient and releases all associated resources with this handle
 * @param[in] clientPtr The pointer of StateClient/ObjectClient.
 */
void FreeClient(void *clientPtr);

/**
 * @brief Construct the objKeys of the std::vector<std::string> type based on the parameters transferred from Go.
 * @param[in] cObjKeys Array of ObjKeys to get values
 * @param[in] cObjKeysLen Length of each objectKey.
 * @param[in] numObjs Number of Keys
 * @return std::vector<std::string> objKeys
 */
std::vector<std::string> GetObjKeysVector(const char **cObjKeys, const size_t *cObjKeysLen, uint64_t cObjKeysNum);

/**
 * @brief Execute get array operation.
 * @param[in] clientPtr The pointer of StateClient.
 * @param[in] cObjKeys Array of ObjKeys to get values
 * @param[in] cObjKeysLen Length of each objectKey.
 * @param[in] numObjs Number of Keys
 * @param[in] ctimeoutms The timeout of the get operation.
 * @param[out] cVals The pointer to the char * array of the fetched values.
 * @param[out] valsLen The length of values to get
 * @param[out] totalSize The size of values to get
 * @param[out] reqParam Request parameter of the client, which records the request information of the client.
 * @return status of the call
 */
struct StatusC ExecuteGetArray(void *clientPtr, const char **cObjKeys, const size_t *cObjKeysLen, uint64_t cObjKeysNum,
                               uint32_t ctimeoutms, char **cVals, size_t *valsLen, size_t *totalSize,
                               datasystem::RequestParam *reqParam);
#endif