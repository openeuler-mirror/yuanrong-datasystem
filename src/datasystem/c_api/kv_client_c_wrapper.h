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

#ifndef DATASYSTEM_KV_CLIENT_C_WRAPPER_H
#define DATASYSTEM_KV_CLIENT_C_WRAPPER_H

#include <stddef.h>
#include <stdint.h>

#include "datasystem/c_api/status_definition.h"

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @brief This wrapper provides a standard C api to invoke calls into the ObjectClientImpl
 * It wraps various operations over the ObjectClientImpl, denoted by the type KVClient_p
 */
typedef void *KVClient_p;  // The main type/handle for interacting with the kv client

/**
 * @brief Creates and returns a handle for a kv client.  This lightweight call only creates the instance
 * of the connection handle.  It does not initialize an actual connection yet. See SCConnectWorker() method for the
 * connection initialization.
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
 * @return Return the pointer of KVClient.
 */
KVClient_p KVCreateClient(const char *cWorkerHost, const int workerPort, const int timeOut, const char *token,
                          size_t tokenLen, const char *clientPublicKey, size_t cClientPublicKeyLen,
                          const char *clientPrivateKey, size_t clientPrivateKeyLen, const char *serverPublicKey,
                          size_t cServerPublicKeyLen, const char *accessKey, size_t cAccessKeyLen,
                          const char *secretKey, size_t secretKeyLen, const char *tenantId, size_t cTenantIdLen,
                          const char *enableCrossNodeConnection);

/**
 * @brief Executes the initialization of the connection to the worker
 * @param[in] clientPtr The instance of the kv client to connect with
 * @return status of the call
 */
struct StatusC SCConnectWorker(KVClient_p clientPtr);

/**
 * @brief Update the access key and secret key for client.
 * @param[in] clientPtr The pointer of ObjectClient.
 * @param[in] accessKey The access key for AK/SK authorize.
 * @param[in] cAccessKeyLen The length of cAccessKey
 * @param[in] secretKey The secret key for AK/SK authorize.
 * @param[in] cSecretKeyLen The secret key length.
 * @return status of the call
 */
struct StatusC SCUpdateAkSk(KVClient_p clientPtr, const char *cAccessKey, size_t cAccessKeyLen, const char *cSecretKey,
                            size_t cSecretKeyLen);

/**
 * @brief Frees the kv client and releases all associated resources with this handle
 * @param[in] clientPtr The pointer of KVClient.
 */
void SCFreeClient(KVClient_p clientPtr);

/**
 * @brief Sets a value for the given key
 * @param[in] clientPtr The pointer of KVClient.
 * @param[in] cKey The key used for setting the value
 * @param[in] keyLen The length of key to set
 * @param[in] cVal The value to set
 * @param[in] valLen The length of value to set
 * @param[in] cWriteMode The cWriteMode param specifies the write mode. The two modes must be "NONE_L2_CACHE" or
 * "WRITE_THROUGH_L2_CACHE".
 * @param[in] ttlSecond If the value is greater than 0, the data will be deleted automatically after expired,
 *                      if set to 0, the data need to be manually deleted.
 * @param[in] cExistenceOpt Whether to check whether the key exists.
 * @return status of the call
 */
struct StatusC SCSet(KVClient_p clientPtr, const char *cKey, size_t keyLen, const char *cVal, size_t valLen,
                     const char *cWriteMode, uint32_t ttlSecond, const char *cExistenceOpt);

/**
 * @brief Generate ObjectKey.
 * @param clientPtr object client ptr.
 * @param[out] cKey Key of object
 * @param[out] keyLen Key size
 * @param[in] cVal The value to set
 * @param[in] valLen The length of value to set
 * @param[in] cWriteMode The cWriteMode param specifies the write mode. The two modes must be "NONE_L2_CACHE" or
 * "WRITE_THROUGH_L2_CACHE".
 * @param[in] ttlSecond If the value is greater than 0, the data will be deleted automatically after expired,
 *                      if set to 0, the data need to be manually deleted.
 * @param[in] cExistenceOpt Whether to check whether the key exists.
 * @return The key of object.
 */
struct StatusC SCSetValue(KVClient_p clientPtr, char **cKey, size_t *keyLen, const char *cVal, size_t valLen,
                          const char *cWriteMode, uint32_t ttlSecond, const char *cExistenceOpt);

/**
 * @brief Gets a value for the given key.
 * @param[in] clientPtr The pointer of KVClient.
 * @param[in] cKey The key used to identify the value to get
 * @param[in] keyLen The length of key to get
 * @param[in] ctimeoutms The timeout of the get operation.
 * @param[out] cVal The pointer to the char * array of the fetched value. The caller is responsible to free it.
 * @param[out] valLen The length of value to get
 * The returned character array will be null terminated to support ANCI C strings, therefore a length is not given.
 * @return status of the call
 */
struct StatusC SCGet(KVClient_p clientPtr, const char *cKey, const size_t keyLen, uint32_t ctimeoutms, char **cVal,
                     size_t *valLen);

/**
 * @brief Gets values for the given keys
 * @param[in] clientPtr The pointer of KVClient.
 * @param[in] cKeys Array of Keys to get values
 * @param[in] keysLen The length of keys to get
 * @param[in] keysNum Number of Keys
 * @param[in] ctimeoutms The timeout of the get operation.
 * @param[out] cVals The pointer to the char * array of the fetched values.
 * @param[out] valsLen The length of values to get
 * The caller is responsible for allocating string array cVals (len(keys))
 * value strings inside the array will be allocated by function (based on actual size)
 * The caller is responsible to free them.
 * If a Key is invalid empty string with length 0 is returned
 * @return status of the call
 */
struct StatusC SCGetArray(KVClient_p clientPtr, const char **cKeys, const size_t *keysLen, uint64_t keysNum,
                          uint32_t ctimeoutms, char **cVals, size_t *valsLen);

/**
 * @brief Deletes a key and its associated values
 * @param[in] clientPtr The pointer of KVClient.
 * @param[in] cKey The key to delete
 * @param[in] keyLen The length of key
 * @return status of the call
 */
struct StatusC SCDel(KVClient_p clientPtr, const char *cKey, const size_t keyLen);

/**
 * @brief Gets values for the given keys
 * @param[in] clientPtr The pointer of KVClient.
 * @param[in] cKeys Array of Keys to delete
 * @param[in] numObjs Number of Keys
 * @param[out] cFailedKeys The failed delete keys.
 * @param[out] failedCount Number of failed delete keys.
 * @return status of the call
 */
struct StatusC SCDelArray(KVClient_p clientPtr, const char **cKeys, uint64_t numObjs, char **cFailedKeys,
                          uint64_t *failedCount);

/**
 * @brief Generate a key with worker_uuid.
 * @param[in] clientPtr The pointer of KVClient.
 * @param[out] key The key with worker_uuid, i.
 * @return size_t The length of the key, if the key fails to be generated, 0 is returned.
 */
size_t SCGenerateKey(KVClient_p clientPtr, char **key);

#ifdef __cplusplus
};
#endif
#endif  // DATASYSTEM_STATE_CACHE_C_WRAPPER_H
