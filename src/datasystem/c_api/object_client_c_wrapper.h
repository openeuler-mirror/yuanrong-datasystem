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

#ifndef DATASYSTEM_OBJECT_CLIENT_C_WRAPPER_H
#define DATASYSTEM_OBJECT_CLIENT_C_WRAPPER_H

#include <stddef.h>
#include <stdint.h>

#include "datasystem/c_api/status_definition.h"

#ifdef __cplusplus
extern "C" {
#endif
/**
 * @brief This wrapper provides a standard C api to invoke calls into the ObjectClientImpl
 * It wraps various operations over the ObjectClientImpl, denoted by the type ObjectClient_p
 */
typedef void *ObjectClient_p;  // The main type/handle for interacting with the object cache

/**
 * @brief Creates and returns a handle for a object cache client.  This lightweight call only creates the instance
 * of the connection handle.  It does not initialize an actual connection yet. See OCConnectWorker() method for the
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
 * @return Return the pointer of ObjectClient.
 */
ObjectClient_p OCCreateClient(const char *cWorkerHost, const int workerPort, const int timeOut,
                              const char *clientPublicKey, size_t cClientPublicKeyLen, const char *clientPrivateKey,
                              size_t clientPrivateKeyLen, const char *serverPublicKey, size_t cServerPublicKeyLen,
                              const char *accessKey, size_t cAccessKeyLen, const char *secretKey, size_t secretKeyLen,
                              const char *tenantId, size_t cTenantIdLen, const char *enableCrossNodeConnection);

/**
 * @brief Executes the initialization of the connection to the worker
 * @param[in] clientPtr The instance of the object cache client to connect with
 * @return status of the call
 */
struct StatusC OCConnectWorker(ObjectClient_p clientPtr);

/**
 * @brief Frees the object cache client and releases all associated resources with this handle
 * @param[in] clientPtr The pointer of ObjectClient.
 */
void OCFreeClient(ObjectClient_p clientPtr);

/**
 * @brief Invoke worker client to put an object.
 * @param[in] clientPtr The pointer of ObjectClient.
 * @param[in] cObjKey The objectKey to Put.
 * @param[in] cObjKeysLen Length of each objectKey.
 * @param[in] cObjKeyLen The length of objectKey to Put
 * @param[in] cVal The pointer to the char * array of the fetched value. The caller is responsible to free it.
 * @param[in] valLen The length of value to Put
 * @param[in] cNestedObjectKeys The Objects that depend on cObjKey.
 * @param[in] cNestedObjectKeysNum The Number of Objects
 * @param[in] cWriteMode The cWriteMode param specifies the write mode, which must be "NONE_L2_CACHE" or
 * "WRITE_THROUGH_L2_CACHE".
 * @param[in] cConsistencyType The cConsistencyType param specifies the ConsistencyType mode, which must be "PRAM" or
 * "CAUSAL".
 * @return status of the call
 */
struct StatusC OCPut(ObjectClient_p clientPtr, const char *cObjKey, size_t cObjKeyLen, const char *cVal, size_t cValLen,
                     const char **cNestedObjectKeys, const size_t *cNestedObjKeyLenArray,
                     const size_t cNestedObjectKeysNum, const char *cConsistencyType);

/**
 * @brief Get values for the given objKeys.
 * @param[in] clientPtr The pointer of ObjectClient.
 * @param[in] cObjKeys The array of ObjectKeys to get values
 * @param[in] cObjKeysLen Length of each objectKey.
 * @param[in] objsNum The number of ObjectKeys
 * @param[in] cTimeoutMs The timeout of the get operation.
 * @param[out] cVals The pointer to the char * array of the fetched values.
 * @param[out] valsLen The length of values to get
 * The caller is responsible for allocating string array cVals (len(keys))
 * value strings inside the array will be allocated by function (based on actual size)
 * The caller is responsible to free them.
 * @return status of the call
 */
struct StatusC OCGet(ObjectClient_p clientPtr, const char **cObjKeys, const size_t *cObjKeysLen, uint64_t objsNum,
                     uint32_t cTimeoutMs, char **cVals, size_t *valsLen);

/**
 * @brief Increase the global reference count to objects in the data system.
 * @param[in] clientPtr The pointer of ObjectClient.
 * @param[in] cObjKeys The array of ObjectKeys to get values
 * @param[in] cObjKeysLen Length of each objectKey.
 * @param[in] cObjKeysNum The number of ObjectKeys
 * @param[in] cRemoteClientId The remote client id of the client that outside the cloud. Resolve scenarios that ELB
 * forwards requests to different gateways, resulting in incorrect increase or decrease in reference counts, and
 * system availability after gateway crash.
 * @param[in] cRemoteClientIdLen The length of remote client id
 * @param[out] cFailedObjKeys Increase failed object keys.
 * @param[out] failedObjKeysCount  The number of failed object keys.
 * The caller is responsible for allocating string array cFailedObjKeys
 * cFailedObjKeys strings inside the array will be allocated by function (based on actual size)
 * The caller is responsible to free them.
 * @return status of the call
 */
struct StatusC OCGIncreaseRef(ObjectClient_p clientPtr, const char **cObjKeys, const size_t *cObjKeysLen,
                              uint64_t cObjKeysNum, char *cRemoteClientId, size_t cRemoteClientIdLen,
                              char **cFailedObjKeys, size_t *failedObjKeysCount);

/**
 * @brief Decrease the global reference count to objects in the data system.
 * @param[in] clientPtr The pointer of ObjectClient.
 * @param[in] cObjKeys The array of ObjectKeys to get values
 * @param[in] cObjKeysLen Length of each objectKey.
 * @param[in] cObjKeysNum The number of ObjectKeys
 * @param[in] cRemoteClientId The remote client id of the client that outside the cloud. Resolve scenarios that ELB
 * forwards requests to different gateways, resulting in incorrect increase or decrease in reference counts, and
 * system availability after gateway crash.
 * @param[in] cRemoteClientIdLen The length of remote client id
 * @param[out] cFailedObjKeys Decrease failed object keys.
 * @param[out] failedObjKeysCount  The number of failed object keys.
 * The caller is responsible for allocating string array cFailedObjKeys
 * cFailedObjKeys strings inside the array will be allocated by function (based on actual size)
 * The caller is responsible to free them.
 * @return status of the call
 */
struct StatusC OCDeccreaseRef(ObjectClient_p clientPtr, const char **cObjKeys, const size_t *cObjKeysLen,
                              uint64_t cObjKeysNum, char *cRemoteClientId, size_t cRemoteClientIdLen,
                              char **cFailedObjKeys, size_t *failedObjKeysCount);

/**
 * @brief Release obj Ref of remote client id when remote client that outside the cloud crash.
 * @param[in] clientPtr The pointer of ObjectClient.
 * @param[in] cRemoteClientId The remote client id of the client that outside the cloud.
 * @param[in] cRemoteClientIdLen The length of remote client id
 * @return status of the call
 */
struct StatusC OCReleaseGRefs(ObjectClient_p clientPtr, char *cRemoteClientId, size_t cRemoteClientIdLen);

/**
 * @brief Get meta info of the given objects.
 * @param[in] clientPtr The pointer of ObjectClient.
 * @param[in] cTenantId The tenant that the objs belong to.
 * @param[in] cTenantIdLen Length of tenantId.
 * @param[in] cObjKeys The array of ObjectKeys to get values.
 * @param[in] cObjKeysLen Length of each objectKey.
 * @param[in] cObjNum The number of ObjectKeys.
 * @param[out] cObjSizes The size of object data, 0 if object not found.
 * @param[out] cLocations The locations of the ObjectKeys.
 * @param[out] cLocNumPerObj The number of locations per object.
 * @param[in/out] cLocationNum The number of all object locations.
 * @return status of the call
 */
struct StatusC GetObjMetaInfo(ObjectClient_p clientPtr, const char *cTenantId, const size_t cTenantIdLen,
                              const char **cObjKeys, const size_t *cObjKeysLen, const size_t cObjNum, size_t *cObjSizes,
                              char ***cLocations, size_t *cLocNumPerObj, int *cLocationNum);

#ifdef __cplusplus
};
#endif
#endif  // DATASYSTEM_STATE_CACHE_C_WRAPPER_H
