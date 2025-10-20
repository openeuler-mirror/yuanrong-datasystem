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
 * Description: Defines the worker service Get process.
 */
#ifndef DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_GET_IMPL_H
#define DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_GET_IMPL_H

#include <unordered_set>

#include "datasystem/utils/status.h"
#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/rpc/rpc_message.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/protos/object_posix.service.rpc.pb.h"
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"
#include "datasystem/worker/object_cache/object_kv.h"
#include "datasystem/worker/object_cache/service/worker_oc_service_crud_common_api.h"

namespace datasystem {
namespace object_cache {

class WorkerOcServiceGetImpl : public WorkerOcServiceCrudCommonApi,
                               public std::enable_shared_from_this<WorkerOcServiceGetImpl> {
public:
    WorkerOcServiceGetImpl(WorkerOcServiceCrudParam &initParam, EtcdClusterManager *etcdCM, EtcdStore *etcdStore,
                           std::shared_ptr<ThreadPool> memCpyThreadPool, std::shared_ptr<ThreadPool> threadPool,
                           std::shared_ptr<AkSkManager> akSkManager, HostPort localAddress);

    /**
     * @brief Deal with the request for object data from client.
     * @param[in] serverApi The WriterReader in server side which holds unary rpc socket.
     * @return Status of the call.
     */
    Status Get(std::shared_ptr<ServerUnaryWriterReader<GetRspPb, GetReqPb>> &serverApi);

    /**
     * @brief Process get request for these objects not exist in local, query object meta from master, then get
     * object data from remote.
     * @param[in] objectsNeedGetRemote read key info, contain offset, size, objKey.
     * @param[in] objectKeys These objects not exist in local.
     * @param[in] subTimeout The get request timeout for subscribe.
     * @param[out] failedIds The failed object key list.
     * @param[in] needRetryIds retry keys info, contain offset, size, objKey.
     * @param[out] needRetryIds Need retry get id list.
     * @param[in] request Get request instance.
     * @return Status of the call.
     */
    Status ProcessObjectsNotExistInLocal(const std::vector<ReadKey> &objectsNeedGetRemote, const int64_t subTimeout,
                                         std::unordered_set<std::string> &failedIds, std::vector<ReadKey> &needRetryIds,
                                         const std::shared_ptr<GetRequest> &request = nullptr);

    /**
     * @brief Get object data from remote cache (remote worker or redis) based on object meta.
     * @param[in] readKey read key info, contain offset, size, objKey.
     * @param[in] queryMeta The object meta info contains remote address and data size.
     * @param[in] payloads  Get payloads that contains object data.
     * @return Status of the call.
     */
    Status GetObjectFromAnywhere(const ReadKey &readKey, const master::QueryMetaInfoPb &queryMeta,
                                 std::vector<RpcMessage> &payloads);

    /**
     * @brief Get data from L2Cache for primary copy.
     * @param[in] objectKey Object key.
     * @param[in] version Expected object version.
     * @param[in] safeEntry The safe object entry.
     * @return Status of the call.
     */
    Status GetDataFromL2CacheForPrimaryCopy(const std::string &objectKey, uint64_t version,
                                            std::shared_ptr<SafeObjType> &safeEntry);

    /**
     * @brief Set Query info.
     * @param req The request info to master
     * @param objectKeys The key of objects.
     * @param masterAddr master address
     * @param redirect if need redirect
     */
    void SetQueryMetaInfo(master::QueryMetaReqPb &req, const std::vector<std::string> &objectKeys,
                          const std::string &masterAddr, bool redirect, bool isFromOtherAz);

    /**
     * @brief Check is in remote get object or not.
     * @param[in] objectKey Object key.
     * @return True if object is in remote get.
     */
    bool IsInRemoteGetObject(const std::string &objectKey);

    /**
     * @brief Check and set status K_WORKER_PULL_OBJECT_NOT_FOUND to retry.
     * @param[in] meta object meta
     * @param[in] address object location addr
     * @param[in] entry entry of object
     * @param[in] checkConnectStatus worker is connect status
     * @param[in] status status after call.
     */
    void CheckAndReturnPullNotFoundForRetry(const ObjectMetaPb &meta, const std::string &address, SafeObjType &entry,
                                            Status &checkConnectStatus, Status &status);

    /**
     * @brief Get meta info of the input objects
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status GetObjMetaInfo(const GetObjMetaInfoReqPb &req, GetObjMetaInfoRspPb &resp);

    /**
     * @brief Get meta info of the input objects
     * @param[in] req The rpc req protobuf.
     * @param[out] rsp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status QuerySize(const QuerySizeReqPb &req, QuerySizeRspPb &rsp);

    /**
     * @brief Check whether the keys exist in the data system.
     * @param[in] req The exist request protobuf.
     * @param[in] rsp The exist response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status Exist(const ExistReqPb &req, ExistRspPb &rsp);

    /**
     * @brief Get device meta info of the keys.
     * @param[in] req The exist request protobuf.
     * @param[in] rsp The exist response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status GetMetaInfo(const GetMetaInfoReqPb &req, GetMetaInfoRspPb &rsp);

    /**
     * @brief Check whether the specified object key is still in the Get process.
     * @param[in] key The object key to check.
     * @return true if the key is still in the Get process and should not be deleted; false otherwise.
     */
    bool IsObjectInGetProcess(const std::string &key);

private:
    using ObjectKeysQueryMetaFailed = std::tuple<std::unordered_set<std::string>, std::unordered_set<std::string>>;

    struct QueryMetadataFromMasterResult {
        std::vector<master::QueryMetaInfoPb> queryMetas;
        std::vector<RpcMessage> payloads;
        std::map<std::string, uint64_t> absentObjectKeysWithVersion;
    };

    struct BatchQueryMetaResult {
        datasystem::master::QueryMetaRspPb rsp;
        std::unordered_set<std::string> failedKeys;
        std::vector<RpcMessage> payloads;
    };

    /**
     * @brief Get map of objectKeys grouped by master.
     * @param[in] objectKeys The vector of objectkeys.
     * @param[out] result The meta info result of objects.
     * @param[out] lastRc The last status.
     * @return K_OK on success; the error code otherwise.
     */
    Status GetMapOfObjectKeys(const std::vector<std::basic_string<char>> &objectKeys,
                              std::unordered_map<std::string, master::ObjectLocationInfoPb> &result, Status &lastRc);

    /**
     * @brief Process a get request from client.
     * @param[in] objectKeys The object keys of the get request.
     * @param[in] offsetInfos The offest info for get request.
     * @param[in] serverApi The WriterReader in server side which holds unary rpc socket.
     * @param[in] subTimeout The timeout for subscribe of the get request.
     * @param[in] clientId The client making this request.
     * @return Status of the call.
     */
    Status ProcessGetObjectRequest(const std::vector<std::string> &objectKeys,
                                   const std::unordered_map<std::string, OffsetInfo> &offsetInfos,
                                   std::shared_ptr<::datasystem::ServerUnaryWriterReader<GetRspPb, GetReqPb>> serverApi,
                                   const int64_t subTimeout, const std::string &clientId,
                                   std::shared_ptr<AccessRecorder> accessRecorderPoint, const GetReqPb &getReqPb);

    /**
     * @brief Get one object from local.
     * @param[in] offsetInfos The offset info of the get request.
     * @param[out] request The GetRequest.
     * @param[out] objectsNeedGetRemote These objects not exist in local.
     */
    void TryGetObjectFromLocal(const std::unordered_map<std::string, OffsetInfo> &offsetInfos,
                               std::shared_ptr<GetRequest> &request, std::vector<ReadKey> &objectsNeedGetRemote);

    /**
     * @brief Get one object from remote.
     * @param[in] subTimeout The get request timeout for subscribe.
     * @param[out] request The GetRequest.
     * @param[in] objectsNeedGetRemote These objects not exist in local.
     * @return Status of the call
     */
    Status TryGetObjectFromRemote(int64_t subTimeout, std::shared_ptr<GetRequest> &request,
                                  std::vector<ReadKey> &objectsNeedGetRemote);

    /**
     * @brief Preprocess for get one object.
     * @param[in] objectKey The object key.
     * @param[out] request The request object.
     * @param[out] objectsNeedGetRemote These objects not exist in local.
     * @param[out] localExistKeys vector of keys which exists in local mem.
     * @return Status of the call
     */
    Status PreProcessGetObject(const ReadKey &objectKey, std::shared_ptr<GetRequest> &request,
                               std::vector<ReadKey> &objectsNeedGetRemote, std::vector<std::string> &localExistKeys);

    /**
     * @brief Preprocess for get one object from memory, in this case, we only hold RLock instead of WLock, because the
     * WLock maybe block by other business process, for example, in WriteBack mode, the async thread send data to
     * L2cache will hold the RLock util the data finish send.
     * @param[in] objectKey The object key.
     * @param[out] request The request object.
     * @param[out] objectsNeedGetRemote These objects not exist in local.
     * @param[out] objIsValidInMem whether the object is valid in memory.
     * @param[out] localExistKeys vector of keys which exists in local mem.
     * @return Status of the call
     */
    Status RLockGetObjectFromMem(const ReadKey &objectKey, std::shared_ptr<GetRequest> &request,
                                 std::vector<ReadKey> &objectsNeedGetRemote, bool &objIsValidInMem,
                                 std::vector<std::string> &localExistKeys);

    /**
     * @brief Try to get object from primary copy worker.
     * @param[in] primaryAddress The primary copy worker address.
     * @param[in] dataSize The object data size.
     * @param[out] objectKV The object meta.
     * @param[out] objectsNeedGetRemote These objects not exist in local.
     * @return Status of the call
     */
    Status TryGetObjectsFromPrimaryWorker(const std::string &primaryAddress, uint64_t dataSize, ReadObjectKV &objectKV,
                                          std::vector<ReadKey> &objectsNeedGetRemote);

    /**
     * @brief Get object data from remote worker based on object meta.
     * @param[in] address The remote worker address.
     * @param[in] primaryAddress The remote primary worker address.
     * @param[in] dataSize The object data size.
     * @param[out] entry The reserved and locked safe object.
     * @return Status of the call.
     */
    Status GetObjectFromRemoteWorkerWithoutDump(const std::string &address, const std::string &primaryAddress,
                                                uint64_t dataSize, ReadObjectKV &objectKV);

    /**
     * @brief Get object data from remote worker based on object meta.
     * @param[in] address The remote worker address.
     * @param[in] primaryAddress The remote primary worker address.
     * @param[in] updateLocation Determines whether to update the location information of the master.
     * @param[in] dataSize The object data size.
     * @param[out] objectKV The reserved and locked safe object and its corresponding objectKey.
     * @return Status of the call.
     */
    Status GetObjectFromRemoteWorkerAndDump(const std::string &address, const std::string &primaryAddress,
                                            bool updateLocation, uint64_t dataSize, ReadObjectKV &objectKV);

    /**
     * @brief Helper function to process entry and sync metadata after successful pull.
     * @param[in] updateLocation Determines whether to update the location information of the master.
     * @param[out] objectKV The reserved and locked safe object and its corresponding objectKey.
     * @return Status of the call.
     */
    Status ProcessObjectEntryAndSyncMetadata(bool updateLocation, ReadObjectKV &objectKV);

    /**
     * @brief Create a copy object metadata to master.
     * @param[in/out] objectKV The object to be sealed and its corresponding objectKey.
     * @return Status of the call.
     */
    Status CreateCopyMetaToMaster(ObjectKV &objectKV);

    /**
     * @brief Update location to master.
     * @param[in] objectKey Object key.
     * @param[in] objectKV The reserved and locked safe object and its corresponding objectKey.
     * @return Status of the call.
     */
    Status UpdateLocation(const std::string &objectKey, ObjectKV &objectKV);

    /**
     * @brief Helper function to allocate aggregated memory for objects at Batch Get.
     * @param[in] metas The batched object meta info contains data size.
     * @param[in] lockedEntries The object lock entries.
     * @param[out] shmOwners The allocated shared memory chunks.
     * @param[out] shmIndexMapping The object key to shmOwners index mapping.
     * @return Status of the call.
     */
    Status AggregateAllocateHelper(const std::list<ObjectMetaPb *> &metas,
                                   std::map<std::string, std::pair<std::shared_ptr<SafeObjType>, bool>> &lockedEntries,
                                   std::vector<std::shared_ptr<ShmOwner>> &shmOwners,
                                   std::vector<uint32_t> &shmIndexMapping);

    /**
     * @brief Pull object data from remote worker.
     * @note The request protobuf needs to contain data_size and urma_info fields.
     * @param[in] dataSize The object data size.
     * @param[in] kv The reserved and locked safe object and its corresponding objectKey.
     * @param[in] reqPb The remote GetObject rpc req protobuf.
     * @param[out] shmUnitAllocated did memory allocated during this call.
     * @param[in] shmOwner The allocated shared memory chunks.
     * @return Status of the call.
     */
    template <typename Req>
    Status PrepareGetRequestHelper(uint64_t dataSize, ReadObjectKV &objectKV, Req &reqPb, bool &shmUnitAllocated,
                                   std::shared_ptr<ShmOwner> shmOwner = nullptr);

    /**
     * @brief Pull object data from remote worker.
     * @param[in] address The remote worker address.
     * @param[in] dataSize The object data size.
     * @param[out] kv The reserved and locked safe object and its corresponding objectKey.
     * @return Status of the call.
     */
    Status PullObjectDataFromRemoteWorker(const std::string &address, uint64_t dataSize, ReadObjectKV &kv);

    /**
     * @brief Part two of bypassing memory copy and put remote payload directly into shared memory or private memory
     * @param[in] objKV The reserved and locked safe object and its corresponding objectKey.
     * @param[in] clientApi ClientUnaryWriterReader
     * @param[in] rspPb remote reply
     * @return Status of the call.
     */
    Status RetrieveRemotePayload(
        ReadObjectKV &objKV,
        std::unique_ptr<ClientUnaryWriterReader<GetObjectRemoteReqPb, GetObjectRemoteRspPb>> &clientApi,
        GetObjectRemoteRspPb &rspPb);

    /**
     * @brief Part two of bypassing memory copy and put remote payload directly into shared memory or private memory
     * @param[in] completeDataSize The object size.
     * @param[in] objKV The reserved and locked safe object and its corresponding objectKey.
     * @param[in] payloads The remote payload.
     * @param[in/out] payloadIndex The payload cursor index.
     * @param rspPb remote reply
     * @return Status of the call.
     */
    Status BatchGetRetrieveRemotePayload(uint64_t completeDataSize, ReadObjectKV &objectKV,
                                         std::vector<RpcMessage> &payloads, uint64_t &payloadIndex);

    /**
     * @brief Attempt to get object from local before query meta.
     * @param[in out] lockedEntries Object lock entries.
     */
    void AttemptGetObjectsLocally(const std::map<std::string, ReadKey> &readKeys,
                                  std::map<std::string, std::pair<std::shared_ptr<SafeObjType>, bool>> &lockedEntries);

    /**
     * @brief Query the metadata of the specified objects in the master.
     * @param[in] objectKeys The object key list to be published.
     * @param[in] subTimeout Request timeout for subscribe.
     * @param[out] resulst The meta info result of objects.
     * @param[in] queryEtcdMeta  Whether to query etcd meta.
     * @return Status of the call.
     */
    Status QueryMetadataFromMaster(const std::vector<std::string> &objectKeys, uint64_t subTimeout,
                                   QueryMetadataFromMasterResult &result, bool queryEtcdMeta = true);

    /**
     * @brief Query the metadata of the specified objects in the redirect master.
     * @param[in] rsp Response of redirect.
     * @param[in] subTimeout Request timeout for subscribe.
     * @return Status of the call.
     */
    Status QueryMetadataFromRedirectMaster(master::QueryMetaRspPb &rsp, uint64_t subTimeout, bool isFromOtherAz,
                                           std::vector<RpcMessage> &payloads);

    /**
     * @brief Query metadata from different AZ in the etcd.
     * @param[in] objectKeys The object keys need to get from ETCD.
     * @param[in] workerId The workerId which objects belong to. Be empty if we get objects meta from hash table.
     * @param[in] getLocalAz If need query from local AZ.
     * @param[out] queryMetas The vector stored meta info.
     */
    Status QueryMetaDataFromEtcd(const std::unordered_set<std::string> &objectKeys, const std::string &workerId,
                                 bool getLocalAz, std::vector<master::QueryMetaInfoPb> &queryMetas,
                                 std::vector<std::string> &absentObjectKeys);

    /**
     * @brief The specific implementation of query Meta, including construct message and query from ETCD manager.
     * @param[in] azName The name of AZ.
     * @param[in] objKey The object keys need to get from ETCD.
     * @param[in] workerId The workerId which objects belong to. Be empty if we get objects meta from hash table.
     * @param[out] queryMetas The vector stored meta info.
     */
    Status ConstructKeyAndQueryMetaFromEtcd(const std::string &azName, const std::string &objKey,
                                            const std::string &workerId,
                                            std::vector<master::QueryMetaInfoPb> &queryMetas);

    /**
     * @brief Remove object location.
     * @param[in] objectKey Object key.
     * @param[in] version Object version.
     */
    Status RemoveLocation(const std::string &objectKey, uint64_t version);

    /**
     * @brief Correct the QueryMetaRspPb if need.
     * @param[in] tmpPayloads Temporary payloads that received from QueryMeta request.
     * @param[out] rsp QueryMetaRspPb that need to be corrected.
     * @param[out] payloads Payload list that need to be merged.
     * @return Status of the call.
     */
    static Status CorrectQueryMetaResponse(std::vector<RpcMessage> &tmpPayloads, master::QueryMetaRspPb &rsp,
                                           std::vector<RpcMessage> &payloads);

    /**
     * @brief Get objects from anywhere, can be serial or batched.
     * @param[in] queryMetas QueryMeta result requested from master.
     * @param[in] readKeys read key info, contain offset, size, objKey.
     * @param[in] request Get request instance.
     * @param[in] payloads Get payloads that contains object data.
     * @param[in] lockedEntries Object lock entries.
     * @param[out] failedIds Failed get object keys.
     * @param[out] needRetryIds Need retry get id list.
     * @return Status of the call.
     */
    Status GetObjectsFromAnywhere(std::vector<master::QueryMetaInfoPb> &queryMetas,
                                  const std::map<std::string, ReadKey> &readKeys,
                                  const std::shared_ptr<GetRequest> &request, std::vector<RpcMessage> &payloads,
                                  std::map<std::string, std::pair<std::shared_ptr<SafeObjType>, bool>> &lockedEntries,
                                  std::unordered_set<std::string> &failedIds, std::vector<ReadKey> &needRetryIds);

    /**
     * @brief Get objects from anywhere parallelly.
     * @param[in] queryMetas QueryMeta result requested from master.
     * @param[in] readKeys read key info, contain offset, size, objKey.
     * @param[in] request Get request instance.
     * @param[in] payloads Get payloads that contains object data.
     * @param[in] lockedEntries Object lock entries.
     * @param[out] failedIds Failed get object keys.
     * @param[out] needRetryIds Need retry get id list.
     * @return Status of the call.
     */
    Status GetObjectsFromAnywhereParallelly(
        const std::vector<master::QueryMetaInfoPb> &queryMetas, const std::map<std::string, ReadKey> &readKeys,
        const std::shared_ptr<GetRequest> &request, std::vector<RpcMessage> &payloads,
        std::map<std::string, std::pair<std::shared_ptr<SafeObjType>, bool>> &lockedEntries,
        std::unordered_set<std::string> &failedIds, std::vector<ReadKey> &needRetryIds);

    /**
     * @brief Get objects from anywhere serially.
     * @param[in] queryMetas QueryMeta result requested from master.
     * @param[in] readKeys read key info, contain offset, size, objKey.
     * @param[in] request Get request instance.
     * @param[in] payloads Get payloads that contains object data.
     * @param[in] lockedEntries Object lock entries.
     * @param[out] failedIds Failed get object keys.
     * @param[out] needRetryIds Need retry get id list.
     * @return Status of the call.
     */
    Status GetObjectsFromAnywhereSerially(
        const std::vector<master::QueryMetaInfoPb> &queryMetas, const std::map<std::string, ReadKey> &readKeys,
        const std::shared_ptr<GetRequest> &request, std::vector<RpcMessage> &payloads,
        std::map<std::string, std::pair<std::shared_ptr<SafeObjType>, bool>> &lockedEntries,
        std::unordered_set<std::string> &failedIds, std::vector<ReadKey> &needRetryIds);

    /**
     * @brief Get objects from anywhere batched.
     * @param[in/out] queryMetas QueryMeta result requested from master.
     * @param[in] readKeys read key info, contain offset, size, objKey.
     * @param[in] request Get request instance.
     * @param[in] payloads Get payloads that contains object data.
     * @param[in] lockedEntries Object lock entries.
     * @param[out] failedIds Failed get object keys.
     * @param[out] needRetryIds Need retry get id list.
     * @return Status of the call.
     */
    Status GetObjectsFromAnywhereBatched(
        std::vector<master::QueryMetaInfoPb> &queryMetas, const std::map<std::string, ReadKey> &readKeys,
        const std::shared_ptr<GetRequest> &request, std::vector<RpcMessage> &payloads,
        std::map<std::string, std::pair<std::shared_ptr<SafeObjType>, bool>> &lockedEntries,
        std::unordered_set<std::string> &failedIds, std::vector<ReadKey> &needRetryIds);

    /**
     * @brief Get object data from remote cache (remote worker or redis) based on object meta.
     * @param[in] readKey read key info, contain offset, size, objKey.
     * @param[in] request Get request instance.
     * @param[in] entry Object safe entry.
     * @param[in] isInsert Indicate the entry is new inserted to object table or not.
     * @param[in] queryMeta The object meta info contains remote address and data size.
     * @param[in] payloads Get payloads that contains object data.
     * @return Status of the call.
     */
    Status GetObjectFromAnywhereWithLock(const ReadKey &readKey, const std::shared_ptr<GetRequest> &request,
                                         std::shared_ptr<SafeObjType> &entry, bool isInsert,
                                         const master::QueryMetaInfoPb &queryMeta, std::vector<RpcMessage> &payloads);

    /**
     * @brief Preprocess the list of objectKeys before getting them from L2 cache without meta.
     * @param[in] objectKeys The object keys need to get without meta.
     * @param[in] lockedEntries Object lock entries.
     * @param[out] failedIds Failed get object keys.
     * @return Status of the call.
     */
    Status GetObjectsWithoutMeta(std::map<std::string, uint64_t> &objectKeys,
                                 std::map<std::string, std::pair<std::shared_ptr<SafeObjType>, bool>> &lockedEntries,
                                 std::unordered_set<std::string> &failedIds);

    /**
     * @brief Try to receive locked safe object from L2 storage.
     * @param[in] objectKV The reserved and locked safe object and its corresponding objectKey.
     * @param[in] minVersion The min version to get from L2 storage.
     * @return Status of the call.
     */
    Status GetObjectsWithoutMetaFromL2Cache(ObjectKV &objectKV, uint64_t minVersion);

    /**
     * @brief Get object data from remote cache (remote worker) within the lock.
     * @param[in] meta The object meta info contains remote address and data size.
     * @param[in] request Get request instance.
     * @param[in] address The remote worker address.
     * @param[in] singleCopy Indicate the object is single copy or not.
     * @param[out] objectKV The reserved and locked safe object and its corresponding objectKey.
     * @return Status of the call.
     */
    Status GetObjectFromRemoteOnLock(const ObjectMetaPb &meta, const std::shared_ptr<GetRequest> &request,
                                     const std::string &address, bool singleCopy, ReadObjectKV &objectKV);

    /**
     * @brief Get batch of object data from remote cache (remote worker or redis) within the lock.
     * @param[in] address The remote worker address.
     * @param[in] metas The batched object meta info contains remote address and data size.
     * @param[in] readKeys The read key info, contain offset, size, objKey.
     * @param[in] request Get request instance.
     * @param[in] lockedEntries Object lock entries.
     * @param[out] successIds Succeeded get object keys.
     * @param[out] needRetryIds Need retry get id list.
     * @param[out] failedIds Failed get object keys.
     * @param[out] failedMetas Failed get object metas.
     * @return Status of the call.
     */
    Status BatchGetObjectFromRemoteOnLock(
        const std::string &address, std::list<ObjectMetaPb *> &metas, const std::map<std::string, ReadKey> &readKeys,
        const std::shared_ptr<GetRequest> &request,
        std::map<std::string, std::pair<std::shared_ptr<SafeObjType>, bool>> &lockedEntries,
        std::vector<std::string> &successIds, std::vector<ReadKey> &needRetryIds,
        std::unordered_set<std::string> &failedIds, std::list<ObjectMetaPb *> &failedMetas);

    /**
     * @brief Helper function to split query meta  based off address and threshold.
     * @param[in/out] status The status to handle.
     * @param[in] queryMeta QueryMeta info Pb.
     * @param[out] groupedQueryMeta Grouped meta by address and payload split by threshold.
     * @return Status of the call.
     */
    void GroupQueryMeta(
        master::QueryMetaInfoPb &queryMeta,
        std::unordered_map<std::string, std::list<std::pair<std::list<ObjectMetaPb *>, uint64_t>>> &groupedQueryMetas);

    /**
     * @brief Helper function to handle individual status returned from the batch get request.
     * @param[in/out] status The status to handle.
     * @param[in] objectKey The object key.
     * @param[in] readKey The read key info, contain offset, size, objId.
     * @param[out] successIds Succeeded get object keys.
     * @param[out] needRetryIds Need retry get id list.
     * @param[out] failedIds Failed get object keys.
     * @return Status of the call.
     */
    void BatchGetObjectHandleIndividualStatus(Status &status, const std::string &objectKey, const ReadKey &readKey,
                                              std::vector<std::string> &successIds, std::vector<ReadKey> &needRetryIds,
                                              std::unordered_set<std::string> &failedIds);

    /**
     * @brief Helper function to send batch get request to remote worker and dump the payload into shm.
     * @param[in] address The remote worker address.
     * @param[in] metas The batched object meta info contains remote address and data size.
     * @param[in] readKeys The read key info, contain offset, size, objKey.
     * @param[in] request Get request instance.
     * @param[in] lockedEntries Object lock entries.
     * @param[out] successIds Succeeded get object keys.
     * @param[out] needRetryIds Need retry get id list.
     * @param[out] failedIds Failed get object keys.
     * @param[out] failedMetas Failed get object metas.
     * @return Status of the call.
     */
    Status BatchGetObjectFromRemoteWorker(
        const std::string &address, std::list<ObjectMetaPb *> &metas, const std::map<std::string, ReadKey> &readKeys,
        const std::shared_ptr<GetRequest> &request,
        std::map<std::string, std::pair<std::shared_ptr<SafeObjType>, bool>> &lockedEntries,
        std::vector<std::string> &successIds, std::vector<ReadKey> &needRetryIds,
        std::unordered_set<std::string> &failedIds, std::list<ObjectMetaPb *> &failedMetas);

    /**
     * @brief Helper function to construct batch get request.
     * @param[in] address The remote worker address.
     * @param[in] metas The batched object meta info contains remote address and data size.
     * @param[in] readKeys The read key info, contain offset, size, objKey.
     * @param[in] lockedEntries Object lock entries.
     * @param[out] successIds Succeeded get object keys.
     * @param[out] needRetryIds Need retry get id list.
     * @param[out] failedIds Failed get object keys.
     * @param[out] reqPb The request to initialize and construct.
     * @return Status of the call.
     */
    Status ConstructBatchGetRequest(const std::string &address, std::list<ObjectMetaPb *> &metas,
                                    const std::map<std::string, ReadKey> &readKeys,
                                    std::map<std::string, std::pair<std::shared_ptr<SafeObjType>, bool>> &lockedEntries,
                                    std::vector<std::string> &successIds, std::vector<ReadKey> &needRetryIds,
                                    std::unordered_set<std::string> &failedIds, BatchGetObjectRemoteReqPb &reqPb);

    /**
     * @brief Helper function to process the sub response from batched response.
     * @param[in] subResp The sub response of batched response.
     * @param[in] meta The object meta info contains remote address and data size.
     * @param[in] objectKV The reserved and locked safe object and its corresponding objectKey.
     * @param[in] payloads The payloads that comes together with response.
     * @param[in/out] payloadIndex The payload cursor index.
     * @param[out] tryGetFromElsewhere Whether to try get from elsewhere when response is failure.
     * @param[out] dataSizeChange Whether the object data size is changed.
     * @return Status of the call.
     */
    Status HandleBatchSubResponse(const GetObjectRemoteRspPb &subResp, ObjectMetaPb *meta, ReadObjectKV &objectKV,
                                  std::vector<RpcMessage> &payloads, uint64_t &payloadIndex, bool &tryGetFromElsewhere,
                                  bool &dataSizeChange);

    /**
     * @brief Helper function to process the sub response from batched response, part 2.
     * @param[in/out] subResp The sub response of batched response.
     * @param[in] address The remote worker address.
     * @param[in] meta The object meta info contains remote address and data size.
     * @param[in] objectKV The reserved and locked safe object and its corresponding objectKey.
     * @param[in] checkConnectStatus worker is connect status.
     * @param[out] tryGetFromElsewhere Whether to try get from elsewhere when response is failure.
     */
    void HandleBatchSubResponsePart2(Status &subRc, const std::string &address, ObjectMetaPb *meta,
                                     ReadObjectKV &objectKV, const Status &checkConnectStatus,
                                     bool &tryGetFromElsewhere);

    /**
     * @brief Helper function process the response from batch get.
     * @param[in] address The remote worker address.
     * @param[in] checkConnectStatus The worker is connect status
     * @param[in] metas The batched object meta info contains remote address and data size.
     * @param[in] readKeys The read key info, contain offset, size, objKey.
     * @param[in] request Get request instance.
     * @param[in] lockedEntries Object lock entries.
     * @param[in] status The status to handle.
     * @param[in] rspPb The response to handle.
     * @param[in] payloads The payloads that comes together with response.
     * @param[out] successIds Succeeded get object keys.
     * @param[out] needRetryIds Need retry get id list.
     * @param[out] failedIds Failed get object keys.
     * @param[out] failedMetas Failed get object metas.
     * @return Status of the call.
     */
    Status ProcessBatchResponse(const std::string &address, Status &checkConnectStatus,
                                std::list<ObjectMetaPb *> &metas, const std::map<std::string, ReadKey> &readKeys,
                                const std::shared_ptr<GetRequest> &request,
                                std::map<std::string, std::pair<std::shared_ptr<SafeObjType>, bool>> &lockedEntries,
                                const Status &status, BatchGetObjectRemoteRspPb &rspPb,
                                std::vector<RpcMessage> &payloads, std::vector<std::string> &successIds,
                                std::vector<ReadKey> &needRetryIds, std::unordered_set<std::string> &failedIds,
                                std::list<ObjectMetaPb *> &failedMetas, bool &dataSizeChange);

    /**
     * @brief Try get object from other AZ.
     * @param[in] meta The object meta info contains remote address and data size.
     * @param[in] hostAddr  The remote worker address.
     * @param[out] objectKV The reserved and locked safe object and its corresponding objectKey.
     * @param[out] status Status of the call.
     */
    void TryGetObjectFromOtherAZ(const ObjectMetaPb &meta, const HostPort &hostAddr, ReadObjectKV &objectKV,
                                 Status &status);

    /**
     * @brief Try to get object from L2 cache when object not found in other worker.
     * @param[in] meta The object meta info contains remote address and data size.
     * @param[in] address The remote worker address.
     * @param[in] isWorkerConnected Whether create meta data depend on this parameter.
     * @param[out] entry The reserved and locked safe object.
     * @param[out] status Status of the call.
     */
    void TryGetFromL2CacheWhenNotFoundInWorker(const ObjectMetaPb &meta, const std::string &address,
                                               bool ifWorkerConnected, ObjectKV &objectKV, Status &status);

    /**
     * @brief Get object data from persistence api without creating copy meta.
     * @param[in] objectKV The safe object to update and its corresponding objectKey.
     * @param[in] noVersionAvailable Get object from persistant storage without any version information.
     * @return Status of the call.
     */
    Status GetObjectFromPersistenceAndDumpWithoutCopyMeta(ObjectKV &objectKV, bool noVersionAvailable = false,
                                                          bool needDelete = true, uint64_t minVersion = 0);

    /**
     * @brief Get object data from persistence api.
     * @param[in] objectKV The safe object to update and its corresponding objectKey.
     * @return Status of the call.
     */
    Status GetObjectFromPersistenceAndDump(ObjectKV &objectKV);

    /**
     * @brief Get object data from QueryMeta result directly.
     * @param[in] queryMeta QueryMeta info Pb.
     * @param[in] payloads Get payloads that contains object data.
     * @param[out] objectKV The reserved and locked safe object and its corresponding objectKey.
     * @return Status of the call.
     */
    Status GetObjectFromQueryMetaResultOnLock(const master::QueryMetaInfoPb &queryMeta,
                                              std::vector<RpcMessage> &payloads, ReadObjectKV &objectKV);

    /**
     * @brief Get or Create a worker to Master api object for objKey in format uuid:host:port
     * @param[in] objKey Object key that contain remote master's ip address
     * @param[out] masterAddr The ip address of primary worker which stores objectKey's meta data
     * @return Status of the call.
     */
    Status GetMetaAddress(const std::string &objKey, HostPort &masterAddr) const;

    /**
     * @brief Determines whether to update the location information of the master node..
     * @param[in] consistencyType Determines whether to update location information based on the consistency type.
     * @return bool Update location information.
     */
    static bool IsUpdateLocation(ConsistencyType consistencyType);

    /**
     * @brief Check if object is near-death.
     * @param[in] location Location get from master.
     * @param[in] meta Object metadata.
     * @return True if object is near-death.
     */
    static bool IsNearDeathObject(const std::string &location, const ObjectMetaPb &meta);

    /**
     * @brief test the condition if get the object from the special storageType
     * @param[in] canNotFindInWorker the object exist in worker or not
     * @param[in] writeToL2Storage the object is write to l2 storage or not
     * @param[in] storageType the l2 storage type
     * @return true if need get object from l2 storage, otherwise false
     */
    bool IsGetFromL2Storage(bool canNotFindInWorker, bool writeToL2Storage, datasystem::L2StorageType storageType);

    /**
     * @brief Indicates whether the worker start with other az names.
     * @return true if worker have other az names.
     */
    static bool HaveOtherAZ();

    /**
     * @brief Batch lock for remote get scenario.
     * @param[in out] objectKeys Object key list that needs to be locked, if locked failed, the object key would be move
     * to failObjects.
     * @param[out] lockedEntries Locked entries save the locked object instances.
     * @param[out] failObjects Locked failed object list.
     * @return Status of the call.
     */
    Status BatchLockForGet(const std::vector<ReadKey> &objectKeys,
                           std::map<std::string, std::pair<std::shared_ptr<SafeObjType>, bool>> &lockedEntries,
                           std::unordered_set<std::string> &failObjects);

    /**
     * @brief Batch unlock for remote get scenario.
     * @param[in] failedObjectKeys Failed object key list that needs to be unlocked and erase.
     * @param[in] lockedEntries Locked entry list.
     */
    void BatchUnlockForGet(const std::unordered_set<std::string> &failedObjectKeys,
                           std::map<std::string, std::pair<std::shared_ptr<SafeObjType>, bool>> &lockedEntries);

    /**
     * @brief Batch unlock and erase for remote get via failed object keys.
     * @param[in] failedObjectKeys Failed object key list that needs to be unlocked and erase.
     * @param[in] lockedEntries Locked entry list.
     */
    void BatchUnlockForGet(const std::map<std::string, uint64_t> &failedObjectKeys,
                           std::map<std::string, std::pair<std::shared_ptr<SafeObjType>, bool>> &lockedEntries);

    /**
     * @brief Add remote get object key list.
     * @param[in] objectKeys Object key list.
     */
    void AddInRemoteGetObjects(const std::vector<ReadKey> &objectsNeedGetRemote);

    /**
     * @brief Remove remote get object key list.
     * @param[in] objectKeys Object key list.
     */
    void RemoveInRemoteGetObjects(const std::vector<ReadKey> &objectsNeedGetRemote);

    /**
     * @brief Remove remote get object key.
     * @param[in] objectKey Object key.
     */
    void RemoveInRemoteGetObject(const std::string &objectKey);

    /**
     * @brief Mark the specified object keys as being in the Get process.
     * @param[in] keys The list of object keys that are entering the Get process.
     */
    void MarkObjectsInGetProcess(const std::vector<std::string> &keys);

    /**
     * @brief Unmark the specified object keys from the Get process.
     * @param[in] keys The list of object keys that are exiting the Get process.
     */
    void UnmarkObjectsInGetProcess(const std::vector<std::string> &keys);

    /**
     * @brief Fill the GetObjMetaInfoRspPb.
     * @param[in] objectKeys The objects for obtaining meta info.
     * @param[in] result The meta info result of objects
     * @param[out] resp The response in GetObjMetaInfoRspPb format
     */
    void FillGetObjMetaInfoRspPb(const std::vector<std::string> &objectKeys,
                                 const std::unordered_map<std::string, master::ObjectLocationInfoPb> &result,
                                 GetObjMetaInfoRspPb &resp);

    /**
     * @brief Keep object data in memory, if it is on l2 cache or disk, just load it.
     * @param[in] objectKV The safe object to update and its corresponding objectKey.
     * @return Status of the call.ocm
     */
    Status KeepObjectDataInMemory(ReadObjectKV &objectKV);

    /**
     * @brief Query the metadata of the specified objects in the master.
     * @param[in] destMasterHostPort The dest master hostPort.
     * @param[in] subTimeout Request timeout for subscribe.
     * @param[in] objKeysToQuery The objects to query.
     * @param[out] rsp The qurey meta response.
     * @param[out] payloads The payloads that contains object data.
     * @return Status of the call.
     */
    Status QueryMetaDataFromMasterImpl(const HostPort &destMasterHostPort, uint64_t subTimeout,
                                       const std::vector<std::string> &objKeysToQuery, bool isFromOtherAz,
                                       datasystem::master::QueryMetaRspPb &rsp, std::vector<RpcMessage> &payloads);

    /**
     * @brief Process the object keys that fail to query metadata if cross-az get meta is allowed.
     * @param[in] subTimeout Request timeout for subscribe.
     * @param[out] objKeysUndecidedMaster The objects whose metadata location cannot be found.
     * @param[out] objectKeysQueryMetaFailed The objects whose metadata location can be found, but querying the metadata
     * fails.
     * @param[out] objectKeysMayInOtherAz The objKey which need to be tried to get metadata from other AZ.
     * @param[out] queryMetas The vector stored meta info.
     * @param[out] payloads The payloads that contains object data.
     */
    Status ProcessQueryMetaFailedObjsIfAllowCrossAzGetMeta(
        uint64_t subTimeout, std::unordered_map<std::string, std::unordered_set<std::string>> &objKeysUndecidedMaster,
        ObjectKeysQueryMetaFailed &objectKeysQueryMetaFailed, std::unordered_set<std::string> &objectKeysMayInOtherAz,
        std::vector<master::QueryMetaInfoPb> &queryMetas, std::vector<RpcMessage> &payloads);

    /**
     * @brief Process the object keys that fail to query metadata If meta is stored in etcd.
     * @param[in] objKeysUndecidedMaster The objects whose metadata location cannot be found.
     * @param[in] objectKeysPuzzled The objects whose metadata location can be found, but querying the metadata
     * fails. (Excluding objects that are confirmed to have no metadata)
     * @param[in] objectKeysMayInOtherAz The objKey which need to be tried to get metadata from other AZ.
     * @param[out] queryMetas The vector stored meta info.
     * @param[out] absentObjectKeys The objects whose metadata fails to be queried.
     */
    void ProcessQueryMetaFailedObjsWhenMetaStoredInEtcd(
        const std::unordered_map<std::string, std::unordered_set<std::string>> &objKeysUndecidedMaster,
        std::unordered_set<std::string> &&objectKeysNotExist, const std::unordered_set<std::string> &objectKeysPuzzled,
        const std::unordered_set<std::string> &objectKeysMayInOtherAz, std::vector<master::QueryMetaInfoPb> &queryMetas,
        std::vector<std::string> &absentObjectKeys);

    void HandleGetFailureHelper(const std::string &objectKey, uint64_t version, std::shared_ptr<SafeObjType> &entry,
                                bool isInsert);

    /**
     * @brief Checks if an object exists locally and is valid.
     * @param[in] key The object key to check.
     * @return true if the object exists locally and is valid; false otherwise.
     */
    bool IsLocalObject(const std::string &key);

    /**
     * @brief Check if object can tolerance does not exist node or not.
     * @param[in] singleCopy Indicate object is primary without copy or not.
     * @param[in] writeMode Write mode.
     * @return True if can tolerance does not exist node.
     */
    bool ToleranceNotExistNode(bool singleCopy, uint32_t writeMode);

    EtcdClusterManager *etcdCM_{ nullptr };  // back pointer to the cluster manager

    EtcdStore *etcdStore_;  // pointer to EtcdStore in WorkerOcServer

    std::shared_ptr<ThreadPool> memCpyThreadPool_{ nullptr };

    std::shared_ptr<ThreadPool> workerBatchThreadPool_{ nullptr };

    std::shared_ptr<ThreadPool> threadPool_{ nullptr };

    std::unique_ptr<ThreadPool> remoteGetThreadPool_{ nullptr };

    std::shared_ptr<AkSkManager> akSkManager_{ nullptr };

    HostPort localAddress_;

    std::shared_timed_mutex inRemoteGetIdsMutex_; // the mutex for inRemoteGetIds_

    std::unordered_set<std::string> inRemoteGetIds_; // the object keys that in remote get

    std::vector<std::string> otherAZNames_;

    std::shared_mutex objectsInGetProcessMutex_; // the mutex for objectsInGetProcess_

    std::unordered_map<std::string, int> objectsInGetProcess_;

    static constexpr size_t OBJECTS_NOT_EXIST_IDX = 0;
    static constexpr size_t OBJECTS_PUZZLED_IDX = 1;
};

}  // namespace object_cache
}  // namespace datasystem
#endif  // DATASYSTEM_OBJECT_CACHE_WORKER_SERVICE_GET_IMPL_H
