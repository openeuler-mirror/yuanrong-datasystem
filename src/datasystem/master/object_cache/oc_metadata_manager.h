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
 * Description: Module responsible for managing the object cache metadata on the master.
 */
#ifndef DATASYSTEM_MASTER_OBJECT_CACHE_OC_METADATA_MANAGER_H
#define DATASYSTEM_MASTER_OBJECT_CACHE_OC_METADATA_MANAGER_H

#include <cstdint>
#include <iomanip>
#include <list>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <google/protobuf/repeated_field.h>
#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/l2cache/persistence_api.h"
#include "datasystem/common/rpc/rpc_server_stream_base.h"
#include "datasystem/master/object_cache/device/master_dev_oc_manager.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/worker/hash_ring/hash_ring.h"
#include "datasystem/common/eventloop/timer_queue.h"
#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/object_cache/object_ref_info.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/master/metadata_redirect_helper.h"
#include "datasystem/master/object_cache/expired_object_manager.h"
#include "datasystem/master/object_cache/master_worker_oc_api.h"
#include "datasystem/master/object_cache/oc_global_cache_delete_manager.h"
#include "datasystem/master/object_cache/oc_nested_manager.h"
#include "datasystem/master/object_cache/delete_object_mediator.h"
#include "datasystem/master/object_cache/oc_notify_worker_manager.h"
#include "datasystem/master/object_cache/store/object_meta_store.h"
#include "datasystem/protos/master_heartbeat.pb.h"
#include "datasystem/protos/master_object.pb.h"
#include "datasystem/protos/master_object.service.rpc.pb.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/worker/object_cache/worker_worker_oc_api.h"

namespace datasystem {
namespace master {
enum MULTI_SET_STATE { IDLE = 0, PENDING = 1 };
struct SubscribeMeta {
    SubscribeMeta(std::string reqId, std::list<std::string> objects, std::string address, bool isFromOtherAz)
        : reqId_(std::move(reqId)),
          objects_(std::move(objects)),
          address_(std::move(address)),
          isFromOtherAz_(isFromOtherAz),
          timer_(nullptr)
    {
    }
    // request id
    std::string reqId_;
    // the waiting publish objects
    std::list<std::string> objects_;
    // the subscribe rpc address
    std::string address_;
    bool isFromOtherAz_;
    // The timer_ indicates timeout event, it needs to be canceled if the timeout event is returned in advance.
    std::unique_ptr<TimerQueue::TimerImpl> timer_;
};

enum class AckState: int {
    UNACK = 0,
    ACK = 1
};

struct ObjectMeta {
    ObjectMetaPb meta;
    int64_t value = 0;
    std::unordered_map<ImmutableString, AckState> locations;
    MULTI_SET_STATE multiSetState = MULTI_SET_STATE::IDLE;
    int64_t multiSetTimestamp = 0;

    /**
     * @brief Check if the object is binary.
     * @return True if the object is binary, false otherwise.
     */
    bool IsBinary() const
    {
        return meta.config().data_format() == static_cast<uint32_t>(DataFormat::BINARY);
    }

    /**
     * @brief Check if the object is a dev object.
     * @return True if the object is a dev object, false otherwise.
     */
    bool IsHetero() const
    {
        return meta.config().data_format() == static_cast<uint32_t>(DataFormat::HETERO);
    }

    /**
     * @brief Check if consistency type is causal.
     * @return True if consistency type is causal, false otherwise.
     */
    bool IsCausal() const
    {
        return meta.config().consistency_type() == static_cast<uint32_t>(ConsistencyType::CAUSAL);
    }

    /**
     * @brief Check if consistency type is pram.
     * @return True if consistency type is pram, false otherwise.
     */
    bool IsPram() const
    {
        return meta.config().consistency_type() == static_cast<uint32_t>(ConsistencyType::PRAM);
    }

    /**
     * @brief Check if the object has L2 cache.
     * @return True if the object has L2 cache, false otherwise.
     */
    bool HasL2Cache() const
    {
        return meta.config().write_mode() == static_cast<uint32_t>(WriteMode::WRITE_THROUGH_L2_CACHE)
               || meta.config().write_mode() == static_cast<uint32_t>(WriteMode::WRITE_BACK_L2_CACHE)
               || meta.config().write_mode() == static_cast<uint32_t>(WriteMode::WRITE_BACK_L2_CACHE_EVICT);
    }

    /**
     * @brief Get object's L2 cache type.
     * @return The object's L2 cache type.
     */
    int GetL2CacheType() const
    {
        return static_cast<int>(meta.config().write_mode());
    }

    /**
     * @brief Check if the object is None L2 cache evict type.
     * @return True if the object is None L2 cache evict type, false otherwise.
     */
    bool IsNoneL2CacheEvict() const
    {
        return meta.config().write_mode() == static_cast<uint32_t>(WriteMode::NONE_L2_CACHE_EVICT);
    }

    /**
     * @brief Check if the object is None L2 cache type.
     * @return True if the object is None L2 cache type, false otherwise.
     */
    bool IsNoneL2Cache() const
    {
        return meta.config().write_mode() == static_cast<uint32_t>(WriteMode::NONE_L2_CACHE);
    }

    /**
     * @brief Check if the object is write back l2 cache type.
     * @return True if the object is write back l2 cache type, false otherwise.
     */
    bool IsWriteBackL2Cache() const
    {
        return meta.config().write_mode() == static_cast<uint32_t>(WriteMode::WRITE_BACK_L2_CACHE);
    }

    /**
     * @brief Check if the object is write back l2 cache type.
     * @return True if the object is write back l2 cache type, false otherwise.
     */
    bool IsWriteBackL2CacheEvict() const
    {
        return meta.config().write_mode() == static_cast<uint32_t>(WriteMode::WRITE_BACK_L2_CACHE_EVICT);
    }

    /**
     * @brief Check if the object is write through l2 cache type.
     * @return True if the object is write through l2 cache type, false otherwise.
     */
    bool IsWriteThroughL2Cache() const
    {
        return meta.config().write_mode() == static_cast<uint32_t>(WriteMode::WRITE_THROUGH_L2_CACHE);
    }

    /**
     * @brief Check if the object IsPrimaryWithoutCopy.
     * @param[in] address primary address
     * @return True if the object is IsPrimaryWithoutCopy, false otherwise.
     */
    bool IsPrimaryWithoutCopy(const std::string &address) const
    {
        return locations.size() == 1 && locations.begin()->first == address;
    }

    /**
     * @brief Check if the object is replica type.
     * @return true The object is replica type, false otherwise.
     */
    bool IsReplica() const
    {
        return meta.config().is_replica();
    }
};

using TbbMetaTable = tbb::concurrent_hash_map<ImmutableString, ObjectMeta>;
using TbbSubMetaTable = tbb::concurrent_hash_map<ImmutableString, std::shared_ptr<SubscribeMeta>>;
using TbbReqIdTable = tbb::concurrent_hash_map<ImmutableString, std::set<ImmutableString>>;
using TbbRemoteClientIdRefTable = tbb::concurrent_hash_map<ImmutableString, std::unordered_set<ImmutableString>>;

class OCMetadataManager : public MetadataRedirectHelper, public std::enable_shared_from_this<OCMetadataManager> {
public:
    /**
     * @brief Construct a new OCMetadataManager object
     * @param[in] akSkManager Used to do AK/SK authenticate.
     * @param[in] rocksStore pointer to RocksStore owned by WorkerOcServer
     * @param[in] etcdStore pointer to EtcdStore owned by WorkerOcServer
     * @param[in] persistApi Provides calls to a persistence service through a cloud client.
     * @param[in] masterAddress The address of master.
     * @param[in] cm ETCD cluster manager for persistence.
     * @param[in] dbName The db name.
     */
    OCMetadataManager(std::shared_ptr<AkSkManager> akSkManager, RocksStore *rocksStore, EtcdStore *etcdStore,
                      std::shared_ptr<PersistenceApi> persistApi = nullptr, const std::string &masterAddress = "",
                      EtcdClusterManager *cm = nullptr, const std::string &dbName = "", bool newNode = false);

    ~OCMetadataManager();

    /**
     * @brief Initialize OCMetadataManager.
     * @return Status of the call.
     */
    Status Init();

    /**
     * @brief Initialize the subscribe to the event.
     * @return Status of the call.
     */
    void InitSubscribeEvent();

    struct BinaryFormatParamsStruct {
        uint32_t writeMode;
        uint32_t dataFormat;
        uint32_t consistencyType;
        uint32_t cacheType;
        bool isReplica;
    };

    /**
     * @brief Check whether publish parameters of the binary format data is matched.
     * @param[in] objectKey object key parameters.
     * @param[in] prevMeta previous meta parameters.
     * @param[in] newMeta New meta parameters.
     * @param[in] nestedObjectKeys Nested keys need to be check.
     * @return
     */
    Status CheckBinaryFormatParamMatch(const std::string &objectKey, const ObjectMeta &prevMeta,
                                       const BinaryFormatParamsStruct &newMeta,
                                       const std::set<ImmutableString> &nestedObjectKeys = {});

    /**
     * @brief Create object meta info in cache and rocksdb.
     * @param[in] request The meta of object.
     * @param[out] response The response to worker.
     * @return Status of the call.
     */
    Status CreateMeta(const CreateMetaReqPb &request, CreateMetaRspPb &response);

    /**
     * @brief Create meta info in cache and rocksdb.
     * @param[in] newMeta Meta info
     * @param[in] address request address
     * @param[in] nestedObjectKeys nested object keys
     * @param[out] version object version
     * @param[out] firstOne create first time or not
     */
    Status CreateMeta(const ObjectMetaPb &newMeta, const std::string &address,
                      const std::set<ImmutableString> &nestedObjectKeys, int64_t &version, bool &firstOne);

    /**
     * @brief Create multi object meta info in cache and rocksdb transaction.
     * @param[in] request The meta of object.
     * @param[out] response The response to worker.
     * @return Status of the call.
     */
    Status CreateMultiMetaTx(const CreateMultiMetaReqPb &req, CreateMultiMetaRspPb &rsp);

    /**
     * @brief Create multi object meta info in cache and rocksdb not transaction.
     * @param[in] request The meta of object.
     * @param[out] response The response to worker.
     * @return Status of the call.
     */
    Status CreateMultiMetaNtx(const CreateMultiMetaReqPb &req, CreateMultiMetaRspPb &rsp);

    /**
     * @brief Set meta info for meta
     * @param[in] newMeta Meta info
     * @param[in] address request address
     * @param[in] version object version
     * @param[out] metaCache out meta info.
     */
    void SetMetaInfo(const ObjectMetaPb &newMeta, const std::string &address, int64_t version, ObjectMeta &metaCache);

    /**
     * @brief Construct meta info for meta
     * @param[in] req request info
     * @param[in] info object base info
     * @param[in] version object version
     * @param[out] meta out meta info.
     */
    void ConstructMetaInfo(const CreateMultiMetaReqPb &req, const ObjectBaseInfoPb &info, int64_t version,
                           ObjectMetaPb &meta);

    /**
     * @brief Insert to etcd memory table.
     * @param[in] objectKey The key of object.
     * @param[in] tableName table name.
     * @param[in] metaPb The meta info.
     * @param[in] key The key of etcd.
     */
    void InsertToEtcdTableInMemory(const std::string &objectKey, const ObjectMetaPb &metaPb,
                                   const std::string &tableName, const std::string &key);

    /**
     * @brief Create Migration Meta data
     * @param[in] metaCache meta info
     * @param[in] addr worker location
     * @param[in] ackState ack state
     * @param[in] objectKey object key
     * @param[in] metaPb meta info
     * @return Status of the call
     */
    Status AddLocation(ObjectMeta &metaCache, const std::string &addr, AckState ackState, const std::string &objectKey,
                       const ObjectMetaPb &metaPb);

    /**
     * @brief Create object copy meta info in cache and rocksdb.
     * @param[in] request The meta of object.
     * @param[out] response The response to worker.
     * @return Status of the call.
     */
    Status CreateCopyMeta(const CreateCopyMetaReqPb &request, CreateCopyMetaRspPb &response);

    /**
     * @brief Create multi object copy meta info in cache and rocksdb.
     * @param[in] request The multi meta of object.
     * @param[out] response The response to worker.
     * @return Status of the call.
     */
    Status CreateMultiCopyMeta(const CreateMultiCopyMetaReqPb &request, CreateMultiCopyMetaRspPb &response);

    /**
     * @brief Query metas by objectKeys.
     * @param[in] req The request of QueryMeta.
     * @param[out] rsp The response of QueryMeta.
     * @param[out] payloads If object's data in this node, we will put it in the payoads.
     * @return Status of the call.
     */
    Status QueryMeta(const QueryMetaReqPb &req, QueryMetaRspPb &rsp, std::vector<RpcMessage> &payloads);

    /**
     * @brief Query metas by objectKeys from metaTable.
     * @param[in] req The request of QueryMeta.
     * @param[in] objectKeys ID of the metadata to be queried.
     * @param[out] rsp The response of QueryMeta.
     * @param[out] payloads If object's data in this node, we will put it in the payloads.
     * @param[out] notExistObjectKeys ID of the object to be subscribed to.
     * @return Status of the call.
     */
    Status QueryMetaFromMetaTable(const QueryMetaReqPb &req, const std::vector<std::string> &objectKeys,
                                  QueryMetaRspPb &rsp, std::vector<RpcMessage> &payloads,
                                  std::vector<std::string> &notExistObjectKeys);

    /**
     * @brief Roll back meta when multi create meta failed.
     * @param[in] rollBackIds need roll back object keys
     * @param[in] address The address of worker.
     * @param[in] version The version of meta.
     */
    void RollBackMultiMetaWhenCreateFailed(const std::vector<std::string> &rollBackIds, const std::string address,
                                           uint64_t version = 0);

    /**
     * @brief Create multi meta in rocks db and etcd and update subscribe.
     * @param[in] objectKeys The object keys.
     * @param[in] address Request address.
     * @param[in] type write type.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call
     */
    Status PublishMultiMeta(const std::vector<std::string> &objectKeys, const std::string &address,
                            ObjectMetaStore::WriteType type, uint64_t version, CreateMultiMetaRspPb &rsp);

    /**
     * @brief Create multi meta.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     */
    Status CreateMultiMeta(const CreateMultiMetaReqPb &req, CreateMultiMetaRspPb &rsp);

    /**
     * @brief Get the Primary Replica Addr object
     * @param[in] masterAddr masterAddr
     * @param[out] primaryAddr primary replica addr
     * @return Status of the call;
     */
    Status GetPrimaryReplicaAddr(const std::string &masterAddr, HostPort &primaryAddr);

    /**
     * @brief Create multi meta phase two.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the call.
     */
    Status CreateMultiMetaPhaseTwo(const CreateMultiMetaPhaseTwoReqPb &req, CreateMultiMetaRspPb &rsp);

    /**
     * @brief Remove object meta info of server in cache and rocksdb.
     * @param[in] request The rpc request protobuf.
     * @param[out] response The rpc response protobuf.
     * @return Status of the call.
     */
    Status RemoveMeta(const RemoveMetaReqPb &request, RemoveMetaRspPb &response);

    /**
     * @brief Update object meta in cache and rocksdb.
     * @param[in] request The rpc request protobuf.
     * @param[in] objectMeta The meta of object.
     * @param[out] response The rpc response protobuf.
     * @return Status of the call.
     */
    Status UpdateMetaByState(const UpdateMetaReqPb &request, ObjectMeta &objectMeta, UpdateMetaRspPb &response);

    /**
     * @brief Update object meta in cache and rocksdb.
     * @param[in] request The rpc request protobuf.
     * @param[out] response The rpc response protobuf.
     * @return Status of the call.
     */
    Status UpdateMeta(const UpdateMetaReqPb &request, UpdateMetaRspPb &response);

    /**
     * @brief Remove object meta infos of server in cache and rocksdb by worker address.
     * @param[in] workerAddr The rpc service endpoint of worker.
     * @return Status of the call.
     */
    Status RemoveMetaByWorker(const std::string &workerAddr);

    /**
     * @brief Remove object meta infos and global ref because of worker time out.
     * @param[in] changePrimaryCopy changePrimaryCopy when timeout.
     * @param[in] removeFailWorkerMetaData remove meta info about failed worker.
     * @return Status of the call.
     */
    Status ProcessWorkerTimeout(const std::string &workerAddr, bool changePrimaryCopy, bool removeFailWorkerMetaData);

    /**
     * @brief Process other az worker dead event.
     * @param[in] workerAddr The dead worker.
     * @return Status of the call.
     */
    Status ProcessOtherAzWorkerDead(const std::string &workerAddr);

    /**
     * @brief Processing After the Worker Restart.
     * @param[in] workerAddr The rpc service endpoint of worker.
     * @param[in] timestamp timestamp of the event triggering reconciliation.
     * @param[in] sync Call OCNotifyWorkerManager::PushMetaToWorker or OCNotifyWorkerManager::AsyncPushMetaToWorker
     * @return Status of the call.
     */
    Status ProcessWorkerRestart(const std::string &workerAddr, int64_t timestamp, bool sync = false);

    /**
     * @brief Recover master app ref
     * @param matchFunc[in] match function to find match remoteclientids
     * @param standbyWorker[in] standby worker of scale down worker.
     * @return Status
     */
    Status RecoverMasterAppRef(std::function<bool(const std::string &)> matchFunc, const std::string &standbyWorker);

    /**
     * @brief Processing After the Worker Network Recovery.
     * @param[in] workerAddr The rpc service endpoint of worker.
     * @param[in] timestamp timestamp of the event triggering reconciliation.
     * @param[in] isOffline Indicates whether the worker is offline.
     * @return Status of the call.
     */
    Status ProcessWorkerNetworkRecovery(const std::string &workerAddr, int64_t timestamp, bool isOffline);

    /**
     * @brief After the worker dies, clears data and process pushed data by the worker.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status ProcessWorkerPushMeta(const PushMetaToMasterReqPb &req, PushMetaToMasterRspPb &rsp);

    /**
     * @brief Get all worker addresses of object metas.
     * @param[out] workerAddresses the ip port of workers.
     */
    void GetWorkerAddress(std::set<std::string> &workerAddresses);

    /**
     * @brief Get object locations by objectKeys.
     * @param[in] req Includes ids for objects.
     * @param[out] locations List of objects location info.
     * @return Status of the call.
     */
    Status GetObjectLocations(const GetObjectLocationsReqPb &req, std::vector<ObjectLocationInfoPb> &locations);

    /**
     * @brief Delete metadata and notify other workers to delete these objects synchronously.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf. Valid if serverApi is empty.
     * @param[in] serverApi The WriterReader in server side which holds unary rpc socket.
     * @param[in] needReleaseRpc Whether to release master rpc thread when notify worker detele.
     */
    void DeleteAllCopyMetaImpl(
        const DeleteAllCopyMetaReqPb &request, DeleteAllCopyMetaRspPb &response,
        const std::shared_ptr<ServerUnaryWriterReader<DeleteAllCopyMetaRspPb, DeleteAllCopyMetaReqPb>> &serverApi,
        bool needReleaseRpc);

    /**
     * @brief Delete metadata and notify other workers to delete these objects synchronously.
     * @param[in] request The rpc request protobuf.
     * @param[out] response The rpc response protobuf.
     * @return Status of the call.
     */
    void DeleteAllCopyMeta(const DeleteAllCopyMetaReqPb &request, DeleteAllCopyMetaRspPb &response);

    /**
     * @brief Delete metadata and notify other workers to delete these objects synchronously.
     * @param[in] request The rpc request protobuf.
     * @param[in] serverApi The WriterReader in server side which holds unary rpc socket.
     * @return Status of the call.
     */
    void DeleteAllCopyMetaWithServerApi(
        const DeleteAllCopyMetaReqPb &request,
        const std::shared_ptr<ServerUnaryWriterReader<DeleteAllCopyMetaRspPb, DeleteAllCopyMetaReqPb>> &serverApi);

    /**
     * @brief Check whether the remoteClientId requires redirection.
     * @param[in] remoteClientId The remote client id.
     * @param[out] needRedirect Whether redirect or not.
     * @param[out] newAddr The new redirection address.
     * @return result of the redirection.
     */
    bool RedirectClientIdRef(const std::string &remoteClientId, bool needRedirect, std::string &newAddr);

    /**
     * @brief Check whether the objectKey requires redirection.
     * @param[out] objectKey The rpc request protobuf.
     * @param[out] needRedirect Whether redirect or not.
     * @param[out] newAddr The new redirection address.
     * @param[out] isMigrating Is meta or refs is migrating.
     */
    void RedirectObjRefs(std::string &objectKey, bool &needRedirect, std::string &newAddr, bool &isMigrating);

    /**
     * @brief Check whether object keys requires redirection. Put ids into resp if need, otherwise put ids into
     * objectKeys
     * @param[out] response Response of redirect.
     * @param[in] redirect Redirect or not.
     * @param[out] objectKeys The object keys no redirection required.
     */
    template <typename Rsp>
    void RedirectObjRefs(Rsp &response, bool redirect, std::vector<std::string> &objectKeys)
    {
        INJECT_POINT("OCMetadataManager.Redirect.ConstructRsp",
                     [&response, &objectKeys](const std::string &addr1, const std::string &addr2) {
                         RedirectMetaInfo *info1 = response.add_infos();
                         info1->set_redirect_meta_address(addr1);
                         RedirectMetaInfo *info2 = response.add_infos();
                         info2->set_redirect_meta_address(addr2);
                         const int mod = 2;
                         for (size_t i = 0; i < objectKeys.size(); i++) {
                             if (i % mod == 0) {
                                 info1->add_change_meta_ids(objectKeys[i]);
                             } else {
                                 info2->add_change_meta_ids(objectKeys[i]);
                             }
                         }
                     });
        response.set_ref_is_moving(false);
        if (!redirect || !FLAGS_enable_redirect) {
            VLOG(1) << "receive redirect req";
            redirect = false;
            return;
        }
        std::vector<std::string> objectKeysNoNeedRedirect;
        std::unordered_map<std::string, std::vector<std::string>> objectKeysNeedRedirectInfo;
        for (auto &objectKey : objectKeys) {
            std::string newAddr;
            bool needRedirect = false;
            bool isMigrating = false;
            RedirectObjRefs(objectKey, needRedirect, newAddr, isMigrating);
            if (isMigrating) {
                response.set_ref_is_moving(isMigrating);
                return;
            }
            if (needRedirect) {
                objectKeysNeedRedirectInfo[newAddr].emplace_back(objectKey);
            } else {
                objectKeysNoNeedRedirect.emplace_back(objectKey);
            }
        }
        objectKeys = objectKeysNoNeedRedirect;
        for (auto &it : objectKeysNeedRedirectInfo) {
            RedirectMetaInfo *info = response.add_infos();
            info->set_redirect_meta_address(it.first);
            for (auto &objectKey : it.second) {
                info->add_change_meta_ids(objectKey);
            }
        }
    }

    /**
     * @brief Load ref from rocksdb.
     * @param[in] tableName The remote client object ref table name.
     * @param[in] func The func of increase ref.
     * @return Status of the call.
     */
    Status LoadRefFromRocks(const std::string &tableName,
                            std::function<void(const std::string &, const std::string &)> func);

    /**
     * @brief Load ref from rocksdb.
     * @param[in] tableName The name of GLOBAL_REF_TABLE.
     * @param[in] func The func of increase ref.
     * @return Status of the call.
     */
    Status LoadRefFromRocks(const std::string &tableName,
                            std::function<void(const std::string &, const std::string &, bool)> func);

    /**
     * @brief Get the objKeys and remoteClientIds that meet the migration conditions.
     * @param[in] matchFunc The conditions to meet.
     * @param[out] objKeys The obj key need to migrate.
     * @return Status of the call.
     */
    void GetObjRefsMatch(const std::function<bool(const std::string &)> &matchFunc,
                         std::unordered_set<std::string> &objKeys);

    /**
     * @brief Get object global cache delete info match.
     * @param[in] matchFunc The conditions to meet.
     * @param[out] objectDeleteInfos The global cache delete info need to migrate.
     */
    void GetObjGlobalCacheDeletesMatch(
        const std::function<bool(const std::string &)> &matchFunc,
        std::unordered_map<std::string, std::unordered_map<uint64_t, uint64_t>> &objectDeleteInfos);

    /**
     * @brief Get the Remote Client Ids Match object
     * @param[in] matchFunc The conditions to meet.
     * @param[in] remoteClientIds The remoote clients need tooo migrate
     */
    void GetRemoteClientIdsMatch(const std::function<bool(const std::string &)> &matchFunc,
                                 std::vector<std::string> &remoteClientIds);

    /**
     * @brief Get the Nested Refs Match object
     * @param[in] matchFunc The conditions to meet.
     * @param[out] nestedObjKeys The nestedObjKeys.
     */
    void GetNestedRefsMatch(const std::function<bool(const std::string &)> &matchFunc,
                            std::unordered_set<std::string> &nestedObjKeys);

    /**
     * @brief Fill in the remote client id ref data to be migrated.
     * @param[in] remoteClientId The client id to be migrated.
     * @param[in] destination The destination master to migrate.
     * @param[out] clientIdRefs Data to be sent
     * @return Status of the call.
     */
    void FillClientIdRefsForMigration(const std::string &remoteClientId, const std::string &destination,
                                      ClientIdRefsForMigrationPb *clientIdRefs);

    /**
     * @brief Fill in the obj ref data to be migrated.
     * @param[in] objectKey The object key to be migrated.
     * @param[out] MetaForMigrationPb Data to be sent.
     * @return Status of the call.
     */
    void FillObjRefsForMigration(const std::string &objectKey, MetaForMigrationPb &objectRefsPb);

    /**
     * @brief Fill in the remoteClientIds data to be migrated.
     * @param[out] req The rpc request protobuf.
     */
    void FillRemoteClientIdForMigration(MigrateMetadataReqPb &req);

    /**
     * @brief Saves ref data migrated from other masters.
     * @param[in] objKey object key.
     * @param[in] objMeta The meta need t save.
     * @param[in] allRemoteClientIds all remote client in source node.
     * @return return true if success.
     */
    bool SaveOneMigrationObjRefData(const std::string &objKey, const MetaForMigrationPb &objMeta,
                                    const std::vector<std::string> &allRemoteClientIds);

    /**
     * @brief SaveSubscribeData
     * @param[in] req req The rpc request protobuf.
     * @return Status of the call
     */
    Status SaveSubscribeData(const MigrateMetadataReqPb &req);

    /**
     * @brief Handling data migration if success.
     * @param[in] objKey The object key to be migrated.
     * @param[in] remoteClientIds The remote client id to be migrated.
     * @return Status of the call.
     */
    void HandleObjRefDataMigrationOnSuccess(const std::string &objKey, const std::vector<std::string> &remoteClientIds);

    /**
     * @brief Process the GIncrease remote client id to master req.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @return Status of the call.
     */
    Status GIncreaseMasterAppRef(const GIncreaseReqPb &req, GIncreaseRspPb &resp);

    /**
     * @brief SaveMigrationRemoteClientRefData
     * @param[in] req all migration remote clientt info need to save.
     * @return Status
     */
    Status SaveMigrationRemoteClientRefData(const MigrateMetadataReqPb &req);

    /**
     * @brief Send request to master to increase remote client id count.
     * @param[in] remoteClientId The remote client id to increase.
     * @param[in/out] masterAddr The master of remoteclientid.
     * @return Status of the call.
     */
    Status GIncreaseRemoteClientIdToMaster(const std::string &remoteClientId, HostPort masterAddr = HostPort());

    /**
     * @brief Send request to master to increase the global reference count with remote client id.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @return Status of the call.
     */
    Status GIncreaseRefWithRemoteClientId(const GIncreaseReqPb &req, GIncreaseRspPb &resp);

    /**
     * @brief Release all objects of the remote client id.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     */
    void ReleaseGRefs(const ReleaseGRefsReqPb &req, ReleaseGRefsRspPb &resp);

    /**
     * @brief Send request to master to decrease the obj of remote client id.
     * @param[in] remoteClientId The remote client id.
     * @param[in] masterAddress The master address.
     * @return Status of the call.
     */
    Status ReleaseGRefsToMaster(const std::string &remoteClientId, const std::string &masterAddress);

    /**
     * @brief Release all objects of the remote client id.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @return Status of the call.
     */
    Status ReleaseGRefsOfRemoteClientId(const ReleaseGRefsReqPb &req, ReleaseGRefsRspPb &resp);

    /**
     * @brief Send request to master to increase the global reference count.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @return Status of the call.
     */
    Status GIncreaseRef(const GIncreaseReqPb &req, GIncreaseRspPb &resp);

    /**
     * @brief Construct requestObjectKeyMap.
     * @param[in] failDecIds Fail decrease ids.
     * @param[in] finishDecIds The last time to decrease ids.
     * @param[in] requestObjectKeyMap The map of object should be delete.
     */
    void ConstructRequestObjectKeyMap(const std::vector<std::string> failedDecIds,
                                      const std::vector<std::string> finishDecIds,
                                      std::unordered_map<std::string, bool> &requestObjectKeyMap);

    /**
     * @brief Send request to master to decrease the obj reference count with remote Pclient id.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     * @param[in] serverApi The WriterReader in server side which holds unary rpc socket.
     * @param[in] needReleaseRpc Whether to release master rpc thread when notify worker detele.
     * @return Status of the call.
     */
    void GDecreaseRefImplWithRemoteClientId(
        const GDecreaseReqPb &req, GDecreaseRspPb &resp,
        const std::shared_ptr<ServerUnaryWriterReader<GDecreaseRspPb, GDecreaseReqPb>> serverApi, bool needReleaseRpc);

    /**
     * @brief Send request to master to decrease the global reference count.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf. Valid if serverApi is empty.
     * @param[in] serverApi The WriterReader in server side which holds unary rpc socket.
     * @param[in] needReleaseRpc Whether to release master rpc thread when notify worker detele.
     */
    void GDecreaseRefImpl(const GDecreaseReqPb &req, GDecreaseRspPb &resp,
                          const std::shared_ptr<ServerUnaryWriterReader<GDecreaseRspPb, GDecreaseReqPb>> &serverApi,
                          bool needReleaseRpc);
    /**
     * @brief Send request to master to decrease the global reference count.
     * @param[in] req The rpc request protobuf.
     * @param[out] resp The rpc response protobuf.
     */
    void GDecreaseRef(const GDecreaseReqPb &req, GDecreaseRspPb &resp);

    /**
     * @brief Send request to master to decrease the global reference count.
     * @param[in] req The rpc request protobuf.
     * @param[in] serverApi The WriterReader in server side which holds unary rpc socket.
     */
    void GDecreaseRefWithServerApi(
        const GDecreaseReqPb &req,
        const std::shared_ptr<ServerUnaryWriterReader<GDecreaseRspPb, GDecreaseReqPb>> &serverApi);

    /**
     * @brief Rollback seal.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status RollbackSeal(const RollbackSealReqPb &req, RollbackSealRspPb &rsp);

    /**
     * @brief Increase dependent object count from remote worker.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the result.
     */
    Status IncreaseNestedRefCnt(const GIncNestedRefReqPb &req, GIncNestedRefRspPb &resp);

    /**
     * @brief Decrease dependent object count from remote worker.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the result.
     */
    Status DecreaseNestedRefCnt(const GDecNestedRefReqPb &req, GDecNestedRefRspPb &resp);

    /**
     * @brief Takes a copy of the local MasterWorkerService to allow rpc bypass when the master and worker are
     * collocated.
     * @param[in] service Pointer to the receiving side service of the MasterWorker api's
     * @param[in] masterAddr A copy of the local master host port
     */
    void AssignLocalWorker(object_cache::MasterWorkerOCServiceImpl *masterWorkerService,
                           object_cache::WorkerWorkerOCServiceImpl *workerWorkerService, const HostPort &masterAddr);

    /**
     * @brief Requests the worker to send meta to the master in the response.
     * @param[in] masterAddr The address of the master requesting for the metadata.
     * @param[in] workerAddr The address of the worker to whom the request is sent.
     * @return Status of the result.
     */
    Status RequestMetaFromWorker(const std::string &masterAddr, const std::string &workerAddr);

    /**
     * @brief Check whether there are any requests for asynchronously writing metadata to ETCD.
     * @return True if there are unfinished async requests.
     */
    bool HaveAsyncMetaRequest();

    static ObjectMetaStore::WriteType WriteMode2MetaType(uint32_t writeMode);

    /**
     * @brief Get object Meta type
     * @param[in] objectKey The key of object.
     * @param[in] metaCache The object meta.
     * @param[out] status last failed status.
     * @return status of the call.
     */
    Status SaveMigrationData(const std::string &objectKey, ObjectMeta &metaCache, Status &status);

    /**
     * @brief Saves metadata migrated from other masters.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return Status of the result.
     */
    Status SaveMigrationMetadata(const MigrateMetadataReqPb &req, MigrateMetadataRspPb &rsp);

    /**
     * @brief Replace object primary copy.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status ReplacePrimary(const ReplacePrimaryReqPb &req, ReplacePrimaryRspPb &rsp);

    /**
     * @brief Pure query object metadata.
     * @param[in] req The rpc request protobuf.
     * @param[out] rsp The rpc response protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status PureQueryMeta(const PureQueryMetaReqPb &req, PureQueryMetaRspPb &rsp);

    /**
     * @brief SaveNestedMigrationMetadata
     * @param[in] objMeta objMeta for nested info.
     */
    void SaveNestedMigrationMetadata(const MetaForMigrationPb &objMeta);

    /**
     * @brief Fill sub meta for migration
     * @param[in] objKeys objKeys for subscribe
     * @param[out] subMetas subMeta info for migration.
     */
    void FillSubMetas(const std::vector<std::string> &objKeys, std::vector<SubscribeInfoPb> &subMetas);

    /**
     * @brief Get object Meta type
     * @param[in] objectKey The key of object.
     * @param[out] The write type of object.
     * @return status of the call.
     */
    Status GetObjectMetaType(const std::string &objectKey, ObjectMetaStore::WriteType &type);

    /**
     * @brief Fill wait async put etcd elements to migrate metadata request.
     * @param[in] elements Async elements.
     * @param[out] meta Migrate metadata request.
     */
    void FillWaitAsyncElements(const std::unordered_set<std::shared_ptr<AsyncElement>> &elements,
                               MetaForMigrationPb &meta);

    /**
     * @brief Fill in the data to be migrated.
     * @param[in] objectKey The object key to be midrated.
     * @param[out] meta Data to be sent
     * @return Status of the result.
     */
    Status FillMetadataForMigration(
        const std::string &objectKey, MetaForMigrationPb &meta,
        std::unordered_map<std::string, std::unordered_set<std::shared_ptr<AsyncElement>>> &asyncMap);

    /**
     * @brief FillNestedDependencyObjForMigration
     * @param[in] objectKey parent object keys for nested object
     * @param[out] meta The meta info for migration.
     */
    void FillNestedInfoForMigration(const std::string &objectKey, MetaForMigrationPb &meta);

    /**
     * @brief Get the Subscibe Info Match object
     * @param[in] matchFunc condition to meet
     * @param[out] subMetaInfos subscribe info.
     */
    void GetSubscibeInfoMatch(std::function<bool(const std::string &)> matchFunc, std::vector<std::string> &objKeys);

    /**
     * @brief HandleSubDataMigrateSuccess
     * @param[in] req subscribe info.
     */
    void HandleSubDataMigrateSuccess(const MigrateMetadataReqPb &req);

    /**
     * @brief HandleNestedRefMigrateSuccess
     * @param[in] id nested keys for nested object
     */
    void HandleNestedRefMigrateSuccess(const std::string &id);

    /**
     * @brief Handling data migration failed.
     * @param[in] objMeta Metadata to be migrated
     */
    void HandleMetaDataMigrationFailed(
        const MetaForMigrationPb &objMeta,
        const std::unordered_map<std::string, std::unordered_set<std::shared_ptr<AsyncElement>>> &asyncMap = {});

    /**
     * @brief Delete the migrated metadata.
     * @param[in] objectKey The object key to be deleted.
     */
    void HandleMetaDataMigrationSuccess(const std::string &objectKey);

    /**
     * @brief Check if the metadata is available for given object key.
     * @param[in] id The object key.
     * @return Returns true if meta is found in meta table.
     */
    bool MetaIsFound(const std::string &objectKey) override;

    /**
     * @brief clear data without meta.
     * @param[in] range The hash range of objectKey to clear data.
     * @param[in] workerAddr The worker Addr.
     * @param[in] halfCompletedRanges Objects in this range need to be reconciliated before cleanup.
     * @return Status of the call.
     */
    Status ClearDataWithoutMeta(const worker::HashRange &range, const std::string &workerAddr,
                                const worker::HashRange &halfCompletedRanges, const std::vector<std::string> &uuids);

    /**
     * @brief Clear device metadata for scaled-in worker nodes if current node is the metadata master
     * @details Retrieves the metadata master address from etcd and compares with current worker address.
     *          If this node is the metadata master, it triggers cleanup of client metadata associated
     *          with the removed worker nodes.
     * @param[in] removeNodes List of worker node addresses that have been scaled in and need cleanup
     * @return Status of the call.
     */
    Status ClearDevClientMetaForScaledInWorker(const std::vector<std::string> &removeNodes);

    /**
     * @brief Shutdown the oc metadata manager module.
     */
    void Shutdown() override;

    /**
     * @brief Get objects that meet the meta conditions
     * @param[in] matchFunc The conditions to meet.
     * @param[out] objKeys Objects that meet the meta conditions.
     * @param[in] exitEarly Whether to exit cycle early.
     */
    void GetMetasMatch(std::function<bool(const std::string &)> &&matchFunc, std::vector<std::string> &objKeys,
                       bool *exitEarly = nullptr);

    void GetMetasInAsyncQueueMatch(
        std::function<bool(const std::string &)> &&matchFunc,
        std::unordered_map<std::string, std::unordered_set<std::shared_ptr<AsyncElement>>> &objAsyncMap);

    void GetAsyncElementsByObjectKey(const std::string &objectKey,
                                    std::unordered_set<std::shared_ptr<AsyncElement>> &elements);

    /**
     * @brief Recover data of faulty worker from etcd.
     * @param[in] workerUuids The uuids to be recovered.
     * @param[in] extraRanges The hash range of faulty worker.
     * @return Status of the result.
     */
    Status RecoverDataOfFaultyWorker(const std::vector<std::string> &workerUuids, const worker::HashRange &extraRanges);

    /**
     * @brief Get the usage of the queue for asynchronously writing ETCD data.
     * @note currentSize: the number of tasks in the current queue.
     *       totalLimit:  the maximum queue capacity
     * @return The Usage: "currentSize/totalLimit/workerL2CacheQueueUsag"
     */
    std::string GetETCDAsyncQueueUsage();

    /**
     * @brief Recover global cache del and cache invalid task
     * @param[in] workerUuids The uuids to be recovered.
     * @param[in] extraRanges The hash range of faulty worker.
     * @return Status of the result.
     */
    Status RecoverAsyncTask(const std::vector<std::string> &workerUuids, const worker::HashRange &extraRanges);

    /**
     * @brief Check meta table is empty;
     * @return meta table is empty or not
     */
    bool CheckMetaTableEmpty();

    /**
     * @brief Get the async threadpool usage of master.
     * @return Usage: "idleNum/currentTotalNum/maxThreadNum/waitingTaskNum/threadPoolUsage".
     */
    std::string GetMasterAsyncPoolUsage();

    /**
     * @brief Processe the primary copy when the worker times out.
     * @param[in] workerAddr Timeout worker.
     * @param[in] ifvoluntaryScaleDown Judge whether the worker is voluntary scale down,
     * a dead worker don't need to add PRIMARY_COPY_INVALID.
     */
    void ProcessPrimaryCopyByWorkerTimeout(const std::string &workerAddr, bool ifvoluntaryScaleDown = false);

    /**
     * @brief Return a pointer to global reference table.
     */
    object_cache::ObjectGlobalRefTable<ImmutableString> *GetGlobalRefTable()
    {
        return globalRefTable_.get();
    }

    /**
     * @brief Find all object keys need to deleted including the nested object, set to delete mediator.
     * @param[in/out] delMediator The object delete mediator to record delete state.
     */
    void FindNeedDeleteIds(DeleteObjectMediator &delMediator);

    /**
     * @brief Notify worker delete if need, and clear all meta info if delete successfully.
     * @param[in/out] delMediator The object delete mediator to record delete state.
     * @param[in] isExpired Is expired delete.
     */
    void NotifyDeleteAndClearMeta(DeleteObjectMediator &delMediator, bool isExpired);

    /**
     * @brief Create the meta of device object.
     * @param[in] newMeta The meta info.
     * @param[in] address The address of req protobuf.
     * @param[in] isMultiset if Multiset or not.
     * @return K_OK on success; the error code otherwise.
     */
    Status CreateDeviceMeta(const ObjectMetaPb &newMeta, const std::string &address);

    /**
     * @brief Get rocksdb name.
     * @return std::string the rocksdb name.
     */
    std::string GetDbName()
    {
        return dbName_;
    }

    /**
     * @brief Get the MasterDevOcManager instance.
     * @return The MasterDevOcManager instance.
     */
    std::shared_ptr<MasterDevOcManager> GetDeviceOcManager();

#ifdef WITH_TESTS
    OCNestedManager *GetNestedRefManager()
    {
        return nestedRefManager_.get();
    }
#endif

    /**
     * @brief Rollback metadata in directory.
     * @param[in] req The rpc req protobuf.
     * @param[out] resp The rpc rsp protobuf.
     * @return K_OK on success; the error code otherwise.
     */
    Status RollbackMultiMeta(const RollbackMultiMetaReqPb &req, RollbackMultiMetaRspPb &rsp);

    /**
     * @brief Get object's L2 cache type.
     * @param[in] objKey The object's ID.
     * @return The object's L2 cache type.
     */
    int GetL2CacheType(const std::string &objKey);

    /**
     * @brief When the worker that the primary copy resides on is faulty, reselect a new copy as the primary copy.
     * @param[in] objectKey Object key to be reselect.
     * @param[in] excludedAddr Specifies the worker address to be excluded.
     * @param[in] accessor A tbb accessor lock for id2location table.
     * @param[out] primaryCopy Reselected primary copy.
     * @return Status of the call.
     */
    Status ReselectPrimaryCopy(const std::string &objectKey, const std::unordered_set<std::string> &excludedAddr,
                               TbbMetaTable::accessor &accessor, std::string &primaryCopy);

    /**
     * @brief Notify cross-az deletion
     * @param[in] objsNeedAsyncNotify The objs grouping by <objectKey, azNames> that need to notify
     */
    void AsyncNotifyCrossAzDelete(const std::unordered_map<std::string, std::vector<std::string>> &objsNeedAsyncNotify);

    /**
     * @brief Get object version.
     * @param[in] objectKey Object key.
     * @param[out] version The version of this object.
     * @return T/F
     */
    bool GetObjectVersion(const std::string &objKey, int64_t &version);

    /**
     * @brief Set expiration time for metas by objectKeys.
     * @param[in] req The request of Expire.
     * @param[out] rsp The response of Expire.
     * @return Status of the call.
     */
    Status Expire(const ExpireReqPb &req, ExpireRspPb &rsp);

protected:
    /**
     * @brief Recovery object locations
     * @param[in] objLocMap The map record object and locations.
     * @return Status of the call
     */
    Status RecoverObjectLocations(
        const std::unordered_map<std::string, std::vector<std::pair<std::string, AckState>>> &objLocMap);

    /**
     * @brief Load object locations
     * @param[in] isFromRocksdb Specifies whether to obtain data from rocksdb.
     * @param[out] objLocMap The map record object and locations.
     * @return Status of the call
     */
    Status LoadObjectLocations(bool isFromRocksdb,
        std::unordered_map<std::string, std::vector<std::pair<std::string, AckState>>> &objLocMap);

    std::shared_timed_mutex metaTableMutex_;
    TbbMetaTable metaTable_;  // Metadata table.
private:
    friend class MasterOCServiceImpl;
    friend class OCNotifyWorkerManager;
    friend class OCGlobalCacheDeleteManager;
    friend class ExpiredObjectManager;
    friend class OCMigrateMetadataManager;

    struct ChangedMeta {
        const std::string &newAddress;
        int64_t newVersion;
        uint64_t newDataSz;
        uint32_t newLifeState;
        const google::protobuf::RepeatedField<uint64_t> &newBlobSizes;
    };

    /**
     * @brief Create meta for binary format.
     * @param[in] newMeta Metadata of object.
     * @param[in] address Server address.
     * @param[in] nestedObjectKeys nested object keys.
     * @param[out] version Object version.
     * @param[out] firstOne create first time or not
     * @return Status of call.
     */
    Status CreateMetaForBinaryFormat(const ObjectMetaPb &newMeta, const std::string &address,
                                     const std::set<ImmutableString> &nestedObjectKeys, int64_t &version,
                                     bool &firstOne);

    /**
     * @brief Check the object update enable or not
     * @param[in] meta meta info
     * @param[in] objectKey object key
     * @param[in] existence update enable or not
     * @param[in] firstOne if the object exist before
     */
    Status CheckExistenceOpt(const ObjectMeta &meta, const std::string &objectKey, const ExistenceOptPb &existence,
                             bool &firstOne);

    /**
     * @brief Do cache invalidation notification for binary format.
     * @note Tbb lock should be acquired before this function call.
     * @param[in] objectKey object key parameters.
     * @param[in] prevMeta Previous object meta.
     * @param[in] changedMeta Changed meta parameters.
     * @return Status of call.
     */
    Status DoBinaryCacheInvalidationUnlocked(const std::string &objectKey, ObjectMeta &prevMeta,
                                             const ChangedMeta &changedMeta);
    /**
     * @brief Create meta entry for the first-time create meta rpc.
     * @param[in] newMeta Metadata of object.
     * @param[in] address Server address.
     * @param[in] version Object version.
     * @param[in] nestedObjectKeys nested object keys.
     * @param[in/out] accessor Tbb table accessor with lock and data.
     * @return Status of call.
     */
    Status CreateMetaFirstTime(const ObjectMetaPb &newMeta, const std::string &address, int64_t version,
                               const std::set<ImmutableString> &nestedObjectKeys, TbbMetaTable::accessor &accessor);

    /**
     * @brief Create pending meta entry for the first-time create meta rpc.
     * @param[in] newMeta Metadata of object.
     * @param[in] address Server address.
     * @param[in] pendingTtl The ttl of pending.
     * @param[out] firstOne if the object exist before.
     * @return Status of call.
     */
    Status CreatePendingMeta(const ObjectMetaPb &newMeta, const std::string &address, int64_t pendingTtl,
                             bool &firstOne);

    /**
     * @brief Update meta info in cache and rocksdb.
     * @param[in] meta The meta info
     * @param[in] newMeta The new meta info
     * @param[in] address The request address
     * @param[in] version The object version
     * @param[out] firstOne Create first time or not
     */
    Status UpdateMeta(ObjectMeta &meta, const ObjectMetaPb &newMeta, const std::string &address, int64_t &version);

    /**
     * @brief Create meta info in cache and rocksdb.
     * @param[in] objectKey The object key.
     * @param[in] newMeta The new meta info
     * @param[in] address The request address
     * @param[in] version The object version
     * @param[out] firstOne Create first time or not
     */
    Status CreateMeta(const std::string &objectKey, ObjectMeta &newMeta, const std::string &address, int64_t &version,
                      bool &firstOne);

    /**
     * @brief Selete primary copy if old primary copy node is crash in scale-in scenarios
     * @param[in] objectKey The key of object.
     * @param[in] primaryAddress The old primary copy address.
     * @param[in] objLocMap The map record object and locations.
     * @return The new primary copy
     */
    std::string SelectPrimaryCopyWhenScaleIn(
        const std::string &objectKey, const std::string &primaryAddress,
        const std::unordered_map<std::string, std::vector<std::pair<std::string, AckState>>> &objLocMap);

    /**
     * @brief Load meta into the cache from the object meta table.
     * @param[in] metas The meta of Objects.
     * @param[in] expireObjects Expired Objects.
     * @param[in] objLocMap The map of local objects.
     * @param[in] isFromRocksdb Specifies whether to obtain data from rocksdb.
     * the current node should be used as the object expiration time, because the steady_clock of different machines may
     * be different. The question needs to be optimized later.
     * @param[in] workerUuids Load the data of specified worker uuids. If the value is empty, obtains the data of the
     * current worker.
     * @param[in] extraRanges Load the data of specified hash ranges if not empty.
     * @return Status of the call.
     */
    Status HandleLoadMeta(std::vector<std::pair<std::string, std::string>> &metas,
        std::vector<std::tuple<std::string, uint64_t, uint32_t>> &expireObjects,
        const std::unordered_map<std::string, std::vector<std::pair<std::string, AckState>>> &objLocMap,
        bool &isFromRocksdb, const std::vector<std::string> &workerUuids, const worker::HashRange &extraRanges);

    /**
     * @brief Load meta into the cache from the object meta table.
     * @param[in] isFromRocksdb Specifies whether to obtain data from rocksdb.
     * the current node should be used as the object expiration time, because the steady_clock of different machines may
     * be different. The question needs to be optimized later.
     * @param[in] workerUuids Load the data of specified worker uuids. If the value is empty, obtains the data of the
     * current worker.
     * @param[in] extraRanges Load the data of specified hash ranges if not empty.
     * @return Status of the call.
     */
    Status LoadMeta(bool isFromRocksdb, const std::vector<std::string> &workerUuids = {},
                    const worker::HashRange &extraRanges = {});

    /**
     * @brief SendChangePrimaryCopy
     * @param[in,out] workerForChangePrimaryIds Need to send change primary copy ids, failed will save, success will be
     * erase.
     * @param[in] rsp RemoveMetaRspPb rsp
     */
    void SendChangePrimaryCopy(
        std::unordered_map<std::string, std::unordered_set<std::string>> &workerForChangePrimaryIds,
        RemoveMetaRspPb &rsp);

    /**
     * @brief Obtains the information about expired objects.
     * @param[in] metaPb The meta info.
     * @param[out] expireObjects The expired object info
     */
    void InsertExpireObjects(ObjectMetaPb &metaPb,
                             std::vector<std::tuple<std::string, uint64_t, uint32_t>> &expireObjects);

    /**
     * @brief Initialize ObjectGlobalRefTable and recovery data from Rocksdb.
     * @return Status of the call.
     */
    Status InitGlobalRef();

    /**
     * @brief Add subscription request to cache in order to publish later.
     * @param[in] subMeta The subscribe meta info.
     * @return Status of the call.
     */
    Status AddSubscribeCache(const std::shared_ptr<SubscribeMeta> &subMeta);

    /**
     * @brief Update subscription request cache after object created.
     * @param[in] objectKey object key parameters.
     * @param[in] objectMeta The created object meta.
     */
    void UpdateSubscribeCache(const std::string &objectKey, const ObjectMeta &objectMeta);

    /**
     * @brief Notify worker which has object data to delete, please attention that when the worker is rpc timeout, we
     * will consider the worker failed to process, and put the objectKeys into failedObjects
     * @param[in] sourceWorker The worker initiates to delete object.
     * @param[in] sendAllDelObjs All holdings of obj that will be evicted and can send to sourceWorker.
     * @param[in] isAsync Is async process mode.
     * @param[out] failedObjects The failed object list.
     * @return Status of the call.
     */
    Status NotifyWorkerDelete(const std::string &sourceWorker,
                              const std::unordered_map<std::string, DeleteStruct> &sendAllDelObjs, bool isAsync,
                              std::unordered_set<std::string> &failedObjects);

    /**
     * @brief Get deleted object meta infos.
     * @param[in] objectKey The id of object.
     * @param[out] replicas The workers which hold the object.
     * @param[out] delMediator The object delete mediator to record delete state.
     * @return Status of the call.
     */
    Status GetMetaInfoAndSetDeleting(const std::string &objectKey, DeleteStruct &replicas,
                                     DeleteObjectMediator &delMediator);

    /**
     * @brief Clear object meta infos of server in cache rocksdb by object key.
     * @note Shred lock of metaTableMutex_ should be acquired before this function call.
     * @param[in] sendAllDelObjs bfs id.
     * @param[in] isExpired Is expired delete.
     * @param[in/out] failedObjects worker delete failed object keys.
     * @return Status of the call.
     */
    Status ClearMetaInfo(const std::unordered_map<std::string, DeleteStruct> &sendAllDelObjs, bool isExpired,
                         std::unordered_set<std::string> &failedObjects, DeleteObjectMediator &delMediator);

    /**
     * @brief Clear one object meta infos of server in rocksdb
     * @param[in] accessor The TBB accessor.
     * @param[in] isDataMigration Indicates whether the data migration scenario is used. If the value is true, data in
     * etcd is not deleted.
     * @return Status of the call.
     */
    Status ClearOneMetaInfo(const TbbMetaTable::const_accessor &accessor, bool isDataMigration = false);

    /**
     * @brief Remove subscribe cache info after the request timeout.
     * @param[in] requestId Subscribe request id.
     */
    void RemoveSubscribeCache(const std::string &requestId);

    /**
     * @brief Remove object meta location.
     * @param[in] objectKey The object to remove.
     * @param[in] address The worker address of the object.
     * @param[in] version Object version to remove.
     * @return Status of the call.
     */
    Status RemoveMetaLocation(const std::string &objectKey, const std::string &address, uint64_t version = UINT64_MAX);

    /**
     * @brief Remove object meta location.
     * @param[in] request The rpc request protobuf.
     * @param[in] address The worker address of the object.
     * @param[out] response The rpc response protobuf.
     * @param[in] version Object version to remove.
     */
    void RemoveMetaLocation(const RemoveMetaReqPb &request, const std::string &address, RemoveMetaRspPb &response,
                            uint64_t version = UINT64_MAX);

    /**
     * @brief Remove object meta info for invalidating buffer operation.
     * @param[in] request The rpc request protobuf.
     * @param[in] address The worker address of the object.
     * @param[out] response The rpc response protobuf.
     */
    void RemoveMetaForInvalidateBuffer(const RemoveMetaReqPb &request, const std::string &address,
                                       RemoveMetaRspPb &response);

    /**
     * @brief GiveUpPrimaryLocation
     * @param[in] request Request need to remoove meta
     * @param[in] address Request address.
     * @param[out] response Rsp of RemoveMetaRspPb
     */
    void GiveUpPrimaryLocation(const RemoveMetaReqPb &request, const std::string &address, RemoveMetaRspPb &response);

    /**
     * @brief ChangePrimaryCopy
     * @param[in] primaryAddr Need to change primary addr.
     * @param[in] objectKey object key.
     * @return Status of the call.
     */
    Status ChangePrimaryCopy(const std::string &primaryAddr, const std::string &objectKey);

    /**
     * @brief RetryForFailedIds
     * @param[in] workerForChangePrimaryIds Need to retry change primary addr.
     * @param[in] rsp Rsp of removemeta.
     */
    void RetryForFailedIds(
        const std::unordered_map<std::string, std::unordered_set<std::string>> &workerForChangePrimaryIds,
        RemoveMetaRspPb &rsp);

    /**
     * @brief Notify worker to delete objects.
     * @param[in] sourceWorker The source worker address.
     * @param[in] obj2Replicas The workers which hold the object but cannot send to sourceWorker.
     * @param[in] sendAllDelObjs All holdings of obj that will be evicted and can be sent to sourceWorker.
     * @param[out] response The response protobuf.
     * @return Status of the call.
     */
    Status NotifyWorkerDeleteImpl(const std::string &sourceWorker,
                                  const std::unordered_map<std::string, std::set<std::string>> &obj2Replicas,
                                  const std::unordered_map<std::string, std::set<std::string>> &sendAllDelObjs,
                                  DeleteAllCopyMetaRspPb &response);

    /**
     * @brief Get and clear all copies that died because the objects life cycle ended.
     * @param[in] objectKeys The object keys.
     * @param[out] sendAllReplicas The object need to evict in all worker.
     * @param[out] toBeNotifiedNestedRefs The objects that belong to a different master that need to be notified
     * @param[out] delMediator The object delete mediator to record delete state.
     */
    void GetAndClearAllUnKeepMetas(const std::unordered_set<std::string> &objectKeys,
                                   std::unordered_map<std::string, DeleteStruct> &sendAllReplicas,
                                   std::vector<std::string> &toBeNotifiedNestedRefs, DeleteObjectMediator &delMediator);

    /**
     * @brief An recursive function for BFS to search dead objects.
     * Using the example in oc_nested_manager.h:
     *  1.parentId : ObjA end of life, childId ObjB,ObjC,ObjD refCount decrease 1 and get zero count ids {ObjB}.
     *  2.check if zero count ids ({ObjB}) from previous step are end of life, if true then next step.
     *  3.set all dead id as parentId. parentId : ObjB, childId ObjC refCount decrease 1 and get zero count ids {ObjC}.
     *  4.check if zero count ids ({ObjC}) from previous step are end of life, if true then next step.
     *  5.set all dead id as parentId. parentId : ObjC, childId ObjD refCount decrease 1 and get zero count ids {ObjD}.
     *  6.check if zero count ids ({ObjD}) from previous step are end of life, if true then next step.
     *  7.set all dead id as parentId. parentId : ObjC, childId empty, end of BFS and get final vector {ObjB,ObjC,ObjD}.
     * @param[in] beginDeadObject The parent obj in nested relationship which can be evicted.
     * @param[out] finalDeadObjects All objects that can be evicted in dataSystem.
     * @param[out] toBeNotifiedNestedRefs All nested refs of the objects that belong to other masters
     * @return Status of the call.
     */
    Status BFSGetDeadObjects(const std::unordered_set<std::string> &beginDeadObject,
                             std::vector<std::string> &finalDeadObjects,
                             std::vector<std::string> &toBeNotifiedNestedRefs);

    /**
     * @brief Create the hash object metadata.
     * @param[in] newMeta Metadata of object.
     * @param[in] address Server address.
     * @param[in] isMultiset if Multiset or not.
     * @return Status of the call.
     */
    Status CreateHashMeta(const ObjectMetaPb &meta, const std::string &address);

    /**
     * @brief Modify the primary copy record in the master table.
     * @param[in] objectKey Object key to be modify.
     * @param[in] workerId New primary copy.
     * @param[in] ifvoluntaryScaleDown Judge whether the worker is voluntary scale down,
     * a dead worker don't need to add PRIMARY_COPY_INVALID.
     */
    void ModifyPrimaryCopy(const std::string &objectKey, const std::string &workerId, bool ifvoluntaryScaleDown);

    /**
     * @brief Recover Metadata from Worker.
     * @param[in] workerAddr Worker address that send metadata.
     * @param[in] meta metadata to be recovery.
     * @return Status of the call.
     */
    Status RecoveryMetaFromWorker(const std::string &workerAddr, const ObjectMetaPb &meta);

    /**
     * @brief Select a location of object for read.
     * @param[in] objectKey Object key to be read.
     * @param[in] sourceWorker Request worker address.
     * @param[in] locations Alternative Locations.
     * @return Selected location.
     */
    std::string SelectObjectLocation(const std::string &objectKey, const std::string &sourceWorker,
                                     const std::unordered_map<ImmutableString, AckState> &locations);

    template <class F, class... Args>
    void ExecuteAsyncTask(F &&f, Args &&... args)
    {
        // if interrupt manager, stop to execute task.
        if (!interruptFlag_) {
            asyncPool_->Execute(f, args...);
        }
    }

    /**
     * @brief Delete all copy by expired mechanism asynchronously.
     * @param[in/out] deletedObjectKeys The object delete mediator to record delete state.
     */
    void AsyncDeleteByExpired(DeleteObjectMediator &deletedObjectKeys);

    /**
     * @brief Set DeleteAllCopyMetaRspPb to return to worker.
     * @param[in] status The final DeleteAllCopyMeta status.
     * @param[in] failedObjectKeys The objectKeys delete failed in master.
     * @param[out] response The rpc response protobuf.
     */
    template <typename container>
    static void SetDeleteAllCopyMetaRspPb(const Status &status, const container &failedObjectKeys,
                                          DeleteAllCopyMetaRspPb &response)
    {
        for (const auto &failedId : failedObjectKeys) {
            response.add_failed_object_keys(failedId);
        }
        response.mutable_last_rc()->set_error_code(status.GetCode());
        response.mutable_last_rc()->set_error_msg(status.GetMsg());
    }

    /**
     * @brief Rollback the refcount if objects delete failed in GDecreaseRef process.
     * @param[in] delMediator The mediator that store delete state.
     * @param[out] failedDecIds The objectKeys GDecreaseRef failed.
     * @param[in] clientId The remote client id.
     */
    void RollbackIfGDecRefFail(DeleteObjectMediator &delMediator, std::vector<std::string> &failedDecIds,
                               const std::string &remoteClientId = "");

    /**
     * @brief Set GDecreaseRefRspPb to return to worker.
     * @param[in] status The final GDecreaseRef status.
     * @param[in] failedObjectKeys The objectKeys GDecreaseRef failed in master.
     * @param[in] noRefIds The objectKeys don't have any refcount.
     * @param[in] processFinished Whether the GDecraseRef process in master is finished.
     * If true, worker don't need to wait for master's NotifyGDecreaseRefResultReqPb request,
     * worker can get delete result in GDecreaseRefRspPb directly.
     * @param[out] response The rpc response protobuf.
     */
    static void SetGDecreaseRefRspPb(const Status &status, const std::vector<std::string> &&failedDecIds,
                                     std::vector<std::string> &&noRefIds, GDecreaseRspPb &resp);

    /**
     * @brief Try to subscribe cache
     * @param[in] timeout The remaining timeout.
     * @param[in] reqPb The request of QueryMeta.
     * @param[in] objectKeys The objectKeys whose metadata not in metaTable.
     */
    Status TryToSubscribeCache(int64_t timeout, const QueryMetaReqPb &reqPb, std::list<std::string> &objectKeys);

    /**
     * @brief Save one obj meta.
     * @param objMeta ObjMeta info.
     * @param status Last error status.
     * @return True if save success.
     */
    bool SaveOneMeta(const MetaForMigrationPb &objMeta, Status &status);

    /**
     * @brief Transfer to the deletion thread to process the sync delete process.
     * @param[in] deleteMediator Delete mediator.
     * @param[in] response DeleteAllCopyMeta response.
     * @param[in] serverApi DeleteAllCopyMeta server api.
     */
    void TransferSyncDeleteRequest(
        DeleteObjectMediator &deleteMediator, DeleteAllCopyMetaRspPb &response,
        const std::shared_ptr<ServerUnaryWriterReader<DeleteAllCopyMetaRspPb, DeleteAllCopyMetaReqPb>> &serverApi);

    /**
     * @brief Wait for the initialization of meta manager completed.
     */
    void WaitInitializaiton();

    /**
     * @brief Try get the object data directly.
     * @param[in] objectKey Object key.
     * @param[in] accessor Metatable TBB lock.
     * @param[in out] payloadSize Payload size get directly.
     * @param[out] queryMeta QueryMeta response, if we get data success, the payload index would be saved here.
     * @param[out] payloads If we get data success, the data would be saved here.
     */
    void TryGetObjectData(const std::string &objectKey, const TbbMetaTable::accessor &accessor, uint64_t &payloadSize,
                          QueryMetaInfoPb &queryMeta, std::vector<RpcMessage> &payloads);

    /**
     * @brief Add heavy operation.
     * @param[in] objectKey Object key.
     * @return True if no heavy op now.
     */
    bool AddHeavyOp(const std::string &objectKey);

    /**
     * @brief Add heavy operation via object key list.
     * @param[in] objectKeys Object key list.
     * @return True if no heavy op now.
     */
    bool AddHeavyOp(const std::vector<std::string> &objectKeys);

    /**
     * @brief Remove heavy op via object key list.
     * @param[in] objectKeys Object key list.
     */
    void RemoveHeavyOp(const std::vector<std::string> &objectKeys);

    /**
     * @brief Async notify worker to GDecreaseRef.
     * @param[in] delMediator The object delete mediator to record delete state..
     * @param[in] failedDecIds The objectKeys GDecreaseRef failed..
     * @param[in] resp The rpc response protobuf.
     * @param[in] serverApi The WriterReader in server side which holds unary rpc socket.
     * @param[in] clientId The remote client id.
     */
    void AsyncNotifyWorkerGDec(
        DeleteObjectMediator &delMediator, std::vector<std::string> &failedDecIds, GDecreaseRspPb &resp,
        const std::shared_ptr<ServerUnaryWriterReader<GDecreaseRspPb, GDecreaseReqPb>> &serverApi,
        const std::string &remoteClientId = "");

    /**
     * @brief Check whether rocksdb is trustworthy and determine where to load the l2_cache table from.
     * @param[in] tablePrefix ETCD table prefix.
     * @param[in] rocksTable Need write rocksdb table.
     * @param[in] isFromRocksdb Specifies whether to obtain data from rocksdb.
     * @param[in] workerUuids Load the data of specified worker uuids. If the value is empty, obtains the data of the
     * current worker.
     * @param[in] extraRanges Load the data of specified hash ranges if not empty.
     * @param[out] outMetas The output metas in table.
     * @return Status of the call.
     */
    Status CheckRocksdbStatusAndLoadL2Table(const std::string &tablePrefix, const std::string &rocksTable,
                                            bool isFromRocksdb, const std::vector<std::string> &workerUuids,
                                            const worker::HashRange &extraRanges,
                                            std::vector<std::pair<std::string, std::string>> &outMetas);

    /**
     * @brief Check whether meta is updating.
     * @param[in] objKey The object key.
     * @param[out] version The updating meta version.
     * @return T/F
     */
    bool CheckIfUpdating(const std::string &objKey, int64_t &version);

    /**
     * @brief Start meta monitor thread.
     */
    void StartMetaMonitor();

    /**
     * @brief Notify nodes in other clusters to delete the metadata of an object.
     * Note: Hash type keys will care about this.
     * @param[in] objectKey The object key.
     * @param[in] version The new meta's version.
     * @param[in] type write type.
     * @return Status of the call.
     */
    Status NotifyOtherAzNodeRemoveMeta(const std::string &objectKey, int64_t version, ObjectMetaStore::WriteType type);

    /**
     * @brief Process remove meta notification from other az.
     * @param[in] request The rpc request.
     * @param[out] response The rpc response.
     */
    void ProcessRemoveMetaNotifyFromOtherAz(const RemoveMetaReqPb &request, RemoveMetaRspPb &response);

    /**
     * @brief Mark meta is updating and update remove meta notification
     * @param[in] objectKey The object key.
     * @param[in] version The meta version.
     * @param[out] raiiP The tmp lock cleaner.
     */
    void MarkUpdatingAndUpdateRemoveMetaNotification(const std::string &objectKey, int64_t version, RaiiPlus &raiiP);

    /**
     * @brief Forward delete all copy meta request to other az.
     * @param[in] objsNeedTryInOtherAz The object key.
     * @param[out] delMediator The object delete mediator to record delete state.
     */
    void ForwardDeleteAllCopyMeta2OtherAz(std::unordered_set<std::string> &&objsNeedTryInOtherAz,
                                          DeleteObjectMediator &deleteMediator);

    /**
     * @brief Process hash objs without meta when delete all copy meta.
     * @param[in] request DeleteAllCopyMeta request.
     * @param[in] hashObjsWithoutMeta The object key.
     * @param[out] response DeleteAllCopyMeta response.
     * @param[out] delMediator The object delete mediator to record delete state.
     */
    void ProcessHashObjsWithoutMetaWhenDeleteAllCopyMeta(const DeleteAllCopyMetaReqPb &request,
                                                         std::unordered_set<std::string> &&hashObjsWithoutMeta,
                                                         DeleteAllCopyMetaRspPb &response,
                                                         DeleteObjectMediator &deleteMediator);

    /**
     * @brief Process hash objs without meta when delete all copy meta.
     * @param[in] otherAZName The other AZ name.
     * @param[in] objKeysGrpByMaster The object key grouped by master.
     * @param[out] objsNeedTryInOtherAz The objects needed to be retried in other az.
     * @param[out] objsNeedAsyncNotify Objects need to be added to a queue that notifies other nodes asynchronously.
     * @param[out] delMediator The object delete mediator to record delete state.
     */
    void ProcessForwardDeleteAllCopyMetaInCurrAz(
        const std::string &otherAZName,
        const std::unordered_map<MetaAddrInfo, std::vector<std::string>> &objKeysGrpByMaster,
        std::unordered_set<std::string> &objsNeedTryInOtherAz,
        std::unordered_map<std::string, std::vector<std::string>> &objsNeedAsyncNotify,
        DeleteObjectMediator &deleteMediator);

    bool IsPrimaryCopyWithCopy(const ObjectMeta &meta, const std::string &address);

    std::set<std::string> GetValidWorkersInHashRing();

    /**
     * @brief Check if the object is binary.
     * @param[in] address The location address.
     * @param[in] objectKey The object key.
     * @param[in] version The object version.
     * @param[in] dataFormat The object data format.
     * @param[out] accessor The accessor for meta table.
     * @param[out] isExpired The version is expired.
     * @return Status of the call.
     */
    Status ProcessCopyMetaHelper(const std::string &address, const std::string &objectKey, uint64_t version,
                                    uint32_t dataFormat, TbbMetaTable::accessor &accessor, bool &isExpired);

    std::string masterAddress_;

    std::shared_ptr<AkSkManager> akSkManager_;
    std::unique_ptr<object_cache::WorkerLocalWorkerOCApi> localApi_;

    std::shared_ptr<ObjectMetaStore> objectStore_{ nullptr };                        // Metadata store for object.
    std::unique_ptr<object_cache::ObjectGlobalRefTable<ImmutableString>> globalRefTable_{
        nullptr
    };                                                                               // object global reference.
    std::unique_ptr<OCNestedManager> nestedRefManager_{ nullptr };                   // object nested reference manager.

    TbbRemoteClientIdRefTable clientIdRefTable_;     // remote client id to master ip table
    std::shared_timed_mutex clientIdRefTableMutex_;  // mutex for clientIdRefTable

    std::atomic<bool> interruptFlag_{ false };

    std::shared_timed_mutex subTableMutex_;
    // The hash table maps the query request id to subscribe meta info.
    TbbSubMetaTable request2SubMeta_;
    // The hash table maps the object key to a set of the query requests on this object.
    TbbReqIdTable objKey2ReqId_;

    std::unique_ptr<OCNotifyWorkerManager> notifyWorkerManager_{ nullptr };
    std::unique_ptr<OCGlobalCacheDeleteManager> globalCacheDeleteManager_{ nullptr };
    std::unique_ptr<ExpiredObjectManager> expiredObjectManager_{ nullptr };
    std::shared_ptr<MasterDevOcManager> masterDevOcManager_{ nullptr };
    std::unique_ptr<ThreadPool> asyncTaskPool_{ nullptr };

    // All async task in master should execute in asyncPool_ for centralized control.
    // Each manager keep its main processes in its own life cycle to avoid asyncPool_ starvation.
    std::unique_ptr<ThreadPool> asyncPool_{ nullptr };
    bool backendStoreExist_ = false;

    // Protects heavyOps_
    std::mutex heavyOpMutex_;
    // Record heavy op such as causal set or sync del
    std::unordered_set<ImmutableString> heavyOps_;

    // Protects isDeletingObjs_
    std::shared_mutex isDeletingObjMutex_;
    // Record objects id deleting.
    std::unordered_set<ImmutableString> isDeletingObjs_;

    // Protects updatingObjsTable_
    std::shared_timed_mutex updatingObjsTableMutex_;
    // <objKey, version>. Record the object whose metadata is being updated
    std::unordered_map<ImmutableString, int32_t> updatingObjsTable_;

    std::atomic<bool> initialized_{ false };
    std::string eventName_;
    std::unique_ptr<Thread> monitor_;
    std::string dbName_;
    std::shared_ptr<PersistenceApi> persistApi_;

    // Indicate the master is new added node or not.
    const bool newNode_;
};
}  // namespace master
}  // namespace datasystem
#endif  // DATASYSTEM_MASTER_OBJECT_CACHE_OC_METADATA_MANAGER_H
