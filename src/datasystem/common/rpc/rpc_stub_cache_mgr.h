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

#ifndef DATASYSTEM_COMMON_RPC_ZMQ_STUB_CACHE_MGR_H
#define DATASYSTEM_COMMON_RPC_ZMQ_STUB_CACHE_MGR_H

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <string>

#include <brpc/channel.h>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/lru/lru_cache.h"
#include "datasystem/common/rpc/rpc_auth_key_manager.h"
#include "datasystem/common/rpc/rpc_credential.h"
#include "datasystem/common/rpc/rpc_stub_base.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/object_posix.pb.h"
#include "datasystem/utils/status.h"

namespace datasystem {

// brpc uses the same port as ZMQ (the one specified by --worker_address / --master_address).
// No offset: FLAGS_use_brpc selects which server listens on the port (ZMQ or brpc, not both).
constexpr int kBrpcPortOffset = 0;
constexpr int kBrpcConnMaxRetries = 30;
constexpr int kBrpcConnRetryIntervalUs = 100000;

// Wait for brpc health check to establish TCP connection to the given brpc address.
// Returns true if the socket becomes available within maxRetries * intervalUs.
bool WaitForBrpcSocketAvailable(const HostPort& brpcAddr, int maxRetries = kBrpcConnMaxRetries,
                                int intervalUs = kBrpcConnRetryIntervalUs);

enum class StubPriority : int {
    HIGH = 0,
    LOW = 1,
    INVALID = 100
};

enum class StubType : int {
    WORKER_WORKER_OC_SVC = 0,
    WORKER_MASTER_OC_SVC = 1,
    MASTER_WORKER_OC_SVC = 2,
    WORKER_WORKER_SC_SVC = 3,
    WORKER_MASTER_SC_SVC = 4,
    MASTER_WORKER_SC_SVC = 5,
    MASTER_MASTER_OC_SVC = 6,
    WORKER_WORKER_TRANS_SVC = 7,
    TO_COORDINATOR_SVC = 8,
    COORDINATOR_WORKER_SVC = 9,
    CLIENT_WORKER_SC_SVC = 10,
#ifdef WITH_TESTS
    TEST_TYPE_1 = 1000,
    TEST_TYPE_2 = 1001,
    TEST_TYPE_3 = 1002,
#endif
};

namespace stub_priority {
StubPriority GetStubPriority(StubType type);
}  // namespace stub_priority

class RpcStubCacheMgrObj : public LruCountPolicyObjBase<RpcStubCacheMgrObj> {
public:
    RpcStubCacheMgrObj(HostPort hostPort, StubType type)
        : LruCountPolicyObjBase<RpcStubCacheMgrObj>(static_cast<int>(stub_priority::GetStubPriority(type))),
          hostPort_(std::move(hostPort)),
          type_(type){};

    ~RpcStubCacheMgrObj() = default;

    Status EvictImpl(EvictionCtrl evictFlags)
    {
        (void)evictFlags;
        RaiiPlus raiiP;
        if (!lockedExternally_) {
            mutex_.lock();
            raiiP.AddTask([this]() { mutex_.unlock(); });
        }
        auto useCount = data_.use_count();
        if (useCount > 1) {  // Check the number of shart_ptr holders to ensure that evict can really release fd.
            RETURN_STATUS(K_TRY_AGAIN, FormatString("Current use count: %ld, can not evict.", useCount));
        }
        return Status::OK();
    }

    std::string ToStringImpl() const
    {
        std::string dataMsg = data_ == nullptr ? "nullptr" : "useful data";
        return hostPort_.ToString() + "-" + std::to_string(static_cast<int>(type_)) + "-" + dataMsg;
    };

    void SetDataWithoutLck(const std::shared_ptr<RpcStubBase> &rpcStub)
    {
        data_ = rpcStub;
    }

    std::shared_ptr<RpcStubBase> GetData()
    {
        std::shared_lock<std::shared_timed_mutex> lck(mutex_);
        return data_;
    }

    void GetWriteLck()
    {
        mutex_.lock();
        lockedExternally_ = true;
    }

    void ReleaseWriteLck()
    {
        mutex_.unlock();
        lockedExternally_ = false;
    }

private:
    std::atomic<bool> lockedExternally_{ false };
    std::shared_timed_mutex mutex_;
    std::shared_ptr<RpcStubBase> data_;
    HostPort hostPort_;
    StubType type_;
};

class HashKeyForRpcStubCacheMgr : public HashKey<HashKeyForRpcStubCacheMgr> {
public:
    HashKeyForRpcStubCacheMgr() = default;
    HashKeyForRpcStubCacheMgr(const HostPort &hostPort, StubType stubType)
        : hostPort_(hostPort), stubType_(stubType){};

    ~HashKeyForRpcStubCacheMgr() = default;

    bool EqualImpl(const HashKeyForRpcStubCacheMgr *cmp) const
    {
        return (this->hostPort_ == cmp->hostPort_ && this->stubType_ == cmp->stubType_);
    }

    std::string ToStringImpl() const
    {
        return hostPort_.ToString() + "-" + std::to_string(static_cast<int>(stubType_));
    };

    static std::size_t HashImpl(const HashKeyForRpcStubCacheMgr *k)
    {
        size_t h1 = std::hash<std::string>()(k->hostPort_.ToString());
        size_t h2 = std::hash<StubType>()(k->stubType_);
        return h1 ^ (h2 << 1);
    }

    HostPort hostPort_;
    StubType stubType_;
};

class RpcStubCacheMgr {
public:
    /**
     * @brief Singleton mode, obtaining instance.
     * @return Instance of RpcStubCacheMgr.
     */
    static RpcStubCacheMgr &Instance()
    {
        static RpcStubCacheMgr instance;
        return instance;
    }

    /**
     * @brief Init RpcStubCacheMgr.
     * @param[in] maxStubCount Maximum number of stubs to cache.
     * @param[in] localAddress ip address of this process (necessary for urma case, but optional otherwise).
     * @return Status of the call.
     */
    Status Init(uint64_t maxStubCount, const HostPort &localAddress = HostPort());

    /**
     * @brief Get stub.
     * @param[in] hostPort The host and port of the stub.
     * @param[in] type The type of the stub.
     * @param[out] rpcStub Obtained stub.
     * @return Status of the call.
     */
    Status GetStub(const HostPort &hostPort, StubType type, std::shared_ptr<RpcStubBase> &rpcStub);

    /**
     * @brief Verify the cached brpc stub's underlying socket is still alive; evict if stale.
     *
     * Only meaningful under FLAGS_use_brpc. Resets rpcStub to nullptr when the cached
     * channel's socket is dead so the caller falls through to stub recreation.
     *
     * @param[in] hostPort The worker address (zmq port convention).
     * @param[in] type The stub type.
     * @param[in,out] rpcStub The cached stub; reset to nullptr if evicted.
     */
    void MaybeEvictStaleBrpcStub(const HostPort &hostPort, StubType type, std::shared_ptr<RpcStubBase> &rpcStub);

    /**
     * @brief Remove stub.
     * @param[in] hostPort The host and port of the stub.
     * @param[in] type The type of the stub.
     * @return Status of the call.
     */
    Status Remove(const HostPort &hostPort, StubType type);

    uint32_t Size()
    {
        return lruCache_->Size();
    }

protected:
    using LruForRpcStubCacheMgr =
        LruCache<HashKeyForRpcStubCacheMgr, std::shared_ptr<RpcStubCacheMgrObj>, LruCountPolicy>;
    using RpcStubCacheCreateFunc = std::function<Status(const HostPort &hostPort, std::shared_ptr<RpcStubBase> &)>;

    RpcStubCacheMgr() = default;

    /**
     * @brief Init stub creators.
     */
    void InitCreators();

    /**
     * @brief Enable object cache worker worker direct tcp port or not.
     * @return True if worker worker direct port is enable.
     */
    static bool EnableOcWorkerWorkerDirectPort();

    /**
     * @brief Enable worker worker direct tcp port or not.
     * @return True if worker worker direct port is enable.
     */
    static bool EnableScWorkerWorkerDirectPort();

    static Status CreateRpcStub(StubType type, const std::shared_ptr<RpcChannel> &channel,
                                std::shared_ptr<RpcStubBase> &stub);

    static Status CreateRpcChannel(const HostPort &hostPort, const std::string &serviceName,
                                   std::shared_ptr<RpcChannel> &channel, size_t poolSize = 0);

    static Status CreateBrpcStub(StubType type, const std::shared_ptr<brpc::Channel> &brpcChannel,
                                 std::shared_ptr<RpcStubBase> &stub);

    static Status CreateBrpcChannel(const HostPort &hostPort, std::shared_ptr<brpc::Channel> &brpcChannel);

    template <typename CreateRpcChannelFunc>
    static Status CreatorTemplate(CreateRpcChannelFunc &&createRpcChannelFunc, StubType stubType,
                                  std::shared_ptr<RpcStubBase> &rpcStub)
    {
        std::shared_ptr<RpcChannel> channel;
        RETURN_IF_NOT_OK(createRpcChannelFunc(channel));
        return CreateRpcStub(stubType, channel, rpcStub);
    }

    /**
     * @brief Brpc version of CreatorTemplate that creates a brpc::Channel and _BrpcGenericStub.
     * Uses a holder struct to keep the brpc::Channel alive for the lifetime of the stub.
     */
    template <typename CreateBrpcChannelFunc>
    static Status BrpcCreatorTemplate(CreateBrpcChannelFunc &&createBrpcChannelFunc, StubType stubType,
                                      std::shared_ptr<RpcStubBase> &rpcStub)
    {
        // Holder keeps both the channel and stub alive; aliased shared_ptr exposes only the RpcStubBase.
        struct BrpcChannelStubHolder {
            std::shared_ptr<brpc::Channel> channel;
            std::shared_ptr<RpcStubBase> stub;
        };
        auto holder = std::make_shared<BrpcChannelStubHolder>();
        RETURN_IF_NOT_OK(createBrpcChannelFunc(holder->channel));
        RETURN_IF_NOT_OK(CreateBrpcStub(stubType, holder->channel, holder->stub));
        // Aliased shared_ptr: control block is holder's, stored pointer is the stub.
        rpcStub = std::shared_ptr<RpcStubBase>(holder, holder->stub.get());
        return Status::OK();
    }

    std::mutex initMutex_;  // Avoid repeated initialization.
    bool init_ = false;
    std::unique_ptr<LruForRpcStubCacheMgr> lruCache_ = nullptr;
    std::unordered_map<StubType, RpcStubCacheCreateFunc> creators_;
    const static int stubPriorityNum_ = 2;
    HostPort localAddress_;
};
};  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_ZMQ_STUB_CACHE_MGR_H
