/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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
 * Description: Urma resource manager.
 */

#ifndef DATASYSTEM_COMMON_RDMA_URMA_RESOURCE_H
#define DATASYSTEM_COMMON_RDMA_URMA_RESOURCE_H

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <tbb/concurrent_hash_map.h>

#include <ub/umdk/urma/urma_api.h>
#include <ub/umdk/urma/urma_ubagg.h>

#include "datasystem/common/rdma/rdma_util.h"
#include "datasystem/common/rdma/urma_info.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/protos/meta_transport.pb.h"
#include "datasystem/utils/status.h"

#define URMA_DISABLE_COPY_AND_MOVE(CLASS)     \
    CLASS(const CLASS &) = delete;            \
    CLASS &operator=(const CLASS &) = delete; \
    CLASS(CLASS &&) = delete;                 \
    CLASS &operator=(CLASS &&) = delete

namespace datasystem {
class UrmaConnection;
class UrmaJetty;
class UrmaResource;
enum class JettyType : uint8_t {
    SEND = 0,
    RECV = 1,
};
class UrmaEvent : public Event {
public:
    enum class OperationType : uint8_t { UNKNOWN = 0, READ = 1, WRITE = 2 };

    /**
     * @brief Construct an Urma event for an in-flight request.
     * @param[in] requestId Unique request id.
     * @param[in] jetty Jetty snapshot at request-submit time. The owning connection is
     *            recovered via jetty->GetConnection() when needed for recovery.
     * @param[in] remoteAddress Remote address string for logging.
     * @param[in] remoteInstanceId Remote instance id for logging.
     * @param[in] dataSize Request data size for logging.
     * @param[in] operationType Read or write.
     * @param[in] waiter Optional waiter used for notification.
     */
    UrmaEvent(uint64_t requestId, std::weak_ptr<UrmaJetty> jetty, std::string remoteAddress,
              std::string remoteInstanceId, uint64_t dataSize, OperationType operationType,
              std::shared_ptr<EventWaiter> waiter = nullptr)
        : Event(requestId, std::move(waiter)),
          jetty_(std::move(jetty)),
          remoteAddress_(std::move(remoteAddress)),
          remoteInstanceId_(std::move(remoteInstanceId)),
          dataSize_(dataSize),
          operationType_(operationType)
    {
    }

    ~UrmaEvent() = default;

    /**
     * @brief Mark the event as failed and store the completion status code.
     * @param[in] statusCode Completion status code returned by Urma.
     */
    void SetFailed(int statusCode)
    {
        failed_ = true;
        statusCode_ = statusCode;
    }

    /**
     * @brief Get the connection that owned the Jetty at request-submit time.
     *        Derived from jetty_->GetConnection() rather than stored directly.
     * @return Weak pointer to the associated connection (empty if Jetty expired).
     */
    std::weak_ptr<UrmaConnection> GetConnection() const;

    /**
     * @brief Get the Jetty associated with this request when it was submitted.
     * @return Weak pointer to the request Jetty.
     */
    std::weak_ptr<UrmaJetty> GetJetty() const
    {
        return jetty_;
    }

    /**
     * @brief Get the recorded Urma status code.
     * @return Urma completion status code.
     */
    int GetStatusCode() const
    {
        return statusCode_;
    }

    const std::string &GetRemoteAddress() const
    {
        return remoteAddress_;
    }

    uint64_t GetDataSize() const
    {
        return dataSize_;
    }

    const std::string &GetRemoteInstanceId() const
    {
        return remoteInstanceId_;
    }

    OperationType GetOperationType() const
    {
        return operationType_;
    }

    static const char *OperationTypeName(OperationType type)
    {
        switch (type) {
            case OperationType::READ:
                return "READ";
            case OperationType::WRITE:
                return "WRITE";
            default:
                return "UNKNOWN";
        }
    }

private:
    int statusCode_{ 0 };
    std::weak_ptr<UrmaJetty> jetty_;
    std::string remoteAddress_;
    std::string remoteInstanceId_;
    uint64_t dataSize_{ 0 };
    OperationType operationType_{ OperationType::UNKNOWN };
};

class UrmaContext {
public:
    explicit UrmaContext(urma_context_t *raw) : raw_(raw)
    {
    }
    ~UrmaContext();

    URMA_DISABLE_COPY_AND_MOVE(UrmaContext);

    /**
     * @brief Create a local Urma context on the specified device and EID.
     * @param[in] device Local Urma device.
     * @param[in] eidIndex EID index to bind.
     * @param[out] context Created context wrapper.
     * @return Status of the call.
     */
    static Status Create(urma_device_t *device, uint32_t eidIndex, std::unique_ptr<UrmaContext> &context);

    /**
     * @brief Switch bonding context to balance aggregation mode.
     * @return Status of the call.
     */
    Status ChangeBondingBalanceMode() const;

    /**
     * @brief Get the underlying Urma context handle.
     * @return Raw Urma context handle.
     */
    urma_context_t *Raw() const
    {
        return raw_;
    }

private:
    urma_context_t *raw_ = nullptr;
};

class UrmaJfce {
public:
    explicit UrmaJfce(urma_jfce_t *raw) : raw_(raw)
    {
    }
    ~UrmaJfce();

    URMA_DISABLE_COPY_AND_MOVE(UrmaJfce);

    /**
     * @brief Create the event channel used by JFC.
     * @param[in] context Local Urma context.
     * @param[out] jfce Created JFCE wrapper.
     * @return Status of the call.
     */
    static Status Create(urma_context_t *context, std::unique_ptr<UrmaJfce> &jfce);

    /**
     * @brief Get the underlying JFCE handle.
     * @return Raw JFCE handle.
     */
    urma_jfce_t *Raw() const
    {
        return raw_;
    }

private:
    urma_jfce_t *raw_ = nullptr;
};

class UrmaJfc {
public:
    explicit UrmaJfc(urma_jfc_t *raw) : raw_(raw)
    {
    }
    ~UrmaJfc();

    URMA_DISABLE_COPY_AND_MOVE(UrmaJfc);

    /**
     * @brief Create a JFC for completion polling or event notification.
     * @param[in] context Local Urma context.
     * @param[in] deviceAttr Attributes of the selected Urma device.
     * @param[in] jfce Event channel used by the JFC.
     * @param[out] jfc Created JFC wrapper.
     * @return Status of the call.
     */
    static Status Create(urma_context_t *context, const urma_device_attr_t &deviceAttr, urma_jfce_t *jfce,
                         std::unique_ptr<UrmaJfc> &jfc);

    /**
     * @brief Rearm the JFC when event mode is enabled.
     * @return Status of the call.
     */
    Status Rearm() const;

    /**
     * @brief Get the underlying JFC handle.
     * @return Raw JFC handle.
     */
    urma_jfc_t *Raw() const
    {
        return raw_;
    }

private:
    urma_jfc_t *raw_ = nullptr;
};

class UrmaJfr {
public:
    explicit UrmaJfr(urma_jfr_t *raw) : raw_(raw)
    {
        counter_.fetch_add(1);
    }
    ~UrmaJfr();

    URMA_DISABLE_COPY_AND_MOVE(UrmaJfr);

    /**
     * @brief Create a jetty for receive.
     * @param[in] resource Owning UrmaResource that provides context, jfc, and token.
     * @param[out] jfr Created JFR wrapper.
     * @return Status of the call.
     */
    static Status Create(const UrmaResource &resource, uint32_t depth, std::shared_ptr<UrmaJfr> &jfr);

    /**
     * @brief Get the underlying JFR handle.
     * @return Raw JFR handle.
     */
    urma_jfr_t *Raw() const
    {
        return raw_;
    }

private:
    static std::atomic<uint32_t> counter_;
    urma_jfr_t *raw_ = nullptr;
};

class UrmaJetty {
public:
    UrmaJetty(urma_jetty_t *raw, std::shared_ptr<UrmaJfr> sharedJfr, UrmaResource *resource)
        : raw_(raw), sharedJfr_(std::move(sharedJfr)), resource_(resource), valid_(true)
    {
        counter_.fetch_add(1);
    }
    ~UrmaJetty();

    URMA_DISABLE_COPY_AND_MOVE(UrmaJetty);

    /**
     * @brief Create a local Jetty. The implementation uses a shared JFR because UB providers require
     *        jetty_cfg.shared.jfr to be populated.
     * @param[in] resource Owning UrmaResource that provides context, JFC, priority, and token.
     * @param[out] jetty Created Jetty wrapper.
     * @return Status of the call.
     */
    static Status Create(UrmaResource &resource, JettyType jettyType, std::shared_ptr<UrmaJetty> &jetty);

    urma_jetty_t *Raw() const
    {
        return raw_;
    }

    urma_jfr_t *SharedJfrRaw() const
    {
        return sharedJfr_ == nullptr ? nullptr : sharedJfr_->Raw();
    }

    bool IsValid() const
    {
        return valid_;
    }

    bool MarkInvalid()
    {
        bool expected = true;
        return valid_.compare_exchange_strong(expected, false);
    }

    Status ModifyToError();

    uint32_t GetJettyId() const
    {
        return raw_->jetty_id.id;
    }

    void BindConnection(const std::shared_ptr<UrmaConnection> &connection);

    std::weak_ptr<UrmaConnection> GetConnection() const;

private:
    static std::atomic<uint32_t> counter_;
    urma_jetty_t *raw_ = nullptr;
    std::shared_ptr<UrmaJfr> sharedJfr_;
    UrmaResource *resource_ = nullptr;
    mutable std::mutex connectionMutex_;
    std::weak_ptr<UrmaConnection> connection_;
    std::atomic<bool> valid_ = false;
};

class UrmaTargetJetty {
public:
    explicit UrmaTargetJetty(urma_target_jetty_t *raw) : raw_(raw)
    {
    }
    ~UrmaTargetJetty();

    URMA_DISABLE_COPY_AND_MOVE(UrmaTargetJetty);

    static Status Import(urma_context_t *context, urma_rjetty_t *remoteJetty, urma_token_t urmaToken,
                         std::unique_ptr<UrmaTargetJetty> &tjetty);

    urma_target_jetty_t *Raw() const
    {
        return raw_;
    }

private:
    urma_target_jetty_t *raw_ = nullptr;
};

class UrmaLocalSegment {
public:
    explicit UrmaLocalSegment(urma_target_seg_t *raw) : raw_(raw)
    {
    }
    ~UrmaLocalSegment();

    URMA_DISABLE_COPY_AND_MOVE(UrmaLocalSegment);

    /**
     * @brief Register a local memory segment for Urma access.
     * @param[in] context Local Urma context.
     * @param[in] segAddress Starting virtual address of the segment.
     * @param[in] segSize Segment size in bytes.
     * @param[in] urmaToken Token used to protect the segment.
     * @param[in] registerSegmentFlag Registration flags.
     * @param[out] segment Registered segment wrapper.
     * @return Status of the call.
     */
    static Status Register(urma_context_t *context, uint64_t segAddress, uint64_t segSize, urma_token_t urmaToken,
                           urma_reg_seg_flag_t registerSegmentFlag, std::unique_ptr<UrmaLocalSegment> &segment);

    /**
     * @brief Get the underlying local segment handle.
     * @return Raw local segment handle.
     */
    urma_target_seg_t *Raw() const
    {
        return raw_;
    }

private:
    urma_target_seg_t *raw_ = nullptr;
};

class UrmaRemoteSegment {
public:
    explicit UrmaRemoteSegment(urma_target_seg_t *raw) : raw_(raw)
    {
    }
    ~UrmaRemoteSegment();

    URMA_DISABLE_COPY_AND_MOVE(UrmaRemoteSegment);

    /**
     * @brief Import a remote memory segment for local access.
     * @param[in] context Local Urma context.
     * @param[in] urmaToken Token used to import the segment.
     * @param[in] importSegmentFlag Import flags.
     * @param[in] remoteSegment Remote segment descriptor.
     * @param[out] segment Imported remote segment wrapper.
     * @return Status of the call.
     */
    static Status Import(urma_context_t *context, urma_token_t urmaToken, urma_import_seg_flag_t importSegmentFlag,
                         urma_seg_t &remoteSegment, std::unique_ptr<UrmaRemoteSegment> &segment);

    /**
     * @brief Get the underlying imported segment handle.
     * @return Raw imported segment handle.
     */
    urma_target_seg_t *Raw() const
    {
        return raw_;
    }

private:
    urma_target_seg_t *raw_ = nullptr;
};

using UrmaLocalSegmentMap = tbb::concurrent_hash_map<uint64_t, std::unique_ptr<UrmaLocalSegment>>;
using UrmaRemoteSegmentMap = tbb::concurrent_hash_map<uint64_t, std::unique_ptr<UrmaRemoteSegment>>;

class UrmaResource;

class UrmaConnection : public std::enable_shared_from_this<UrmaConnection> {
public:
    /**
     * @brief Construct a connection with a local Jetty and an imported remote target Jetty.
     * @param[in] jetty Local Jetty used by this connection.
     * @param[in] tjetty Imported remote target Jetty.
     * @param[in] urmaJfrInfo Remote Jetty metadata stored in the legacy JFR-shaped handshake structure.
     */
    UrmaConnection(std::shared_ptr<UrmaJetty> jetty, std::unique_ptr<UrmaTargetJetty> tjetty,
                   const UrmaJfrInfo &urmaJfrInfo)
        : jetty_(std::move(jetty)), tjetty_(std::move(tjetty)), urmaJfrInfo_(urmaJfrInfo)
    {
        LOG(INFO) << "Created connection with Jetty " << jetty_->GetJettyId() << " and remote Jetty "
                  << urmaJfrInfo_.jfrId;
    }

    ~UrmaConnection();

    URMA_DISABLE_COPY_AND_MOVE(UrmaConnection);

    /**
     * @brief Get remote JFR metadata associated with this connection.
     * @return Reference to remote JFR info.
     */
    const UrmaJfrInfo &GetUrmaJfrInfo() const;

    /**
     * @brief Get the current local Jetty owned by this connection.
     * @return Shared pointer to the bound Jetty.
     */
    std::shared_ptr<UrmaJetty> GetJetty() const;

    /**
     * @brief Recreate the connection Jetty for a failed request Jetty.
     *        The failure marker and replacement are both performed while holding the
     *        connection lock so only one thread handles a given failed Jetty.
     * @param[in] resource Urma resource used to create and retire JFS handles.
     * @param[in] failedJetty The Jetty that observed the CQE failure.
     * @return Status of the call.
     */
    Status ReCreateJetty(UrmaResource &resource, const std::shared_ptr<UrmaJetty> &failedJetty);

    /**
     * @brief Get the imported remote target Jetty handle.
     * @return Raw target Jetty handle.
     */
    urma_target_jetty_t *GetTargetJetty() const;

    /**
     * @brief Look up an imported remote segment by its base address.
     * @param[in] segVa Remote segment base address.
     * @param[out] accessor Accessor bound to the segment entry on success.
     * @return Status of the call.
     */
    Status GetRemoteSeg(uint64_t segVa, UrmaRemoteSegmentMap::const_accessor &accessor) const;

    /**
     * @brief Import and cache a remote segment for this connection.
     * @param[in] importSegmentInfo Remote segment metadata.
     * @param[in] context Local Urma context.
     * @param[in] urmaToken Token used to import the segment.
     * @param[in] importSegmentFlag Import flags.
     * @return Status of the call.
     */
    Status ImportRemoteSeg(const UrmaImportSegmentPb &importSegmentInfo, urma_context_t *context,
                           urma_token_t urmaToken, urma_import_seg_flag_t importSegmentFlag);

    /**
     * @brief Remove an imported remote segment from the cache.
     * @param[in] segmentAddress Base address of the remote segment.
     * @return Status of the call.
     */
    Status UnimportRemoteSeg(uint64_t segmentAddress);

    /**
     * @brief Release all resources owned by this connection.
     */
    void Clear();

private:
    mutable std::mutex jettyMutex_;
    std::shared_ptr<UrmaJetty> jetty_;
    std::unique_ptr<UrmaTargetJetty> tjetty_;
    UrmaJfrInfo urmaJfrInfo_;
    UrmaRemoteSegmentMap tsegs_;
};

class UrmaResource {
public:
    UrmaResource() = default;
    ~UrmaResource() = default;

    /**
     * @brief Initialize core Urma resources for the local device.
     * @param[in] device Local Urma device.
     * @param[in] eidIndex EID index used to create the context.
     * @param[in] isBondingDevice Whether local device name indicates bonding mode.
     * @return Status of the call.
     */
    Status Init(urma_device_t *device, uint32_t eidIndex, bool isBondingDevice = false);

    /**
     * @brief Release all owned Urma resources.
     */
    void Clear();

    /**
     * @brief Get the local Urma context handle.
     * @return Raw Urma context handle.
     */
    urma_context_t *GetContext() const;

    /**
     * @brief Get the JFC event channel handle.
     * @return Raw JFCE handle.
     */
    urma_jfce_t *GetJfce() const;

    /**
     * @brief Get the shared JFC handle.
     * @return Raw JFC handle.
     */
    urma_jfc_t *GetJfc() const;

    /**
     * @brief Get the resource token used for segment and JFR protection.
     * @return Reference to the Urma token.
     */
    const urma_token_t &GetUrmaToken() const
    {
        return urmaToken_;
    }

    /**
     * @brief Get the JFS priority used when creating new JFS instances.
     * @return JFS priority value.
     */
    uint8_t GetJfsPriority() const
    {
        return jfsPriority_;
    }

    /**
     * @brief Gets the priority and sl for CTP.
     * @param[out] priority The priority index for current tp_type
     * @param[out] sl The sl for current tp_type
     * @return Whether the priority/sl is fetched from device capability.
     */
    bool GetJfsPriorityInfoForCTP(uint8_t &priority, uint32_t &sl) const;

    /**
     * @brief Get the Jetty priority used when creating new Jetty instances.
     * @return Jetty priority value.
     */
    uint8_t GetJettyPriority() const
    {
        return jettyPriority_;
    }

    /**
     * @brief Gets the priority and sl for CTP (for Jetty).
     * @param[out] priority The priority index for current tp_type
     * @param[out] sl The sl for current tp_type
     * @return Whether the priority/sl is fetched from device capability.
     */
    bool GetJettyPriorityInfoForCTP(uint8_t &priority, uint32_t &sl) const;

    /**
     * @brief Get the maximum supported Urma write size.
     * @return Maximum write size in bytes.
     */
    uint64_t GetMaxWriteSize() const
    {
        return urmaDeviceAttribute_.dev_cap.max_write_size;
    }

    /**
     * @brief Get the maximum supported Urma read size.
     * @return Maximum read size in bytes.
     */
    uint64_t GetMaxReadSize() const
    {
        return urmaDeviceAttribute_.dev_cap.max_read_size;
    }

    /**
     * @brief Create a new Jetty for a connection.
     * @param[out] jetty Created Jetty wrapper.
     * @return Status of the call.
     */
    Status CreateJetty(std::shared_ptr<UrmaJetty> &jetty, JettyType jettyType = JettyType::SEND);

    /**
     * @brief Asynchronously move a Jetty to error state for later cleanup.
     * @param[in] jetty Jetty to retire.
     * @return Status of the call.
     */
    Status AsyncModifyJettyToError(std::shared_ptr<UrmaJetty> jetty);

    /**
     * @brief Asynchronously delete a Jetty that has been detached from service.
     * @param[in] jettyId Urma-assigned Jetty id.
     */
    void AsyncDeleteJetty(uint32_t jettyId);

    /**
     * @brief Register a Jetty in the resource-level registry for AE lookup.
     * @param[in] jetty Jetty to register.
     * @return Status of the call.
     */
    Status RegisterJetty(const std::shared_ptr<UrmaJetty> &jetty);

    /**
     * @brief Remove a Jetty from the registry.
     * @param[in] jettyId Urma-assigned Jetty id.
     * @param[in] expected Optional pointer; only unregister if the registered Jetty matches.
     */
    void UnregisterJetty(uint32_t jettyId, const UrmaJetty *expected = nullptr);

    /**
     * @brief Look up a Jetty by its Urma-assigned id.
     * @param[in] jettyId Jetty id to look up.
     * @param[out] jetty Locked shared pointer to the Jetty.
     * @return Status of the call.
     */
    Status GetJettyById(uint32_t jettyId, std::shared_ptr<UrmaJetty> &jetty);

    /**
     * @brief Get any valid registered Jetty, used by async-event injection tests.
     * @param[out] jetty Locked shared pointer to a valid Jetty.
     * @return Status of the call.
     */
    Status GetAnyValidJetty(std::shared_ptr<UrmaJetty> &jetty);

    /**
     * @brief Get or lazily create the context-level shared JFR for send-only Jetty.
     * @param[out] jfr Shared JFR used by all Jetty under one urma context.
     * @return Status of the call.
     */
    Status GetOrCreateSharedJettyJfr(std::shared_ptr<UrmaJfr> &jfr);

private:
    struct PendingDeleteJetty {
        std::shared_ptr<UrmaJetty> jetty;
        std::string traceId;
    };

    /**
     * @brief Retire a Jetty to error state and store it for later deletion.
     * @param[in] jetty Failed Jetty instance.
     * @return Status of the call.
     */
    Status RetireJettyToError(const std::shared_ptr<UrmaJetty> &jetty);

    const urma_token_t urmaToken_ = { 0xACFE };  // default token
    uint8_t jfsPriority_ = 0;
    uint8_t jettyPriority_ = 0;
    urma_device_attr_t urmaDeviceAttribute_ = {};
    std::unique_ptr<UrmaContext> context_;
    std::unique_ptr<UrmaJfce> jfce_;
    std::unique_ptr<UrmaJfc> jfc_;
    std::unique_ptr<ThreadPool> deleteJettyThread_;
    std::mutex pendingDeleteMutex_;
    // jetty id to pending delete jetty object with trace context
    std::unordered_map<uint32_t, PendingDeleteJetty> pendingDeleteJettys_;
    std::mutex jettyRegistryMutex_;
    std::unordered_map<uint32_t, std::weak_ptr<UrmaJetty>> jettyRegistry_;
    std::mutex sharedJettyJfrMutex_;
    std::shared_ptr<UrmaJfr> sharedJettyJfr_;
};

}  // namespace datasystem

#endif
