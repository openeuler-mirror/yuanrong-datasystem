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

#include <cstdint>
#include <memory>
#include <unordered_map>
#include <tbb/concurrent_hash_map.h>

#include <ub/umdk/urma/urma_api.h>

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
class UrmaJfs;
class UrmaEvent : public Event {
public:
    enum class OperationType : uint8_t {
        UNKNOWN = 0,
        READ = 1,
        WRITE = 2
    };

    /**
     * @brief Construct an Urma event for an in-flight request.
     * @param[in] requestId Unique request id.
     * @param[in] connection Connection associated with the request.
     * @param[in] waiter Optional waiter used for notification.
     */
    UrmaEvent(uint64_t requestId, std::weak_ptr<UrmaConnection> connection, std::weak_ptr<UrmaJfs> jfs,
              std::string remoteAddress, std::string remoteInstanceId, OperationType operationType,
              std::shared_ptr<EventWaiter> waiter = nullptr)
        : Event(requestId, std::move(waiter)),
          connection_(std::move(connection)),
          jfs_(std::move(jfs)),
          remoteAddress_(std::move(remoteAddress)),
          remoteInstanceId_(std::move(remoteInstanceId)),
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
     * @brief Get the connection associated with this request.
     * @return Weak pointer to the associated connection.
     */
    std::weak_ptr<UrmaConnection> GetConnection() const
    {
        return connection_;
    }

    /**
     * @brief Get the JFS associated with this request when it was submitted.
     * @return Weak pointer to the request JFS.
     */
    std::weak_ptr<UrmaJfs> GetJfs() const
    {
        return jfs_;
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
    std::weak_ptr<UrmaConnection> connection_;
    std::weak_ptr<UrmaJfs> jfs_;
    std::string remoteAddress_;
    std::string remoteInstanceId_;
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

class UrmaJfs {
public:
    explicit UrmaJfs(urma_jfs_t *raw) : raw_(raw), valid_(true)
    {
        counter_.fetch_add(1);
    }
    ~UrmaJfs();

    URMA_DISABLE_COPY_AND_MOVE(UrmaJfs);

    /**
     * @brief Create a jetty for send.
     * @param[in] context Local Urma context.
     * @param[in] jfc Completion queue bound to this JFS.
     * @param[in] priority Service priority used for this JFS.
     * @param[out] jfs Created JFS wrapper.
     * @return Status of the call.
     */
    static Status Create(urma_context_t *context, urma_jfc_t *jfc, uint8_t priority, std::shared_ptr<UrmaJfs> &jfs);

    /**
     * @brief Get the underlying JFS handle.
     * @return Raw JFS handle.
     */
    urma_jfs_t *Raw() const
    {
        return raw_;
    }

    /**
     * @brief Check whether the JFS is still valid for issuing work requests.
     * @return True if the JFS is valid, else false.
     */
    bool IsValid() const
    {
        return valid_;
    }

    /**
     * @brief Atomically mark the JFS as invalid.
     * @return True if the state changes from valid to invalid.
     */
    bool MarkInvalid()
    {
        bool expected = true;
        return valid_.compare_exchange_strong(expected, false);
    }

    /**
     * @brief Move the JFS to error state before cleanup or recreation.
     * @return Status of the call.
     */
    Status ModifyToError();

    /**
     * @brief Get the Urma-assigned JFS id.
     * @return JFS id.
     */
    uint32_t GetJfsId() const
    {
        return raw_->jfs_id.id;
    }

private:
    static std::atomic<uint32_t> counter_;
    urma_jfs_t *raw_ = nullptr;
    std::atomic<bool> valid_ = false;
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
     * @param[in] context Local Urma context.
     * @param[in] jfc Completion queue bound to this JFR.
     * @param[in] urmaToken Token used to protect resource access.
     * @param[out] jfr Created JFR wrapper.
     * @return Status of the call.
     */
    static Status Create(urma_context_t *context, urma_jfc_t *jfc, urma_token_t urmaToken,
                         std::unique_ptr<UrmaJfr> &jfr);

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

class UrmaTargetJfr {
public:
    explicit UrmaTargetJfr(urma_target_jetty_t *raw) : raw_(raw)
    {
    }
    ~UrmaTargetJfr();

    URMA_DISABLE_COPY_AND_MOVE(UrmaTargetJfr);

    /**
     * @brief Import a remote JFR as a target jetty.
     * @param[in] context Local Urma context.
     * @param[in] remoteJfr Remote JFR descriptor.
     * @param[in] urmaToken Token used to import the remote JFR.
     * @param[out] tjfr Imported target JFR wrapper.
     * @return Status of the call.
     */
    static Status Import(urma_context_t *context, urma_rjfr_t *remoteJfr, urma_token_t urmaToken,
                         std::unique_ptr<UrmaTargetJfr> &tjfr);

    /**
     * @brief Get the underlying target jetty handle.
     * @return Raw target jetty handle.
     */
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

class UrmaConnection {
public:
    /**
     * @brief Construct a connection with a local JFS and an imported remote target JFR.
     *        Local JFR is managed separately in UrmaManager::localJfrMap_.
     * @param[in] jfs Local JFS exclusively owned by this connection.
     * @param[in] tjfr Imported remote target JFR for writing to the remote side's JFR.
     * @param[in] urmaJfrInfo Remote JFR metadata.
     */
    UrmaConnection(std::shared_ptr<UrmaJfs> jfs, std::unique_ptr<UrmaTargetJfr> tjfr, const UrmaJfrInfo &urmaJfrInfo)
        : jfs_(std::move(jfs)), tjfr_(std::move(tjfr)), urmaJfrInfo_(urmaJfrInfo)
    {
        LOG(INFO) << "Created connection with JFS " << jfs_->GetJfsId() << " and remote JFR " << urmaJfrInfo_.jfrId;
    }

    ~UrmaConnection() = default;

    URMA_DISABLE_COPY_AND_MOVE(UrmaConnection);

    /**
     * @brief Get remote JFR metadata associated with this connection.
     * @return Reference to remote JFR info.
     */
    const UrmaJfrInfo &GetUrmaJfrInfo() const;

    /**
     * @brief Get the current local JFS exclusively owned by this connection.
     * @return Shared pointer to the bound JFS.
     */
    std::shared_ptr<UrmaJfs> GetJfs() const;

    /**
     * @brief Recreate the connection JFS for a failed request JFS.
     *        The failure marker and replacement are both performed while holding the
     *        connection lock so only one thread handles a given failed JFS.
     * @param[in] resource Urma resource used to create and retire JFS handles.
     * @param[in] failedJfs The JFS that observed the CQE failure.
     * @return Status of the call.
     */
    Status ReCreateJfs(UrmaResource &resource, const std::shared_ptr<UrmaJfs> &failedJfs);

    /**
     * @brief Get the imported remote target JFR handle.
     * @return Raw target JFR handle.
     */
    urma_target_jetty_t *GetTargetJfr() const;

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
    mutable std::mutex jfsMutex_;
    std::shared_ptr<UrmaJfs> jfs_;
    std::unique_ptr<UrmaTargetJfr> tjfr_;
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
     * @return Status of the call.
     */
    Status Init(urma_device_t *device, uint32_t eidIndex);

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
     * @brief Gets the priority and sl for CTP.
     * @param[out] priority The priority index for current tp_type
     * @param[out] sl The sl for current tp_type
     * @return Whether the priority/sl is fetched from device capability.
     */
    bool GetJfsPriorityInfoForCTP(uint8_t &priority, uint32_t &sl) const;

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
     * @brief Create a new JFS for exclusive use by a single connection.
     * @param[out] jfs Created JFS wrapper.
     * @return Status of the call.
     */
    Status CreateJfs(std::shared_ptr<UrmaJfs> &jfs);

    /**
     * @brief Create a new local JFR managed outside UrmaConnection.
     * @param[out] jfr Created JFR wrapper.
     * @return Status of the call.
     */
    Status CreateJfr(std::unique_ptr<UrmaJfr> &jfr);

    /**
     * @brief Asynchronously move a JFS to error state for later cleanup.
     * @param[in] jfs JFS to retire.
     * @return Status of the call.
     */
    Status AsyncModifyJfsToError(std::shared_ptr<UrmaJfs> jfs);

    /**
     * @brief Asynchronously delete a JFS that has been detached from service.
     * @param[in] jfsId Urma-assigned JFS id.
     */
    void AsyncDeleteJfs(uint32_t jfsId);

private:
    struct PendingDeleteJfs {
        std::shared_ptr<UrmaJfs> jfs;
        std::string traceId;
    };

    /**
     * @brief Retire a JFS to error state and store it for later deletion.
     * @param[in] jfs Failed JFS instance.
     * @return Status of the call.
     */
    Status RetireJfsToError(const std::shared_ptr<UrmaJfs> &jfs);

    const urma_token_t urmaToken_ = { 0xACFE };  // default token
    uint8_t jfsPriority_ = 0;
    urma_device_attr_t urmaDeviceAttribute_ = {};
    std::unique_ptr<UrmaContext> context_;
    std::unique_ptr<UrmaJfce> jfce_;
    std::unique_ptr<UrmaJfc> jfc_;
    std::unique_ptr<ThreadPool> deleteJfsThread_;
    std::mutex pendingDeleteMutex_;
    // jfs id to pending delete jfs object with trace context
    std::unordered_map<uint32_t, PendingDeleteJfs> pendingDeleteJfs_;
};

}  // namespace datasystem

#endif
