#ifndef DATASYSTEM_COMMON_DEVICE_ACL_POINTER_WRAPPER_H
#define DATASYSTEM_COMMON_DEVICE_ACL_POINTER_WRAPPER_H

#include <memory>

#include "datasystem/common/device/device_manager_factory.h"
#include "datasystem/common/device/ascend/cann_types.h"

namespace datasystem {
/**
 * @brief RAII wrapper for HCCL communicator
 */
class AclPointerWrapper {
public:
    explicit AclPointerWrapper(void *pointer) : pointer_(pointer)
    {
    }

    AclPointerWrapper() : AclPointerWrapper(nullptr)
    {
    }

    // Must not be copyable
    AclPointerWrapper(const AclPointerWrapper &) = delete;

    AclPointerWrapper &operator=(const AclPointerWrapper &) = delete;

    // Move constructable
    AclPointerWrapper(AclPointerWrapper &&other) noexcept
    {
        std::swap(pointer_, other.pointer_);
    }

    // Move assignable
    AclPointerWrapper &operator=(AclPointerWrapper &&other) noexcept
    {
        std::swap(pointer_, other.pointer_);
        return *this;
    }

    virtual void ShutDown()
    {
    }

    virtual ~AclPointerWrapper() = default;

    /**
     * @brief Get the pointer
     * @return The pointer
     */
    [[nodiscard]] void *Get() const
    {
        return pointer_;
    }

    /**
     * @brief Get the pointer reference
     * @return The pointer reference
     */
    [[nodiscard]] void *&GetRef()
    {
        return pointer_;
    }

protected:
    void *pointer_;
};

class AclRtEventWrapper : public AclPointerWrapper {
public:
    /**
     * @brief Create the aclRtEvent wrapper.
     * @param[out] event The aclRtEvent wrapper.
     * @return Status of the call.
     */
    static Status Create(std::shared_ptr<AclRtEventWrapper> &event)
    {
        if (event == nullptr) {
            event = std::make_shared<AclRtEventWrapper>();
        }
        auto deviceMgr = DeviceManagerFactory::GetDeviceManager();
        return deviceMgr->CreateEvent(&(event->GetRef()));
    }

    ~AclRtEventWrapper()
    {
        auto event = GetRef();
        if (event != nullptr) {
            auto deviceMgr = DeviceManagerFactory::GetDeviceManager();
            deviceMgr->DestroyEvent(event);
            event = nullptr;
        }
    }

    /**
     * @brief Synchronize the event
     * @param[in] timeoutMs The timeout of the sync.
     * @return Status of the call.
     */
    Status SynchronizeEvent(int32_t timeoutMs = 0)
    {
        auto event = GetRef();
        CHECK_FAIL_RETURN_STATUS(event != nullptr, K_RUNTIME_ERROR, "Event is nullptr");
        auto deviceMgr = DeviceManagerFactory::GetDeviceManager();
        if (timeoutMs != 0) {
            return deviceMgr->SynchronizeEventWithTimeout(event, timeoutMs);
        }
        return deviceMgr->SynchronizeEvent(event);
    }

    /**
     * @brief Record the event in this stream.
     * @param[in] stream The acl runtime stream.
     * @return Status of the call.
     */
    Status RecordEvent(aclrtStream stream)
    {
        auto event = Get();
        CHECK_FAIL_RETURN_STATUS(event != nullptr, K_RUNTIME_ERROR, "Event is nullptr");
        auto deviceMgr = DeviceManagerFactory::GetDeviceManager();
        return deviceMgr->RecordEvent(event, stream);
    }

    /**
     * @brief Queries whether the events recorded by aclrtRecordEvent
     * @return Status of the call.
     */
    Status QueryEventStatus()
    {
        CHECK_FAIL_RETURN_STATUS(Get() != nullptr, K_RUNTIME_ERROR, "Event is nullptr");
        auto deviceMgr = DeviceManagerFactory::GetDeviceManager();
        return deviceMgr->QueryEventStatus(Get());
    }
};
}  // namespace datasystem
#endif