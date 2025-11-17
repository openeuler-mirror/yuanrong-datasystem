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

#ifndef DATASYSTEM_SAFE_OBJECT_H
#define DATASYSTEM_SAFE_OBJECT_H

#include <atomic>
#include <memory>
#include <mutex>
#include <thread>
#include <unistd.h>

#include <sys/syscall.h>

#include "datasystem/common/util/locks.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/template_util.h"
#include "datasystem/common/util/thread_local.h"
#include "datasystem/common/util/timer.h"

namespace datasystem {

/**
 * @brief A SafeObject provides an infrastructure to facilitate a thread-safe object.
 *
 * It wraps a custom (template arg) type of object along with a locking implementation to provide a caller with the
 * controls needed to protect the object during critical sections of code that require thread-safe access.
 *
 * @tparam ObjType The object type. This type cannot be given as a pointer type. For example, the following are invalid:
 * SafeObject<int *>
 * SafeObject<std::shared_ptr<int>>
 * SafeObject<std::unique_ptr<int>>
 * A compile time check will generate a compiler error if such a type is attempted.
 * The reason for this is because this class needs to protect concurrent access to the object. If the type of the real
 * data is a pointer, then the caller might have its own pointer to the same data, bypassing the protection (locking)
 * that is provided by the SafeObject.
 * Enforcing that you can only construct the real data via unique_ptr move or a deep copy ensures that no other code
 * can access the real object outside of this SafeObject (see constructors)
 */
template <typename ObjType>
class SafeObject final {
public:
    // Compile time assert that the type cannot be a pointer. See class description for details.
    static_assert(!is_shared_ptr<ObjType>::value && !std::is_pointer<ObjType>::value,
                  "The SafeObject template parameter cannot be a pointer type");

    /**
     * @brief Constructor 1 creates the safe object but real object not populated yet and remains empty.
     */
    SafeObject() : realObject_(nullptr), deleted_(false), wLocked_(false)
    {
    }

    /**
     * @brief Constructor 2 takes a unique ptr to the object type and moves it into the safe object to take ownership of
     * the real object.
     * @param[in] objPtr A unique pointer of the data that this SafeObject will take control of.
     */
    explicit SafeObject(std::unique_ptr<ObjType> objPtr)
        : realObject_(std::move(objPtr)), deleted_(false), wLocked_(false)
    {
    }

    /**
     * @brief Constructor 3 takes the real object and makes a copy of it through its copy constructor (deep copy) and
     * then will take ownership of the copied data. The caller passed-in data is not controlled or modified by this
     * SafeObject.
     * @param[in] obj The object reference of the data that will be copied into this SafeObject.
     */
    explicit SafeObject(const ObjType &obj)
        : realObject_(std::make_unique<ObjType>(obj)), deleted_(false), wLocked_(false)
    {
    }

    /**
     * @brief Default destructor.
     */
    ~SafeObject() = default;

    /**
     * @brief Assigns the real object into the SafeObject via copy constructor of the passed-in object.
     * @param[in] objPtr A unique pointer of the data that this SafeObject will take control of.
     * @note This does not automatically lock the object first, and it may overwrite previous data if
     * it exists.
     */
    void SetRealObject(std::unique_ptr<ObjType> objPtr);

    /**
     * @brief Assigns the real object into the SafeObject via copy constructor of the passed-in object.
     * @param[in] obj The object reference of the data that will be copied into this SafeObject.
     * @note This does not automatically lock the object first, and it may overwrite previous data if
     * it exists.
     */
    void SetRealObject(const ObjType &obj);

    /**
     * @brief Acquires a write lock on the SafeObject. If the lock is already held, it waits until the lock is released
     * and then gets the lock.
     * @param[in] nullable Whether allow null after get write lock. If this parameter passes true, before using the
     * realObject_'s properties, must check whether is a null pointer.
     * @return Status of the call.
     */
    Status WLock(bool nullable = false);

    /**
     * @brief Attempts Acquires a write lock on the SafeObject. If the lock is already held, it returns K_TRY_AGAIN
     * right away and does not wait.
     * @param[in] nullable Whether allow null after get write lock. If this parameter passes true, before using the
     * realObject_'s properties, must check whether is a null pointer.
     * @return Status of the call.
     */
    Status TryWLock(bool nullable = false);

    /**
     * @brief Releases a write lock on the SafeObject.
     */
    void WUnlock();

    /**
     * @brief Acquires a read lock on the SafeObject. If the lock is already held, it waits until the lock is released
     * and then gets the lock.
     * @param[in] nullable Whether allow null after get read lock. If this parameter passes true, before using the
     * realObject_'s properties, must check whether is a null pointer.
     * @return Status of the call.
     */
    Status RLock(bool nullable = false);

    /**
     * @brief Attempt to acquire a read lock on the SafeObject. If the lock is already held, it returns K_TRY_AGAIN
     * right away and does not wait.
     * @param[in] nullable Whether allow null after get read lock. If this parameter passes true, before using the
     * realObject_'s properties, must check whether is a null pointer.
     * @return Status of the call.
     */
    Status TryRLock(bool nullable = false);

    /**
     * @brief Transfers the write lock from the current thread to the calling thread.
     *
     * This function is used to transfer the write lock ownership from the current thread to the thread that calls this
     * function. It ensures that the write lock is held by the calling thread after the transfer.
     *
     * @return Status of the call.
     */
    Status TransferWLockToCurrentThread();

    /**
     * @brief Releases a read lock on the SafeObject.
     */
    void RUnlock();

    /**
     * @brief Acquires a global reference lock on the SafeObject. The lock can prevent the concurrency of increasing or
     * decreasing global reference.
     */
    void GRefLock();

    /**
     * @brief Releases a global reference lock on the SafeObject.
     */
    void GRefUnlock();

    /**
     * @brief Removes and deletes the object that the SafeObject is managing. Future attempts to lock this object will
     * return error because the object is deleted. This is used when multiple callers may have pointer to the same
     * SafeObject, and one of them deletes it. It provides a way that other threads stop using this SafeObject.
     */
    void DeleteObject();

    /**
     * @brief This call will remove (and return as unique_ptr) the object that the SafeObject was managing.
     * Unlike DeleteObject, this SafeObject retains its locking function (and lock state) and is essentially a clean
     * SafeObject that can be assigned new data (via SetRealObject())
     * @return The unique pointer that the SafeObject previously was managing.  This object is no longer protected
     * by the safe object
     */
    std::unique_ptr<ObjType> Detach();
    /**
     * @brief Similar to a smart pointer, this -> dereference provides direct access to the internal object managed
     * by this SafeObject. To provide thread-safety, callers should first acquire a lock before dereferencing.
     * @return Pointer to the object.
     */
    ObjType *operator->();

    /**
     * @brief Similar to a smart pointer, this -> dereference provides direct access to the internal object managed
     * by this SafeObject. To provide thread-safety, callers should first acquire a lock before dereferencing.
     * @return Pointer to the object.
     */
    const ObjType *operator->() const;

    /**
     * @brief Similar to the -> operator, but a different syntax allowing the user to save the results rather than
     * go through ->. Similar to the shared_ptr or unique_ptr syntax.
     * @return Pointer to the object.
     */
    ObjType *Get();

    /**
     * @brief Check if the thread is holding the WLock.
     * Currently, this call only supports the main lock, not the secondary lock.
     * @return True if thread is holding the WLock, false otherwise.
     */
    bool IsWLockedByCurrentThread() const;

    /**
     * @brief For cases when the SafeObject is protecting a base class, this version of Get() returns a pointer to the
     * data down casted to its derived type.
     * @tparam Derived The type of the pointer to dynamically cast to.
     * @return Pointer to the object. Returns nullptr if the dynamic cast was invalid.
     */
    template <typename Derived>
    static Derived *GetDerived(SafeObject<ObjType> &baseObj)
    {
        return dynamic_cast<Derived *>(baseObj.Get());
    }

    /**
     * @brief Similar to a smart pointer, this * dereference provides direct access to the internal object managed
     * by this SafeObject. To provide thread-safety, callers should first acquire a lock before dereferencing.
     * @return Reference to the object.
     */
    ObjType &operator*();

    // Disable all copy and move constructors.
    SafeObject(const SafeObject &) = delete;
    SafeObject(SafeObject &&other) noexcept = delete;
    SafeObject &operator=(const SafeObject &) = delete;
    SafeObject &operator=(SafeObject &&other) noexcept = delete;

private:
    WriterPrefRWLock objLock_;             // The lock for the object metadata and data.
    std::mutex gRefLock_;                  // The lock for the object global reference.
    std::unique_ptr<ObjType> realObject_;  // The actual object stored in a unique_ptr.
    std::atomic<bool> deleted_;            // Flag for checking the deleted state.
    std::atomic<pid_t> lastWriteThread_;   // Last thread that has the WLock on objLock_, valid when wLocked_ is true.
    std::atomic<bool> wLocked_;            // Is there a thread holding WLock on objLock_.
};

template <typename ObjType>
void SafeObject<ObjType>::SetRealObject(std::unique_ptr<ObjType> obj)
{
    realObject_ = std::move(obj);
}

template <typename ObjType>
void SafeObject<ObjType>::SetRealObject(const ObjType &obj)
{
    realObject_ = std::make_unique<ObjType>(obj);
}

template <typename ObjType>
Status SafeObject<ObjType>::WLock(bool nullable)
{
    Timer timer;
    objLock_.WriteLock();
    workerOperationTimeCost.Append("worker SafeObject WLock", timer.ElapsedMilliSecond());
    masterOperationTimeCost.Append("master SafeObject WLock", timer.ElapsedMilliSecond());
    if (deleted_ || (!nullable && realObject_ == nullptr)) {
        objLock_.WriteUnlock();
        RETURN_STATUS(StatusCode::K_NOT_FOUND, deleted_ ? "Object was deleted." : "realObject is null");
    }
    lastWriteThread_ = syscall(__NR_gettid);
    wLocked_ = true;
    return Status::OK();
}

template <typename ObjType>
Status SafeObject<ObjType>::TransferWLockToCurrentThread()
{
    if (!wLocked_) {
        RETURN_STATUS(StatusCode::K_RUNTIME_ERROR, "Write lock is not held by any thread.");
    }
    pid_t currentTid = syscall(__NR_gettid);
    lastWriteThread_ = currentTid;
    return Status::OK();
}

template <typename ObjType>
Status SafeObject<ObjType>::TryWLock(bool nullable)
{
    bool locked = objLock_.TryWriteLock();
    if (!locked) {
        RETURN_STATUS(StatusCode::K_TRY_AGAIN, "Object is in use.");
    }
    if (deleted_ || (!nullable && realObject_ == nullptr)) {
        objLock_.WriteUnlock();
        RETURN_STATUS(StatusCode::K_NOT_FOUND, deleted_ ? "Object was deleted." : "realObject is null");
    }
    lastWriteThread_ = syscall(__NR_gettid);
    wLocked_ = true;
    return Status::OK();
}

template <typename ObjType>
void SafeObject<ObjType>::WUnlock()
{
    if (wLocked_.exchange(false)) {
        objLock_.WriteUnlock();
    }
}

template <typename ObjType>
Status SafeObject<ObjType>::RLock(bool nullable)
{
    objLock_.ReadLock();
    if (deleted_ || (!nullable && realObject_ == nullptr)) {
        objLock_.ReadUnlock();
        RETURN_STATUS(StatusCode::K_NOT_FOUND, deleted_ ? "Object was deleted." : "realObject is null");
    }
    return Status::OK();
}

template <typename ObjType>
Status SafeObject<ObjType>::TryRLock(bool nullable)
{
    bool locked = objLock_.TryReadLock();
    if (!locked) {
        RETURN_STATUS(StatusCode::K_TRY_AGAIN, "Object is in use.");
    }
    if (deleted_ || (!nullable && realObject_ == nullptr)) {
        objLock_.ReadUnlock();
        RETURN_STATUS(StatusCode::K_NOT_FOUND, deleted_ ? "Object was deleted." : "realObject is null");
    }
    return Status::OK();
}

template <typename ObjType>
void SafeObject<ObjType>::RUnlock()
{
    objLock_.ReadUnlock();
}

template <typename ObjType>
void SafeObject<ObjType>::GRefLock()
{
    gRefLock_.lock();
}

template <typename ObjType>
void SafeObject<ObjType>::GRefUnlock()
{
    gRefLock_.unlock();
}

template <typename ObjType>
void SafeObject<ObjType>::DeleteObject()
{
    realObject_.reset();
    deleted_ = true;
}

template <typename ObjType>
std::unique_ptr<ObjType> SafeObject<ObjType>::Detach()
{
    return std::move(realObject_);
}

template <typename ObjType>
ObjType *SafeObject<ObjType>::operator->()
{
    return realObject_.get();
}

template <typename ObjType>
const ObjType *SafeObject<ObjType>::operator->() const
{
    return realObject_.get();
}

template <typename ObjType>
ObjType *SafeObject<ObjType>::Get()
{
    return realObject_.get();
}

template <typename ObjType>
ObjType &SafeObject<ObjType>::operator*()
{
    return *realObject_;
}

template <typename ObjType>
bool SafeObject<ObjType>::IsWLockedByCurrentThread() const
{
    if (objLock_.IsWLocked() && wLocked_ && lastWriteThread_ == syscall(__NR_gettid)) {
        return true;
    }
    return false;
}

}  // namespace datasystem
#endif  // DATASYSTEM_SAFE_OBJECT_H
