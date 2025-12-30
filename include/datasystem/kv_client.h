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
 * Description: Data system state cache client management.
 */
#ifndef DATASYSTEM_KV_CLIENT_H
#define DATASYSTEM_KV_CLIENT_H

#include <memory>
#include <vector>

#include "datasystem/context/context.h"
#include "datasystem/object/buffer.h"
#include "datasystem/object/object_enum.h"
#include "datasystem/kv/read_only_buffer.h"
#include "datasystem/utils/connection.h"
#include "datasystem/utils/optional.h"
#include "datasystem/utils/status.h"
#include "datasystem/utils/string_view.h"

namespace datasystem {
namespace object_cache {
class ObjectClientImpl;
}  // namespace object_cache
}  // namespace datasystem

namespace datasystem {
enum class ExistenceOpt : int {
    NONE = 0,  // Does not check for existence.
    NX = 1,    // Only set the key if it does not already exist.
};
struct SetParam {
    WriteMode writeMode = WriteMode::NONE_L2_CACHE;  // The default value of writeMode is WriteMode::NONE_L2_CACHE.
    // The default value 0 means the key will keep alive until you call Del api to delete the key explicitly.
    uint32_t ttlSecond = 0;
    ExistenceOpt existence = ExistenceOpt::NONE;
    CacheType cacheType = CacheType::MEMORY;
};

struct MSetParam {
    WriteMode writeMode = WriteMode::NONE_L2_CACHE;  // The default value of writeMode is WriteMode::NONE_L2_CACHE.
    uint32_t ttlSecond =
        0;  // The default value means the key will keep alive until you call Del api to delete the key.
    ExistenceOpt existence;  // There is not default value, and MSetNx only support NX mode.
    CacheType cacheType = CacheType::MEMORY;
};

struct ReadParam {
    std::string key;
    uint64_t offset = 0;
    uint64_t size = 0;
};

class __attribute((visibility("default"))) KVClient {
public:
    /// \brief Construct KVClient.
    ///
    /// \param[in] connectOptions The connection options.
    explicit KVClient(const ConnectOptions &connectOptions = {});

    ~KVClient();

    /// \brief Shutdown the state client.
    ///
    /// \return K_OK on success; the error code otherwise.
    Status ShutDown();

    /// \brief Init KVClient object.
    ///
    /// \return Status of the call.
    Status Init();

    /// \brief Invoke worker client to set the value of a key.
    ///
    /// \param[in] key The key.
    /// \param[in] val The value for the key.
    /// \param[in] param The get parameters.
    ///
    /// \return K_OK on success; the error code otherwise.
    ///         K_INVALID: the key or val is empty.
    Status Set(const std::string &key, const StringView &val, const SetParam &param = {});

    /// \brief Invoke worker client to set the value of a key.
    ///
    /// \param[in] val The value for the key.
    /// \param[in] param The get parameters.
    ///
    /// \return Key of object, return empty string if set error.
    std::string Set(const StringView &val, const SetParam &param = {});

    /// \brief Create the shared memory buffer of the data system.
    ///
    /// \param[in] key The ID of the object to create. ID should not be empty and should only contains english
    ///  alphabetics (a-zA-Z), numbers and ~!@#$%^&*.-_ only. ID length should less than 256.
    /// \param[in] size The size in bytes of object.
    /// \param[in] param The create parameters.
    /// \param[out] buffer The buffer for the object.
    ///
    /// \return K_OK on success; the error code otherwise.
    ///         K_INVALID: the key or val is empty.
    ///         K_RUNTIME_ERROR: client fd mmap failed
    Status Create(const std::string &key, uint64_t size, const SetParam &param, std::shared_ptr<Buffer> &buffer);

    /// \brief Store the shared memory buffer created by the Create interface to the data system.
    ///
    /// \param[in] buffer The buffer to set.
    ///
    /// \return K_OK on success; the error code otherwise.
    ///         K_RUNTIME_ERROR: client fd mmap failed
    Status Set(const std::shared_ptr<Buffer> &buffer);

    /// \brief Batch create shared-memory Buffers in datasystem.
    ///
    /// The returned Buffers can be filled directly with data; subsequently call MSet()
    /// to cache it. This interface avoids the need for temporary memory and reduces
    /// one extra memory copy.
    ///
    /// \param[in] keys The ID of the object to create. ID should not be empty and should only contains english
    ///  alphabetics (a-zA-Z), numbers and ~!@#$%^&*.-_ only. ID length should less than 256.
    /// \param[in] size The size in bytes of object.
    /// \param[in] param The create parameters.
    /// \param[out] buffer The buffer for the object.
    ///
    /// \return K_OK on success; the error code otherwise.
    ///         K_INVALID: the key or val is empty, or keys and sizes mismatch.
    ///         K_RUNTIME_ERROR: client fd mmap failed
    Status MCreate(const std::vector<std::string> &keys, const std::vector<uint64_t> &sizes,
                   const SetParam &param, std::vector<std::shared_ptr<Buffer>> &buffers);

    /// \brief Batch setter for multiple buffers.
    ///
    /// This interface is used together with MCreate to cache a batch of
    /// shared-memory Buffers into the data system.
    ///
    /// \param[in] buffers The buffers to set.
    ///
    /// \return K_OK on success; the error code otherwise.
    ///         K_INVALID: buffers is empty.
    ///         K_RUNTIME_ERROR: client fd mmap failed
    Status MSet(const std::vector<std::shared_ptr<Buffer>> &buffers);

    /// \brief Transactional multi-key set interface, it guarantees all the keys are either successfully created or
    ///  none of them is created. The number of keys should be in the range of 1 to 8.
    ///
    /// \param[in] keys The keys to be set.
    /// \param[in] vals The values for the keys.
    /// \param[in] param The set parameters.
    ///
    /// \return K_OK on success; the error code otherwise.
    Status MSetTx(const std::vector<std::string> &keys, const std::vector<StringView> &vals,
                  const MSetParam &param = {});

    /// \brief Multi-key set interface, it can batch set keys and return failed keys. The max keys size < 2000
    ///  and the max value for key to set < 500 * 1024
    ///
    /// \param[in] keys The keys of object
    /// \param[in] vals The vals to set
    /// \param[out] outFailedKeys The failed keys for set
    /// \param[in] param The set parameters.
    ///
    /// \return K_OK on any key success; the error code otherwise.
    Status MSet(const std::vector<std::string> &keys, const std::vector<StringView> &vals,
                std::vector<std::string> &outFailedKeys, const MSetParam &param = {});

    /// \brief Invoke worker client to get the value of a key.
    ///
    /// \param[in] key The key.
    /// \param[out] val The value for the key.
    /// \param[in] subTimeoutMs timeoutMs of waiting for the result return if object not ready. A positive integer
    ///  number required. 0 means no waiting time allowed.
    ///
    /// \return K_OK on success; the error code otherwise.
    ///         K_INVALID: the key is empty.
    ///         K_NOT_FOUND: the key not found.
    ///         K_RUNTIME_ERROR: Cannot get value from worker.
    Status Get(const std::string &key, std::string &val, int32_t subTimeoutMs = 0);

    /// \brief Invoke worker client to get the value of a key.
    ///
    /// \param[in] key The key.
    /// \param[in] subTimeoutMs timeoutMs of waiting for the result return if object not ready. A positive integer
    ///  number required. 0 means no waiting time allowed.
    /// \param[out] readOnlyBuffer The value for the key.
    ///
    /// \return K_OK on success; the error code otherwise.
    ///         K_INVALID: the key is empty.
    ///         K_NOT_FOUND: the key not found.
    ///         K_RUNTIME_ERROR: Cannot get value from worker.
    Status Get(const std::string &key, Optional<ReadOnlyBuffer> &readOnlyBuffer, int32_t subTimeoutMs = 0);

    /// \brief Invoke worker client to get the buffer of a key.
    ///
    /// \param[in] key The key.
    /// \param[in] subTimeoutMs timeoutMs of waiting for the result return if object not ready. A positive integer
    ///  number required. 0 means no waiting time allowed.
    /// \param[out] buffer The value for the key. nullptr if get failed.
    ///
    /// \return K_OK on success; the error code otherwise.
    ///         K_INVALID: the key is empty.
    ///         K_NOT_FOUND: the key not found.
    ///         K_RPC_UNAVAILABLE: disconnect from worker or master.
    ///         K_RUNTIME_ERROR: Cannot get value from worker.
    Status Get(const std::string &key, Optional<Buffer> &buffer, int32_t subTimeoutMs = 0);

    /// \brief Get multiple buffers
    ///
    /// \param[in] keys List of keys
    /// \param[in] subTimeoutMs timeoutMs of waiting for the result return if object not ready. A positive integer
    ///  number required. 0 means no waiting time allowed.
    /// \param[out] buffers The buffer list to get.
    ///
    /// \return K_OK on success; the error code otherwise.
    Status Get(const std::vector<std::string> &keys, std::vector<Optional<Buffer>> &buffers, int32_t subTimeoutMs = 0);

    /// \brief Invoke worker client to get the values of all the given keys.
    ///
    /// \param[in] keys The vector of the keys.
    /// \param[in] subTimeoutMs timeoutMs of waiting for the result return if object not ready. A positive integer
    ///  number required. 0 means no waiting time allowed.
    /// \param[out] vals The vector of the values.
    ///
    /// \return K_OK on any key success; the error code otherwise.
    ///         K_INVALID: the vector of keys is empty or include empty key.
    ///         K_NOT_FOUND: the key not found.
    ///         K_RUNTIME_ERROR: Cannot get values from worker.
    /// \verbatim
    ///  If some keys are not found, The Status OK will return,
    ///  and the existing keys will set the vals with the same index of keys.
    /// \endverbatim
    Status Get(const std::vector<std::string> &keys, std::vector<std::string> &vals, int32_t subTimeoutMs = 0);

    /// \brief Invoke worker client to get the values of all the given keys.
    ///
    /// \param[in] keys The vector of the keys.
    /// \param[in] subTimeoutMs timeoutMs of waiting for the result return if object not ready. A positive integer
    ///  number required. 0 means no waiting time allowed.
    /// \param[out] readOnlyBuffers The vector of the values.
    ///
    /// \return K_OK on any key success; the error code otherwise.
    ///         K_INVALID: the vector of keys is empty or include empty key.
    ///         K_NOT_FOUND: the key not found.
    ///         K_RUNTIME_ERROR: Cannot get values from worker.
    /// \verbatim
    ///  If some keys are not found, The Status OK will return,
    ///  and the existing keys will set the vals with the same index of keys.
    /// \endverbatim
    Status Get(const std::vector<std::string> &keys, std::vector<Optional<ReadOnlyBuffer>> &readOnlyBuffers,
               int32_t subTimeoutMs = 0);

    /// \brief Some data in an object can be read based on the specified key and parameters.
    ///         In some scenarios, read amplification can be avoided.
    ///
    /// \param[in] readParams The vector of the keys and offset.
    /// \param[out] readOnlyBuffers The vector of the values.
    ///
    /// \return K_OK on any key success; the error code otherwise.
    ///         K_INVALID: the vector of keys is empty or include empty key.
    ///         K_NOT_FOUND: the key not found.
    ///         K_RUNTIME_ERROR: Cannot get values from worker.
    /// \verbatim
    ///  If some keys are not found, The Status OK will return,
    ///  and the existing keys will set the vals with the same index of keys.
    /// \endverbatim
    Status Read(const std::vector<ReadParam> &readParams, std::vector<Optional<ReadOnlyBuffer>> &readOnlyBuffers);

    /// \brief Invoke worker client to delete a key.
    ///
    /// \param[in] key The key.
    ///
    /// \return K_OK on success; the error code otherwise.
    ///         K_INVALID: The key is empty.
    Status Del(const std::string &key);

    /// \brief Invoke worker client to delete all the given keys.
    ///
    /// \param[in] keys The vector of the keys.
    /// \param[out] failedKeys The failed delete keys.
    ///
    /// \return K_OK on any key success; the error code otherwise.
    ///         K_INVALID: the vector of keys is empty or include empty kfey.
    Status Del(const std::vector<std::string> &keys, std::vector<std::string> &failedKeys);

    /**
     * @brief Generate a key with workerId.
     * @param[in] prefixKey The user specified key prefix.
     * @return The key with workerId, if the key fails to be generated, an empty string is returned.
     */
    std::string GenerateKey(const std::string &prefixKey = "");

    /// \brief Generate a key with workerId.
    ///
    /// \param[in] prefixKey The user specified key prefix.
    /// \param[out] key The key with workerId, if the key fails to be generated, an empty string is returned.
    ///
    /// \return K_OK on any object success; the error code otherwise.
    Status GenerateKey(const std::string &prefixKey, std::string &key);

    /**
     * @brief Update token for yr iam
     * @param[in] Token message for auth certification
     * @return K_OK on success; the error code otherwise.
     */
    Status UpdateToken(SensitiveValue token);

    /**
     * @brief Update aksk for yr iam
     * @param[in] acessKey message for auth certification
     * @param[in] secretKey message for auth certification
     * @return K_OK on success; the error code otherwise.
     */
    Status UpdateAkSk(const std::string accessKey, SensitiveValue secretKey);

    /**
     * @brief Invoke worker client to query the size of objectKeys (include the objectKeys of other AZ).
     * @param[in] objectKeys The objectKeys need to query size.
     * @param[out] outSizes The size for the objectKeys in bytes.
     * @return K_OK on success; the error code otherwise.
     *         K_INVALID: The objectKeys are empty or invalid.
     *         K_NOT_FOUND: All objectKeys not found.
     *         K_RPC_UNAVAILABLE: Network error.
     *         K_NOT_READY: Worker not ready.
     *         K_RUNTIME_ERROR: Can not get objectKey size from worker.
     */
    Status QuerySize(const std::vector<std::string> &objectKeys, std::vector<uint64_t> &outSizes);

    /// \brief Worker health check.
    ///
    /// \return K_OK on any object success; the error code otherwise.
    Status HealthCheck();

    /// \brief Check whether the keys exist in the data system.
    ///
    /// \param[in] keys The keys to be checked. Constraint: The number of keys cannot exceed 10000.
    /// \param[in] exists The existence of the corresponding key.
    ///
    /// \return K_OK if at least one key is successfully processed, the error code otherwise.
    Status Exist(const std::vector<std::string> &keys, std::vector<bool> &exists);

    /// \brief Sets expiration time for key list (in seconds)
    ///
    /// \param[in] key The keys to set expiration for.
    /// \param[in] ttlSeconds TTL in seconds. If the value is greater than 0, the data will be deleted automatically
    /// after expired. If set to 0, the data need to be manually deleted.
    /// \param[out] failedKeys The failed expire keys.
    /// \return K_OK on success; the error code otherwise.
    ///         K_INVALID: The key is empty or contains invalid characters.
    ///         K_NOT_FOUND: The key is not exist.
    ///         K_NOT_READY: Worker is not ready.
    ///         K_RPC_UNAVAILABLE: Network error.
    ///         K_RUNTIME_ERROR: Inner error happen.
    Status Expire(const std::vector<std::string> &keys, uint32_t ttlSeconds, std::vector<std::string> &failedKeys);
private:
    std::shared_ptr<object_cache::ObjectClientImpl> impl_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_KV_CACHE_KV_CLIENT_H
