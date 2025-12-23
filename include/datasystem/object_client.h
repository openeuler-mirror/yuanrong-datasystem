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
 * Description: Data system object cache client management.
 */

#ifndef DATASYSTEM_OBJECT_CLIENT_H
#define DATASYSTEM_OBJECT_CLIENT_H

#include <memory>
#include <unordered_set>
#include <vector>

#include "datasystem/context/context.h"
#include "datasystem/object/buffer.h"
#include "datasystem/object/object_enum.h"
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
struct CreateParam {
    ConsistencyType consistencyType = ConsistencyType::PRAM;
    CacheType cacheType = CacheType::MEMORY;
};

struct ObjMetaInfo {
    uint64_t objSize{ 0 };               // the size of object data, 0 if object not found.
    std::vector<std::string> locations;  // the workerIds of the locations
};

class __attribute((visibility("default"))) ObjectClient : public std::enable_shared_from_this<ObjectClient> {
public:
    /// \brief Construct ObjectClient.
    ///
    /// \param[in] connectOptions the connect options.
    explicit ObjectClient(const ConnectOptions &connectOptions = {});

    ~ObjectClient();

    /// \brief Shutdown the object client.
    ///
    /// \return K_OK on success; the error code otherwise.
    Status ShutDown();

    /// \brief Init the object client.
    ///
    /// \return K_OK on success; the error code otherwise.
    Status Init();

    /// \brief Increase the global reference count to objects in the data system.
    ///
    /// \param[in] objectKeys The object keys to increase, it cannot be empty. ID should not be empty and should only
    ///  contains english alphabetics (a-zA-Z), numbers and ~!@#$%^&*.-_ only. ID length should less than 256.
    /// \param[out] failedObjectKeys Increase failed object keys.
    ///
    /// \return K_OK on any object success, the failedObjectKeys indicate the failed list.
    ///         K_RPC_UNAVAILABLE: Disconnect from worker or master.
    ///         K_INVALID: The parameter is invalid.
    Status GIncreaseRef(const std::vector<std::string> &objectKeys, std::vector<std::string> &failedObjectKeys);

    /// \brief Decrease the global reference count to objects in the data system.
    ///
    /// \param[in] objectKeys The object keys to decrease, it cannot be empty. ID should not be empty and should only
    ///  contains english alphabetics (a-zA-Z), numbers and ~!@#$%^&*.-_ only. ID length should less than 256.
    /// \param[out] failedObjectKeys Decrease failed object keys.
    ///
    /// \return K_OK on any object success, the failedObjectKeys indicate the failed list.
    ///         K_RPC_UNAVAILABLE: Disconnect from worker or master.
    ///         K_INVALID: The parameter is invalid.
    Status GDecreaseRef(const std::vector<std::string> &objectKeys, std::vector<std::string> &failedObjectKeys);

    /// \brief Release obj Ref of remote client id when remote client that outside the cloud crash.
    ///
    /// \param[in] remoteClientId The remote client id of the client that outside the cloud.
    ///
    /// \return K_OK on any object success, the failedObjectKeys indicate the failed list.
    ///         K_RPC_UNAVAILABLE: Disconnect from worker or master.
    ///         K_INVALID: The parameter is invalid.
    Status ReleaseGRefs(const std::string &remoteClientId);

    /// \brief Query all objects global references in the cluster (out-cloud reference not included).
    ///
    /// \param[in] objectKey The specific object key that to query. ID should not be empty and should only contains
    ///  letters, numbers and ~!@#$%^&*.-_ only. ID length should less than 256.
    ///
    /// \return The objects' global reference num; -1 in case of failure.
    int QueryGlobalRefNum(const std::string &objectKey);

    /// \brief Invoke worker client to create an object.
    ///
    /// \param[in] objectKey The ID of the object to create. ID should not be empty and should only contains english
    ///  alphabetics (a-zA-Z), numbers and ~!@#$%^&*.-_ only. ID length should less than 256.
    /// \param[in] size The size in bytes of object.
    /// \param[in] param The create parameters.
    /// \param[out] buffer The buffer for the object.
    ///
    /// \return K_OK on success; the error code otherwise.
    ///         K_INVALID: the key or val is empty.
    ///         K_RUNTIME_ERROR: client fd mmap failed
    Status Create(const std::string &objectKey, uint64_t size, const CreateParam &param,
                  std::shared_ptr<Buffer> &buffer);

    /// \brief Invoke worker client to put an object (publish semantics).
    ///
    /// \param[in] objectKey The ID of the object to create. ID should not be empty and should only contains english
    ///  alphabetics (a-zA-Z), numbers and ~!@#$%^&*.-_ only. ID length should less than 256.
    /// \param[in] data The data pointer of the user.
    /// \param[in] size The size in bytes of object.
    /// \param[in] param The create parameters.
    /// \param[in] nestedObjectKeys Objects that depend on objectKey.
    ///
    /// \return K_OK on success; the error code otherwise.
    Status Put(const std::string &objectKey, const uint8_t *data, uint64_t size, const CreateParam &param,
               const std::unordered_set<std::string> &nestedObjectKeys = {});

    /// \brief Invoke worker client to get all buffers of all the given object keys.
    ///
    /// \param[in] objectKeys The vector of the object key. ID should not be empty and should only contains english
    ///  alphabetics (a-zA-Z), numbers and ~!@#$%^&*.-_ only. ID length should less than 256.
    /// \param[in] subTimeoutMs Timeout(ms) of waiting for the result return if object not ready. A positive integer
    ///  number required. 0 means no waiting time allowed. And the range is [0, INT32_MAX].
    /// \param[out] buffers The return vector of the objects.
    ///
    /// \return K_OK on any object success; the error code otherwise.
    ///        K_INVALID: the vector of keys is empty or include empty key.
    ///        K_NOT_FOUND: The objects not exists.
    ///        K_RUNTIME_ERROR: Cannot get objects from worker.
    Status Get(const std::vector<std::string> &objectKeys, int32_t subTimeoutMs,
               std::vector<Optional<Buffer>> &buffers);

    /// \brief Update token for yr iam
    ///
    /// \param[in] token message for auth certification
    ///
    /// \return K_OK on success; the error code otherwise.
    Status UpdateToken(SensitiveValue token);

    /// \brief Update aksk for yr iam
    ///
    /// \param[in] accessKey message for auth certification
    /// \param[in] secretKey message for auth certification
    ///
    /// \return K_OK on success; the error code otherwise.
    Status UpdateAkSk(const std::string accessKey, SensitiveValue secretKey);

    /// \brief Get meta info of the given objects.
    ///
    /// \param[in] tenantId The tenant that the objs belong to.
    /// \param[in] objectKeys The vector of the object key.
    /// \param[out] objMetas The vector of the return metas.
    ///
    /// \return K_OK on any object success.
    ///        K_INVALID: the vector of keys is empty or include invalid key.
    ///        K_RPC_UNAVAILABLE: Disconnect from worker or master.
    ///        K_NOT_AUTHORIZED: The client is not authorized for the tenantId.
    Status GetObjMetaInfo(const std::string &tenantId, const std::vector<std::string> &objectKeys,
                          std::vector<ObjMetaInfo> &objMetas);

    /// \brief Add the workerUuid as a suffix to the objectKey.
    ///
    /// \param[in] prefix The objectKey generated by user.
    /// \param[out] key The key with workerUuid.
    ///
    /// \return K_OK on any object success; the error code otherwise.
    Status GenerateKey(const std::string &prefix, std::string &key);

    /// \brief Add the workerUuid as a suffix to the objectId.
    ///
    /// \param[in] prefix The objectId generated by user.
    /// \param[out] objectId The key with workerUuid.
    ///
    /// \return K_OK on any object success; the error code otherwise.
    Status GenerateObjectKey(const std::string &prefix, std::string &objectId);

    /// \brief Get objectKey from a key with workerUuid.
    ///
    /// \param[in] key The key with workerUuid.
    /// \param[out] prefix The objectKey.
    ///
    /// \return K_OK on any object success; the error code otherwise.
    Status GetPrefix(const std::string &key, std::string &prefix);

    /// \brief Worker health check.
    ///
    /// \return K_OK on any object success; the error code otherwise.
    Status HealthCheck();

private:
    std::shared_ptr<object_cache::ObjectClientImpl> impl_;
};
}  // namespace datasystem
#endif  // DATASYSTEM_OBJECT_CACHE_OBJECT_CLIENT_H
