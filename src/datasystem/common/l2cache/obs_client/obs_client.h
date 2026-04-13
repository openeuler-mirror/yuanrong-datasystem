/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: Interface to OBS via HTTP REST API.
 */

#ifndef DATASYSTEM_COMMON_L2CACHE_OBS_CLIENT_OBS_CLIENT_H
#define DATASYSTEM_COMMON_L2CACHE_OBS_CLIENT_OBS_CLIENT_H

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <shared_mutex>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/httpclient/http_client.h"
#include "datasystem/common/httpclient/http_request.h"
#include "datasystem/common/httpclient/http_response.h"
#include "datasystem/common/l2cache/l2cache_client.h"
#include "datasystem/common/l2cache/obs_client/cloud_service_rotation.h"
#include "datasystem/common/l2cache/obs_client/obs_signature.h"
#include "datasystem/common/l2cache/obs_client/obs_xml_util.h"
#include "datasystem/common/metrics/metrics_vector/metrics_obs_code_vector.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/thread.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/utils/sensitive_value.h"

namespace datasystem {
class GetObjectInfoListResp;
class L2CacheClient;

/**
 * @brief Snapshot of OBS credential used for a single HTTP request.
 */
struct ObsCredential {
    std::string ak;
    std::string sk;
    std::string token;  // empty for AK/SK mode, non-empty for token rotation mode
};

class ObsClient : public L2CacheClient {
public:
    struct ObsPutBuffer {
        std::shared_ptr<std::iostream> buffer{ nullptr };
        size_t bufferSize{ 0 };
        size_t offset{ 0 };
        int httpStatus{ 0 };
    };

    struct GetObjectBuffer {
        std::shared_ptr<std::stringstream> buffer{ nullptr };
        size_t size{ 0 };
        int httpStatus{ 0 };
    };

    struct MultiPartUploadBuffer {
        std::shared_ptr<std::iostream> buffer{ nullptr };
        int partNum{ 0 };
        size_t partSize{ 0 };
        size_t offset{ 0 };
        int noStatus{ 0 };
        // mutex to protect the iostream holding buffer when performing multipart upload by threads
        mutable std::mutex mx;
    };

    struct OnePartUploadBuffer {
        static constexpr size_t ETAG_LENGTH = 1024;
        std::shared_ptr<std::iostream> buffer{ nullptr };
        std::string eTag;
        std::string uploadId;
        size_t partNum{ 0 };
        size_t partSize{ 0 };
        size_t offset{ 0 };
        std::string key;
        std::mutex *mx{ nullptr };  // pointing to ObsClient::multiPartUploadMx_
        int httpStatus{ 0 };
    };

    struct ObsObject {
        std::string key;
        int64_t lastModified;
        std::string etag;
        size_t size;
        std::string ownerId;
        std::string ownerDisplayName;
        std::string storageClass;
        std::string type;

        ObsObject() = default;
        ObsObject(const std::string &keyArg, int64_t lastModifiedArg, const std::string &etagArg, uint64_t sizeArg,
                  const std::string &ownerIdArg, const std::string &ownerDisplayNameArg,
                  const std::string &storageClassArg, const std::string &typeArg)
            : key(keyArg),
              lastModified(lastModifiedArg),
              etag(etagArg),
              size(sizeArg),
              ownerId(ownerIdArg),
              ownerDisplayName(ownerDisplayNameArg),
              storageClass(storageClassArg),
              type(typeArg)
        {
        }
    };

    struct ListObjectData {
        int isTruncated{ 0 };
        std::string nextMarker;
        int keyCount{ 0 };
        int commonPrefixCount{ 0 };
        std::vector<std::string> commonPrefixes;
        std::vector<ObsObject> objects;
        int httpStatus{ 0 };
    };

    ObsClient(const std::string &endPoint, const std::string &bucketName)
        : endPoint_(endPoint), bucketName_(bucketName), credentialManager_(this)
    {
    }

    ObsClient() = delete;
    ObsClient(const ObsClient &other) = delete;
    ObsClient &operator=(const ObsClient &other) = delete;
    ~ObsClient() override;

    /**
     * @brief init the ObsClient
     * @return Status of the call
     */
    Status Init() override;

    /**
     * @brief Upload the object content to obs objectPath
     * @param[in] objectPath the object path in obs bucket
     * @param[in] timeoutMs the connect and request timeout in million second
     * @param[in] body the object content
     * @param[in] asyncElapse The time this object being in the async queue
     * @return Status of the call
     */
    Status Upload(const std::string &objectPath, int64_t timeoutMs, const std::shared_ptr<std::iostream> &body,
                  uint64_t asyncElapse = 0) override;

    /**
     * @brief get the object list from obs
     * @param[in] objectPrefix the object path in obs bucket
     * @param[in] timeoutMs the connect and request timeout in million second
     * @param[in] listIncompleteVersions whether to list those incomplete versions.
     * @param[out] listResp the object listResp
     * @return Status of the call
     */
    Status List(const std::string &objectPrefix, int64_t timeoutMs, bool listIncompleteVersions,
                std::shared_ptr<GetObjectInfoListResp> &listResp) override;

    /**
     * @brief get the object content from obs
     * @param[in] objectPath the object path in obs bucket
     * @param[in] timeoutMs the connect and request timeout in million second
     * @param[out] content the object content
     * @return Status of the call
     */
    Status Download(const std::string &objectPath, int64_t timeoutMs,
                    std::shared_ptr<std::stringstream> &content) override;

    /**
     * @brief delete the obs object.
     * @param[in] objectPaths the whole path of the object, not support prefix
     * @param[in] asyncElapse The time this object being in the async queue
     * @return Status of the call
     */
    Status Delete(const std::vector<std::string> &objectPaths, uint64_t asyncElapse = 0) override;

    /**
     * @brief Only for test.
     * @return Status of the call.
     */
    Status CheckValidRotationToken();

    /**
     * @brief Obtains the request success rate of obs.
     * @return Success rate of obs request.
     */
    std::string GetRequestSuccessRate() override
    {
        return successRateVec_.BlockingGetRateToStringAndClean();
    }

private:
    struct ObsTempCredential {
        std::unique_ptr<char[]> tempAccessKey;
        int accessKeyLen = 0;
        std::unique_ptr<char[]> tempSecretKey;
        int secretKeyLen = 0;
        std::unique_ptr<char[]> tempToken;
        int tokenLen = 0;

        ~ObsTempCredential()
        {
            ClearUniqueChar(tempAccessKey, accessKeyLen);
            ClearUniqueChar(tempSecretKey, secretKeyLen);
            ClearUniqueChar(tempToken, tokenLen);
        }
    };

    /**
     * @brief Manages OBS credentials (AK/SK and token rotation).
     * Provides thread-safe credential snapshots via GetCredential().
     */
    class ObsCredentialManager {
    public:
        ObsCredentialManager(ObsClient *client) : client_(client)
        {
        }
        ObsCredentialManager() = delete;

        ~ObsCredentialManager()
        {
            if (accessKey_ != nullptr && accessKeyLen_ > 0) {
                int ret = memset_s(accessKey_.get(), accessKeyLen_, 0, accessKeyLen_);
                if (ret != EOK) {
                    LOG(WARNING) << FormatString("memset failed, ret = %d", ret);
                }
            }
            if (secretKey_ != nullptr && secretKeyLen_ > 0) {
                int ret = memset_s(secretKey_.get(), secretKeyLen_, 0, secretKeyLen_);
                if (ret != EOK) {
                    LOG(WARNING) << FormatString("memset failed, ret = %d", ret);
                }
            }
        }

        bool Init();

        /**
         * @brief Get a credential snapshot for signing an HTTP request.
         * @return ObsCredential snapshot with ak, sk, and optional token.
         */
        ObsCredential GetCredential();

        /**
         * @brief Update credentials of temporary token authentications.
         * @return Status of the call.
         */
        Status UpdateCredentialInfo();

        /**
         * @brief Only for test.
         * @param[in] encryptedAk The encrypted ak.
         * @param[in] encryptedSk The encrypted sk.
         * @param[in] encryptedToken The encrypted token.
         * @return Status of the call.
         */
        Status VerifyEncryptedCredential(const std::string &encryptedAk, const std::string &encryptedSk,
                                         const std::string &encryptedToken);

    private:
        /**
         * @brief Decrypt ak/sk.
         * @return Status of the call.
         */
        Status DecryptAKSK();

        /**
         * @brief Decrypts ciphertext into plaintext.
         * @param[in] cipher The ciphertext needs to decrypt.
         * @param[out] plainText The plaintext after decrypting ciphertext.
         * @param[out] outSize The length of plaintext.
         * @return Status of the call.
         */
        Status Decrypt(const std::string &cipher, std::unique_ptr<char[]> &plainText, int &outSize);

        struct EncryptedCredentialInfo {
            SensitiveValue access;
            SensitiveValue secret;
            SensitiveValue securityToken;
        };

        /**
         * @brief Check whether all the credentials have been initialized.
         * @return true if all credentials have been initialized.
         */
        bool IsCredentialInitialized() const;

        /**
         * @brief Decrypts temp ak, sk or token into plaintext.
         * @param[in] info Encrypted ak, sk or token.
         * @param[in,out] textInfo Ak, sk or token that need to be used.
         * @param[in,out] textLen Ak, sk or token length.
         * @return K_OK if Decrypt info success, K_RUNTIME_ERROR otherwise.
         */
        Status DecryptOneInfo(const SensitiveValue &info, std::unique_ptr<char[]> &textInfo, int &textLen);

        // All are null terminated.
        std::unique_ptr<char[]> endPoint_;
        std::unique_ptr<char[]> bucketName_;
        std::unique_ptr<char[]> accessKey_;
        int accessKeyLen_ = 0;
        std::unique_ptr<char[]> secretKey_;
        int secretKeyLen_ = 0;
        EncryptedCredentialInfo encryptedInfos_;
        std::shared_timed_mutex optionMutex_;  // protect encryptedInfos_.
        ObsClient *client_;
    };

    /**
     * @brief Initialize Obs client in ak/sk authentication mode.
     * @return Status of the call.
     */
    Status ObsClientInitByAkSk();

    /**
     * @brief Initialize Obs client in temporary token authentication mode.
     * @return Status of the call.
     */
    Status ObsClientInitByToken();

    /**
     * @brief Initialize token rotation.
     * @return Status of the call.
     */
    Status TokenRotationInit();

    /**
     * @brief Streaming upload. Used for objects of small size.
     * @param[in] body iostream holding object
     * @param[in] size object size
     * @param[in] objectPath the object path in obs bucket
     * @param[in] timer timer recording elapsed time for timeout limit
     * @return Status of the call
     */
    Status StreamingUpload(const std::shared_ptr<std::iostream> &body, size_t size, const std::string &objPath,
                           Timer &timer);

    /**
     * @brief Multipart upload. Used for objects of large size.
     * @param[in] body iostream holding object
     * @param[in] size object size
     * @param[in] objectPath the object path in obs bucket
     * @param[in] partitionSize size of each part/partition
     * @param[in] timer timer recording elapsed time for timeout limit
     * @return Status of the call
     */
    Status MultiPartUpload(const std::shared_ptr<std::iostream> &body, size_t size, const std::string &objPath,
                           size_t partitionSize, Timer &timer);

    /**
     * @brief Initialize multipart upload via REST API.
     * @param[in] objectPath the object path in obs bucket
     * @param[in] timer timer recording elapsed time for timeout limit
     * @param[out] uploadId returned upload ID
     * @return Status of the call
     */
    Status InitMultiPartUpload(const std::string &objPath, Timer &timer, std::string &uploadId);

    /**
     * @brief Submit threads each uploading a part of an object.
     * @param[in] multiPartBuffer struct holding object body and information of partition
     * @param[in] objectPath the object path in obs bucket
     * @param[in] bufferSize size of the whole buffer/object
     * @param[in] uploadId upload ID
     * @param[in] parts vector of OnePartUploadBuffer all threads
     * @return Status of the call
     */
    Status SubmitUploadThreads(const MultiPartUploadBuffer &multiPartBuffer, const std::string &objPath,
                               size_t bufferSize, const std::string &uploadId,
                               std::vector<OnePartUploadBuffer> &parts);

    /**
     * @brief Upload one part/partition using one thread
     * @param[in] onePartUploadBuffer containing object and information for partition
     * @return Status of the call
     */
    Status OnePartUpload(OnePartUploadBuffer &onePartUploadBuffer);

    /**
     * @brief Abort a multipart upload.
     * @param[in] objectPath the object path in obs bucket
     * @param[in] uploadId upload ID
     * @return Status of the call
     */
    Status AbortMultipartUpload(const std::string &objectPath, const std::string &uploadId);

    /**
     * @brief Get an object from obs.
     * @param[in] objPath the object path in obs bucket
     * @param[in] buf stringstream holding obtained object
     * @param[in] timer timer recording elapsed time for timeout limit
     * @return Status of the call
     */
    Status GetObject(const std::string &objPath, std::shared_ptr<std::stringstream> &buf, Timer &timer);

    /**
     * @brief List objects using prefix.
     * @param[in] objectPrefix prefix of object path in obs bucket
     * @param[in] timer timer recording elapsed time for timeout limit
     * @param[in] listResp GetObjectInfoListResp holding object information
     * @return Status of the call
     */
    Status ListObjects(const std::string &objectPrefix, Timer &timer,
                       std::shared_ptr<GetObjectInfoListResp> &listResp);

    /**
     * @brief Build and send a single ListObjects HTTP request for one page.
     * @param[in] objectPrefix Object prefix filter.
     * @param[in] marker Pagination marker (empty for first page).
     * @param[in] maxKeys Maximum keys per page.
     * @param[in] timeoutMs Request timeout.
     * @param[in] credential OBS credential.
     * @param[out] respBody Response XML body.
     * @return Status of the call.
     */
    Status SendListObjectsRequest(const std::string &objectPrefix, const std::string &marker,
                                  uint16_t maxKeys, int64_t timeoutMs, const ObsCredential &credential,
                                  std::string &respBody);

    /**
     * @brief Delete objects in batch in [beg, end)
     * @param[in] objects the object paths in obs bucket to delete from
     * @param[in] beg begin index of the deleted objects
     * @param[in] end end index of the deleted objects
     * @return Status of the call
     */
    Status BatchDeleteObjects(const std::vector<std::string> &objects, size_t beg, size_t end);

    /**
     * @brief Sign an HTTP request with OBS V2 signature.
     * @param[in] credential AK/SK credential snapshot.
     * @param[in,out] request The HTTP request to sign.
     * @param[in] contentMd5 MD5 of request body (optional).
     * @param[in] subResources Sub-resource query parameters for canonical resource.
     * @return Status of the call.
     */
    Status SignRequest(const ObsCredential &credential, std::shared_ptr<HttpRequest> &request,
                       const std::string &contentMd5,
                       const std::map<std::string, std::string> &subResources);

    /**
     * @brief Build a fully configured HTTP request for OBS.
     * @param[in] method HTTP method.
     * @param[in] objectKey Object key.
     * @param[in] timeoutMs Request timeout.
     * @param[in] subResources Sub-resource query parameters.
     * @return Configured HttpRequest shared_ptr.
     */
    std::shared_ptr<HttpRequest> BuildRequest(HttpMethod method, const std::string &objectKey, int64_t timeoutMs,
                                              const std::map<std::string, std::string> &subResources = {});

    /**
     * @brief Read csms token for build iam request header.
     * @param[in,out] csmsToken Csms token text.
     * @return Status of the call.
     */
    Status ReadCSMSToken(SensitiveValue &csmsToken);

    /**
     * @brief Obtain the configs required for token rotation.
     * @param[in,out] ccmsConfig Csms rotation configs.
     * @return Status of the call.
     */
    Status ReadConfigFromEnv(CCMSTokenConf &ccmsConfig);

    /**
     * @brief Start token rotation action.
     */
    void StartTokenRotation();

    /**
     * @brief Update temporary token authentication parameters.
     * @return Status of the call.
     */
    Status UpdateTempObsToken();

    const int NUM_THREAD = 10;
    std::unique_ptr<ThreadPool> threadPool_{ nullptr };  // Used for concurrent multipart upload.
    std::mutex multiPartUploadMx_;
    CredentialInfo obsTempCredentialInfo_;
    std::atomic<bool> isTokenRotationStarting_{ false };
    std::mutex rotationMutex_;  // protect rotationCv_
    std::condition_variable rotationCv_;
    Thread rotationThread_;
    size_t rotationIntervalSec_ = 0;
    std::string endPoint_;  // OBS host to connect to
    std::string bucketName_;
    ObsCredentialManager credentialManager_;
    std::shared_ptr<HttpClient> httpClient_;
    std::atomic_bool initialized_{ false };
    MetricsObsCodeVector successRateVec_;
};
}  // namespace datasystem
#endif
