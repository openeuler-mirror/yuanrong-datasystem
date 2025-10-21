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
 * Description: Interface to OBS SDK.
 */

#ifndef DATASYSTEM_COMMON_L2CACHE_OBS_CLIENT_OBS_CLIENT_H
#define DATASYSTEM_COMMON_L2CACHE_OBS_CLIENT_OBS_CLIENT_H

#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>

#include <eSDKOBS.h>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/l2cache/l2cache_client.h"
#include "datasystem/common/l2cache/obs_client/cloud_service_rotation.h"
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
class ObsClient : public L2CacheClient {
public:
    struct ObsPutBuffer {
        std::shared_ptr<std::iostream> buffer{ nullptr };
        size_t bufferSize{ 0 };
        size_t offset{ 0 };
        obs_status status{ OBS_STATUS_BUTT };
    };

    struct GetObjectBuffer {
        std::shared_ptr<std::stringstream> buffer{ nullptr };
        size_t size{ 0 };
        obs_status status{ OBS_STATUS_BUTT };
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

    struct ObsRsp {
        obs_status status{ OBS_STATUS_BUTT };
    };

    struct OnePartUploadBuffer {
        static constexpr size_t ETAG_LENGTH = 1024;
        std::shared_ptr<std::iostream> buffer{ nullptr };
        char eTag[ETAG_LENGTH] = {0};
        char *uploadId{ nullptr };
        size_t partNum{ 0 };
        size_t partSize{ 0 };
        size_t offset{ 0 };
        obs_options *option{ nullptr };
        char *key{ nullptr };
        std::mutex *mx{ nullptr };  // pointing to datasystem::ObsClient::mx_
        obs_status status{ OBS_STATUS_BUTT };
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

        ObsObject(const char *keyArg, int64_t lastModifiedArg, const char *etagArg, uint64_t sizeArg,
                  const char *ownerIdArg, const char *ownerDisplayNameArg, const char *storageClassArg,
                  const char *typeArg)
        {
            if (keyArg != nullptr) {
                key = std::string(keyArg);
            }
            lastModified = lastModifiedArg;
            if (etagArg != nullptr) {
                etag = std::string(etagArg);
            }
            size = sizeArg;
            if (ownerIdArg != nullptr) {
                ownerId = std::string(ownerIdArg);
            }
            if (ownerDisplayNameArg != nullptr) {
                ownerDisplayName = std::string(ownerDisplayNameArg);
            }
            if (storageClassArg != nullptr) {
                storageClass = std::string(storageClassArg);
            }
            if (typeArg != nullptr) {
                type = std::string(typeArg);
            }
        }
    };

    struct ListObjectData {
        int isTruncated{ 0 };
        std::string nextMarker;
        int keyCount{ 0 };
        int commonPrefixCount{ 0 };
        std::vector<std::string> commonPrefixes;
        std::vector<ObsObject> objects;
        obs_status status{ OBS_STATUS_BUTT };
    };

    ObsClient(const std::string &endPoint, const std::string &bucketName)
        : endPoint_(endPoint), bucketName_(bucketName), optGenerator_(this)
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
     * @param[in] listIncompleteVersions whether to list those incomplete versions. Usually they are partially uploaded.
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

    class OptionGenerator {
    public:
        OptionGenerator(ObsClient *client) : client_(client)
        {
        }
        OptionGenerator() = delete;

        ~OptionGenerator()
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
         * @brief Specifies the Obs option for which an Obs request needs to be sent.
         * @param[in,out] opts Obs options.
         * @param[in,out] tempCredential Credentials required for a temporary token authentications.
         */
        void GenerateObsOption(obs_options &opts, ObsTempCredential &tempCredential);

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
        * @return Status of the call
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
         * @brief Options required for constructing common AK/SK authentication.
         * @param[in,out] opts Obs options.
         */
        void GenerateCommonObsOption(obs_options &opts);

        /**
         * @brief Options required for constructing temporary token authentication.
         * @param[in,out] opts Obs options.
         * @param[in,out] tempCredential Temp credential that has been decrypted.
         */
        void GenerateTokenRotationObsOption(obs_options &opts, ObsTempCredential &tempCredential);

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
     * @brief Initialize multipart upload.
     * @param[in] objectPath the object path in obs bucket
     * @param[in] opts obs_options
     * @param[in] putProperties obs_put_properties
     * @param[in] timer timer recording elapsed time for timeout limit
     * @param[in] uploadIdSize size of upload ID
     * @param[in] uploadId returned upload ID
     * @return Status of the call
     */
    Status InitMultiPartUpload(const std::string &objPath, obs_options &opts, obs_put_properties &putProperties,
                               Timer &timer, const size_t uploadIdSize, char *uploadId);

    /**
     * @brief Submit threads each uploading a part of an object.
     * @param[in] multiPartBuffer struct holding object body and information of partition
     * @param[in] objectPath the object path in obs bucket
     * @param[in] bufferSize size of the whole buffer/object
     * @param[in] uploadId upload ID
     * @param[in] options obs_options for all threads
     * @param[in] parts vector of OnePartUploadBuffer all threads, containing object and information for partition
     * @return Status of the call
     */
    Status SubmitUploadThreads(const MultiPartUploadBuffer &multiPartBuffer, const std::string &objPath,
                               size_t bufferSize, char *uploadId, std::vector<obs_options> &options,
                               std::vector<OnePartUploadBuffer> &parts);

    /**
     * @brief Upload one part/partition using one thread
     * @param[in] onePartUploadBuffer containing object and information for partition
     * @return Status of the call
     */
    Status OnePartUpload(OnePartUploadBuffer &onePartUploadBuffer);

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
    Status ListObjects(const std::string &objectPrefix, Timer &timer, std::shared_ptr<GetObjectInfoListResp> &listResp);

    /**
     * @brief Delete objects in batch in [beg, end)
     * @param[in] objects the object paths in obs bucket to delete from
     * @param[in] beg begin index of the deleted objects
     * @param[in] end end index of the deleted objects
     * @return Status of the call
     */
    Status BatchDeleteObjects(const std::vector<std::string> &objects, size_t beg, size_t end);

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

    static void CreateBucketRspCompleteCallback(obs_status status, const obs_error_details *errorDetails,
                                                void *callbackData);

    static obs_status PutRspPropertiesCallback(const obs_response_properties *properties, void *callbackData);
    static void PutRspCompleteCallback(obs_status status, const obs_error_details *errorDetails, void *callbackData);
    static int PutObjectDataCallback(int bufferSize, char *buffer, void *callbackData);

    static obs_status MultiUploadRspPropertiesCallback(const obs_response_properties *properties, void *callbackData);
    static void MultiUploadRspCompleteCallback(obs_status status, const obs_error_details *errorDetails,
                                               void *callbackData);
    static obs_status MultiUploadCompleteCallback(const char *location, const char *bucket, const char *key,
                                                  const char* eTag, void *callbackData);
    static obs_status OneUploadRspPropertiesCallback(const obs_response_properties *properties, void *callbackData);
    static void OneUploadRspCompleteCallback(obs_status status, const obs_error_details *error, void *callbackData);
    static int OneUploadDataCallback(int bufferSize, char *buffer, void *callbackData);

    static obs_status GetObjRspPropertiesCallback(const obs_response_properties *properties, void *callbackData);
    static void GetObjResponseCompleteCallback(obs_status status,  const obs_error_details *errorDetails,
                                               void *callbackData);
    static obs_status GetObjDataCallback(int bufferSize, const char *buffer, void *callbackData);

    static obs_status ListObjRspPropertiesCallback(const obs_response_properties *properties, void *callbackData);
    static void ListObjRspCompleteCallback(obs_status status,  const obs_error_details *errorDetails,
                                           void *callbackData);
    static obs_status ListObjectsCallback(int isTruncated, const char *nextMarker, int contentsCount,
                                          const obs_list_objects_content *contents, int commonPrefixesCount,
                                          const char **commonPrefixes, void *callbackData);

    static obs_status BatchDelObjRspPropertiesCallback(const obs_response_properties *properties, void *callbackData);
    static void BatchDelObjResponseCompleteCallback(obs_status status,  const obs_error_details *errorDetails,
                                                    void *callbackData);
    static obs_status BatchDelObjectsDataCallback(int contentsCount, obs_delete_objects *delObjs, void *callbackData);

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
    OptionGenerator optGenerator_;
    std::atomic_bool initialized_{ false };
    MetricsObsCodeVector successRateVec_;
};
}
#endif