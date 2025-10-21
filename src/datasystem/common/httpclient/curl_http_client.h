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
 * Description: curl implement http client
 */
#ifndef DATASYSTEM_COMMON_HTTPCLIENT_CURL_HTTP_CLIENT_H
#define DATASYSTEM_COMMON_HTTPCLIENT_CURL_HTTP_CLIENT_H

#include <atomic>
#include <condition_variable>
#include <mutex>
#include <vector>

#include <curl/curl.h>

#include "datasystem/common/httpclient/http_client.h"
#include "datasystem/utils/sensitive_value.h"

const std::string KEY_ACCESS_TOKEN = "access_token";
const std::string KEY_EXPIRES_IN = "expires_in";
const std::string KEY_TEAM_ID = "teamId";
const std::string KEY_PRODUCTS = "products";

namespace datasystem {
struct TslParam {
    SensitiveValue ca_;
    SensitiveValue cert_;
    SensitiveValue key_;
};

/**
 * @brief A internal class only use by class CurlHandlePool, the purpose is:
 * make sure a handler resource can be used by only one thread at a time.
 */
class CurlHandleManager {
public:
    CurlHandleManager();

    ~CurlHandleManager() = default;

    /**
     * @brief acquire curl resource from pool
     * 1. if pool is empty, block until pool has idle resource.
     * 2. if the pool has been shutdown, return nullptr.
     * @return CURL resource, or nullptr
     */
    CURL *Acquire();

    /**
     * @brief whether the pool has idle resource.
     * @return true if has idle resource, otherwise false.
     */
    bool HasIdleResource();

    /**
     * @brief give back CURL resource
     * @param[in] curl CURL resource
     */
    void Release(CURL *curl);

    /**
     * @brief block util all the resource give back
     * @param[in] createdNum the num of curl Resource has been created.
     * @return all created CURL resource.
     */
    std::vector<CURL *> ShutdownAndWait(size_t createdNum);

private:
    std::vector<CURL *> curlRepository_;
    std::mutex managerMutex_;
    std::condition_variable curlRepoSemaphore_;
    std::atomic<bool> shutDown_;
};

/**
 * @brief a internal class use only by class CurlHttpClient, the purpose is
 * caching the curl handle for reuse.
 * wo don't limit the max num of curl handle to create; because the datasystem business thread num has been limited.
 */
class CurlHandlePool {
public:
    CurlHandlePool() = default;

    ~CurlHandlePool();

    CurlHandlePool(const CurlHandlePool &) = delete;

    const CurlHandlePool &operator=(const CurlHandlePool &) = delete;

    CurlHandlePool(const CurlHandlePool &&) = delete;

    const CurlHandlePool &operator=(const CurlHandlePool &&) = delete;

    /**
     * @brief acquire a curl resource,
     * 1. if CurlHandlePool is empty and createdSize_ < maxSize_, create new curl resource
     * 2. if CurlHandlePool is not empty, get curl from CurlHandlePool.
     * 3. if CurlHandlePool is empty and createdSize_ = maxSize_, Acquire will block util get idle resource which
     * give back by other thread.
     *
     * @return curl resource
     */
    CURL *Acquire();

    /**
     * @brief thread who acquire curl resource must give back where the business is done.
     * @param[in] curl CURL resource
     * @param[in] cleanUp whether clean up the CURL resource
     */
    void Release(CURL *curl, bool cleanUp);

private:
    void CreateNewResource();
    CurlHandleManager curlHandleManager_;
    // the num of curl resource which has been created
    uint32_t createdSize_{ 0 };
    std::mutex createHandleMutex_;
};

class CurlHttpClient : public HttpClient {
public:
    /**
     * @brief This function gets called by libcurl as soon as it has received header data.
     * @param[in] buffer points to the delivered data
     * @param[in] size size is always 1
     * @param[in] nmemb the size of that buffer is nitems
     * @param[out] userdata HttpResponse
     * @return the number of bytes actually taken care of
     */
    static size_t RecvHeadersCallBack(char *buffer, size_t size, size_t nmemb, void *userdata);

    /**
     * @brief This callback function gets called by libcurl as soon as there is data received that needs to be saved.
     * @param[in] ptr points to the delivered data
     * @param[in] size size is always 1
     * @param[in] nmemb  the size of that data is nmemb
     * @param[out] userdata here is HttpResponse
     * @return the number of bytes actually taken
     */
    static size_t RecvBodyCallBack(char *ptr, size_t size, size_t nmemb, void *userdata);

    /**
     * @brief This callback function gets called by libcurl as soon as
     * it needs to read data in order to send it to the peer.
     * The data area pointed at by the pointer buffer should be filled up with
     * at most size multiplied with nitems number of bytes by your function.
     * @param[out] buffer the liburl data pointer
     * @param[in] size fill up the buffer at most size * nmemb
     * @param[in] nmemb fill up the buffer at most size * nmemb
     * @param[in] userdata HttpRequest
     * @return the actual number of bytes that it stored in the data area pointed at by the pointer buffer
     */
    static size_t SendBodyCallBack(char *buffer, size_t size, size_t nmemb, void *userdata);

    /**
     * @brief This function gets called by libcurl to seek to a certain position in the input stream and can be used to
     * fast forward a file in a resumed upload. It is also called to rewind a stream when data has already been sent to
     * the server and needs to be sent again. This may happen when doing an HTTP PUT or POST with a multi-pass
     * authentication method, or when an existing HTTP connection is reused too late and the server closes the
     * connection. connection
     * @param[in] userdata The instance of HttpRequest
     * @param[in] offset The offset.
     * @param[in] origin libcurl currently only passes SEEK_SET.
     * @return CURL_SEEKFUNC_OK on success, CURL_SEEKFUNC_FAIL to cause the upload operation to fail or
     * CURL_SEEKFUNC_CANTSEEK to indicate that while the seek failed.
     */
    static int SeekBodyCallBack(void *userdata, curl_off_t offset, int origin);

    /**
     * @brief libcurl implement of http client.
     * 1. only the first time call, libcurl global config will be init.
     * 2. once the constructor is called, clientNums_ will increase
     * @param[in] config http connection configuration
     */
    explicit CurlHttpClient(bool verifyServer = true);

    /**
     * @brief once the destructor is called, clientNums_ will decrease
     * if clientNums_ = 0, then clean up libcurl global config.
     */
    ~CurlHttpClient() override;

    /**
     * @brief Sned http request with tls.
     * @param[in] request Message of http request.
     * @param[out] response Message of http response.
     * @param[in] param Tsl param for http request.
     * @param[in] reqParam Request param.
     * @return Status of the call.
     */
    virtual Status TslSend(const std::shared_ptr<HttpRequest> &request, std::shared_ptr<HttpResponse> &response,
                           const TslParam &param);

    /**
     * @brief Send http Request
     * @param[in] request Message of http request.
     * @param[out] response Message of http response.
     * @param[in] curlHandle curl handle for send request
     * @param[in] reqParam Request param.
     * @return Status of the call
     */
    Status SendRequest(const std::shared_ptr<HttpRequest> &request, std::shared_ptr<HttpResponse> &response,
                       CURL *curlHandle);

    Status Send(const std::shared_ptr<HttpRequest> &request, std::shared_ptr<HttpResponse> &response) override;

private:
    /**
     * @brief Obtains the action corresponding to the HTTP request, for example, "POST /v1/api/token"
     * @param[in] request http request
     * @return the action of HTTP request
     */
    std::string GetHttpAction(const std::shared_ptr<HttpRequest> &request);

    /**
     * @brief Converts HttpMethod of the enumeration type to the string type.
     * @param[in] method HttpMethod.
     * @return  HttpMethod of the string type.
     */
    std::string GetHttpMethodName(HttpMethod method);

    static std::atomic<bool> curlGlobalInitFlag_;
    // the num of CurlHttpClient instance
    static int clientNums_;
    static std::mutex clientNumsMutex_;
    // curl handle cache pool for reuse
    std::shared_ptr<CurlHandlePool> curlHandlePool_;
    bool verifyServer_;
    std::string userAgent_;

    /**
     * @brief protect curl_global_init call only once after the program start.
     *
     */
    void GlobalInit();

    /**
     * @brief protect curl_global_cleanup call only once when the program finish.
     */
    static void GlobalCleanUp();

    /**
     * @brief config the curl option for every request.
     * @param[in] handle curl handle
     */
    void ConfigCurlDefaultOption(CURL *handle);

    /**
     * @brief config the request method for curl.
     * @param[in] handle curl resource
     * @param[in] request http request
     */
    void ConfigCurlMethod(CURL *handle, const std::shared_ptr<HttpRequest> &request);

    /**
     * @brief set the callback function for curl
     * @param[in] handle curl handle
     * @param[in] request http request
     * @param[in] response http response
     */
    void SetCallBackFunc(CURL *handle, const std::shared_ptr<HttpRequest> &request,
                         std::shared_ptr<HttpResponse> &response);

    /**
     * @brief set the callback function for curl
     * @param[in] request http request
     * @param[in] list linked-list structure for the CURLOPT_QUOTE option
     */
    void HandleHeader(const std::shared_ptr<HttpRequest> &request, curl_slist *&list);
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_HTTPCLIENT_CURL_HTTP_CLIENT_H
