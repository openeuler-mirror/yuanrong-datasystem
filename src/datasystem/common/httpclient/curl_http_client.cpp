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
#include "datasystem/common/httpclient/curl_http_client.h"

#include <atomic>
#include <climits>
#include <condition_variable>
#include <mutex>
#include <sstream>
#include <string>

#include <nlohmann/json.hpp>

#include "datasystem/common/httpclient/http_message.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/perf/perf_manager.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/ssl_authorization.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/utils/status.h"

namespace datasystem {
static const size_t RECV_ERR = -1;
static const std::string CONTENT_LENGTH = "Content-Length";
static const size_t LOG_HTTP_BODY_SIZE_LIMIT = 80;

CurlHandleManager::CurlHandleManager() : shutDown_(false)
{
}

CURL *CurlHandleManager::Acquire()
{
    std::unique_lock<std::mutex> lock(managerMutex_);
    curlRepoSemaphore_.wait(lock, [&]() { return shutDown_.load() || !curlRepository_.empty(); });

    if (shutDown_) {
        return nullptr;
    }

    CURL *curl = curlRepository_.back();
    curlRepository_.pop_back();
    return curl;
}

bool CurlHandleManager::HasIdleResource()
{
    std::lock_guard<std::mutex> lock(managerMutex_);
    return !shutDown_.load() && !curlRepository_.empty();
}

void CurlHandleManager::Release(CURL *curl)
{
    std::unique_lock<std::mutex> lock(managerMutex_);
    curlRepository_.push_back(curl);
    lock.unlock();
    curlRepoSemaphore_.notify_one();
}

std::vector<CURL *> CurlHandleManager::ShutdownAndWait(size_t createdNum)
{
    std::unique_lock<std::mutex> lock(managerMutex_);
    shutDown_ = true;
    // wait other thread give back curl resource.
    curlRepoSemaphore_.wait(lock, [&]() { return curlRepository_.size() >= createdNum; });

    std::vector<CURL *> usingResource = std::move(curlRepository_);
    return usingResource;
}

CurlHandlePool::~CurlHandlePool()
{
    for (CURL *curl : curlHandleManager_.ShutdownAndWait(createdSize_)) {
        // easy_clean_up will cause ssl segment fault
        (void)curl;
    }
}

CURL *CurlHandlePool::Acquire()
{
    if (!curlHandleManager_.HasIdleResource()) {
        CreateNewResource();
    }
    // if pool empty,will block until pool has idle resource.
    return curlHandleManager_.Acquire();
}

void CurlHandlePool::CreateNewResource()
{
    std::lock_guard<std::mutex> lock(createHandleMutex_);

    uint32_t scaleNum = 3;
    uint32_t actualAddNum = 0;
    for (uint32_t i = 0; i < scaleNum; i++) {
        CURL *curl = curl_easy_init();
        if (curl) {
            curlHandleManager_.Release(curl);
            actualAddNum++;
        } else {
            LOG(ERROR) << FormatString("Failed to add curl resource, need %u, already add %u.", scaleNum, actualAddNum);
            break;
        }
    }
    createdSize_ += actualAddNum;
    LOG(INFO) << "The num of curl handle has already create is: " << createdSize_;
}

void CurlHandlePool::Release(CURL *curl, bool cleanUp)
{
    if (curl == nullptr) {
        return;
    }
    curl_easy_reset(curl);
    if (cleanUp) {
        CURL *newCurl = curl_easy_init();
        if (newCurl) {
            curl_easy_cleanup(curl);
            curl = newCurl;
            LOG(INFO) << "Clean up the old curl resource.";
        }
    }
    curlHandleManager_.Release(curl);
}

std::streampos CalcSize(std::shared_ptr<std::iostream> &reqBody)
{
    if (reqBody == nullptr) {
        return static_cast<std::streampos>(0);
    }

    auto currentPos = reqBody->tellg();
    if (currentPos == static_cast<std::streampos>(-1)) {
        currentPos = 0;
        reqBody->clear();
    }
    reqBody->seekg(0, reqBody->end);
    auto size = reqBody->tellg();
    reqBody->seekg(currentPos, reqBody->beg);
    return size;
}

std::string GetParam(std::shared_ptr<std::iostream> &body)
{
    std::stringstream s;
    if (body != nullptr) {
        s << body->rdbuf();
    } else {
        return s.str();
    }
    nlohmann::json jsonRes;
    try {
        jsonRes = nlohmann::json::parse(s.str());
    } catch (nlohmann::json::exception &e) {
        return s.str();
    }
    std::stringstream ret;
    if (jsonRes.contains("accessToken") && jsonRes["accessToken"].contains(KEY_EXPIRES_IN)
        && jsonRes["accessToken"].at(KEY_EXPIRES_IN).is_number_integer()) {
        ret << "{" << KEY_EXPIRES_IN << ":" << jsonRes["accessToken"].at(KEY_EXPIRES_IN);
    } else {
        return s.str();
    }
    if (jsonRes.contains(KEY_TEAM_ID) && jsonRes[KEY_TEAM_ID].is_string()) {
        ret << "," << KEY_TEAM_ID << ":" << jsonRes[KEY_TEAM_ID];
    }
    if (jsonRes.contains(KEY_PRODUCTS) && jsonRes[KEY_PRODUCTS].size() != 0 && jsonRes[KEY_PRODUCTS][0].is_string()) {
        ret << "," << KEY_PRODUCTS << ":" << jsonRes[KEY_PRODUCTS][0] << "}";
    }
    return ret.str();
}

std::string CutRsp(std::string rsp, int code, bool isGet)
{
    if (rsp.length() <= LOG_HTTP_BODY_SIZE_LIMIT || !isGet || code != HttpResponse::HTTP_STATUS_CODE_OK) {
        return rsp;
    }
    return rsp.substr(0, LOG_HTTP_BODY_SIZE_LIMIT) + "...";
}

std::string CutReq(std::string req)
{
    if (req.length() <= LOG_HTTP_BODY_SIZE_LIMIT) {
        return req;
    }
    return req.substr(0, LOG_HTTP_BODY_SIZE_LIMIT) + "...";
}

CurlHttpClient::CurlHttpClient(bool verifyServer)
    : HttpClient(), curlHandlePool_(new CurlHandlePool()), verifyServer_(verifyServer)
{
    userAgent_.append("libcurl ");
    curl_version_info_data *ver = curl_version_info(CURLVERSION_NOW);
    userAgent_.append(ver->version);

    GlobalInit();
}

int CurlHttpClient::clientNums_ = 0;
std::mutex CurlHttpClient::clientNumsMutex_;
std::atomic<bool> CurlHttpClient::curlGlobalInitFlag_ = { false };

CurlHttpClient::~CurlHttpClient()
{
    GlobalCleanUp();
}

void CurlHttpClient::GlobalInit()
{
    bool expected = false;
    if (curlGlobalInitFlag_.compare_exchange_strong(expected, true)) {
        curl_global_init(CURL_GLOBAL_ALL);
    }
    std::lock_guard<std::mutex> lock(clientNumsMutex_);
    clientNums_++;
}

void CurlHttpClient::GlobalCleanUp()
{
    std::lock_guard<std::mutex> lock(clientNumsMutex_);
    clientNums_--;
    if (clientNums_ <= 0) {
        LOG(INFO) << "curl_global_cleanup invoke.";
        curl_global_cleanup();
    }
}

void CurlHttpClient::ConfigCurlDefaultOption(CURL *handle)
{
    curl_easy_setopt(handle, CURLOPT_NOSIGNAL, 1L);
    curl_easy_setopt(handle, CURLOPT_USERAGENT, userAgent_.c_str());
    if (!verifyServer_) {
        curl_easy_setopt(handle, CURLOPT_SSL_VERIFYPEER, 0L);
        curl_easy_setopt(handle, CURLOPT_SSL_VERIFYHOST, 0L);
    }
}

void CurlHttpClient::ConfigCurlMethod(CURL *handle, const std::shared_ptr<HttpRequest> &request)
{
    switch (request->GetMethod()) {
        case HttpMethod::PUT:
            curl_easy_setopt(handle, CURLOPT_UPLOAD, 1L);
            break;
        case HttpMethod::POST:
            curl_easy_setopt(handle, CURLOPT_POST, 1L);
            break;
        case HttpMethod::DELETE:
            curl_easy_setopt(handle, CURLOPT_CUSTOMREQUEST, "DELETE");
            break;
        case HttpMethod::GET:
        default:
            break;
    }

    if (request->Headers().find(CONTENT_LENGTH) == request->Headers().end()) {
        return;
    }

    curl_off_t size = std::atoll(request->GetHeader(CONTENT_LENGTH).c_str());
    if (request->GetMethod() == HttpMethod::PUT) {
        curl_easy_setopt(handle, CURLOPT_INFILESIZE_LARGE, size);
    } else if (request->GetMethod() == HttpMethod::POST) {
        curl_easy_setopt(handle, CURLOPT_POSTFIELDSIZE, size);
    }
}

size_t CurlHttpClient::RecvHeadersCallBack(char *buffer, size_t size, size_t nmemb, void *userdata)
{
    auto resp = static_cast<HttpResponse *>(userdata);

    size_t totalSize = size * nmemb;
    if (resp == nullptr) {
        return RECV_ERR;
    }
    std::string header(buffer, totalSize);
    std::size_t colonPos = header.find(':');
    if (colonPos == std::string::npos) {
        return totalSize;
    }
    std::string headerName = header.substr(0, colonPos);
    std::string headerValue;
    std::size_t endPos = header.rfind('\r');
    // the separator ': ' will skip
    constexpr std::size_t skipCharNum = 2;
    std::size_t valueStart = colonPos + skipCharNum;
    if (endPos != std::string::npos && endPos > valueStart) {
        std::size_t valueLen = endPos - valueStart;
        headerValue = header.substr(valueStart, valueLen);
    } else {
        headerValue = header.substr(colonPos + 1);
    }

    resp->AddHeader(std::move(headerName), std::move(headerValue));
    return totalSize;
}

size_t CurlHttpClient::RecvBodyCallBack(char *ptr, size_t size, size_t nmemb, void *userdata)
{
    auto resp = static_cast<HttpResponse *>(userdata);
    const size_t writeSize = size * nmemb;
    if (writeSize == 0 || resp == nullptr) {
        LOG(ERROR) << "recv error!";
        return RECV_ERR;
    }

    std::shared_ptr<std::iostream> body = resp->GetBody();
    if (body == nullptr || body->fail()) {
        LOG(ERROR) << "recv error, body null or failed.";
        return RECV_ERR;
    }

    body->write(ptr, static_cast<std::streamsize>(writeSize));
    if (body->bad()) {
        LOG(ERROR) << "recv error, body write failed.";
        return RECV_ERR;
    }
    return writeSize;
}

size_t CurlHttpClient::SendBodyCallBack(char *buffer, size_t size, size_t nmemb, void *userdata)
{
    auto req = static_cast<HttpRequest *>(userdata);
    if (req == nullptr) {
        LOG(WARNING) << "req is null.";
        return 0;
    }
    if (size == 0) {
        LOG(WARNING) << "invalid read size 0";
        return 0;
    }
    if (nmemb > SSIZE_MAX / size) {
        LOG(WARNING) << "the total read bytes " << size << " * " << nmemb << " exceed SSIZE_MAX";
        return 0;
    }
    std::shared_ptr<std::iostream> &body = req->GetBody();
    const ssize_t needSize = static_cast<ssize_t>(size * nmemb);
    size_t readSize = 0;
    if (body != nullptr && needSize > 0) {
        body->read(buffer, needSize);
        readSize = static_cast<size_t>(body->gcount());
    }
    return readSize;
}

void AddContentLengthHeader(const std::shared_ptr<HttpRequest> &request)
{
    std::shared_ptr<std::iostream> &reqBody = request->GetBody();
    std::streampos size = CalcSize(reqBody);
    request->AddHeader(CONTENT_LENGTH, std::to_string(size));
}

int CurlHttpClient::SeekBodyCallBack(void *userdata, curl_off_t offset, int origin)
{
    LOG(WARNING) << "SeekBodyCallBack, offset:" << offset << ", origin:" << origin;
    auto req = static_cast<HttpRequest *>(userdata);
    if (req == nullptr) {
        LOG(ERROR) << "req is null.";
        return CURL_SEEKFUNC_FAIL;
    }

    if (origin != SEEK_SET) {
        LOG(ERROR) << "invalid seek, origin:" << origin;
        return CURL_SEEKFUNC_CANTSEEK;
    }
    std::shared_ptr<std::iostream> &body = req->GetBody();
    if (body == nullptr) {
        LOG(ERROR) << "body is null.";
        return CURL_SEEKFUNC_FAIL;
    }
    body->clear();
    body->seekg(offset, body->beg);
    return CURL_SEEKFUNC_OK;
}

void CurlHttpClient::SetCallBackFunc(CURL *handle, const std::shared_ptr<HttpRequest> &request,
                                     std::shared_ptr<HttpResponse> &response)
{
    curl_easy_setopt(handle, CURLOPT_HEADERDATA, response.get());
    curl_easy_setopt(handle, CURLOPT_HEADERFUNCTION, RecvHeadersCallBack);

    curl_easy_setopt(handle, CURLOPT_WRITEDATA, response.get());
    curl_easy_setopt(handle, CURLOPT_WRITEFUNCTION, RecvBodyCallBack);

    curl_easy_setopt(handle, CURLOPT_READDATA, request.get());
    curl_easy_setopt(handle, CURLOPT_READFUNCTION, SendBodyCallBack);

    curl_easy_setopt(handle, CURLOPT_SEEKDATA, request.get());
    curl_easy_setopt(handle, CURLOPT_SEEKFUNCTION, SeekBodyCallBack);
}

void CurlHttpClient::HandleHeader(const std::shared_ptr<HttpRequest> &request, curl_slist *&list)
{
    for (const auto &header : request->Headers()) {
        if (header.second.empty()) {
            continue;
        }
        std::string headerStr = header.first;
        headerStr.append(":").append(header.second);
        list = curl_slist_append(list, headerStr.c_str());
    }
}

CURLcode SslctxFunction(CURL *curl, void *sslctx, void *parm)
{
    (void)curl;
    if (sslctx == nullptr) {
        LOG(ERROR) << "sslctx is nullptr, tls failed, send http request failed";
        return CURLE_ABORTED_BY_CALLBACK;
    }
    if (parm == nullptr) {
        LOG(ERROR) << "parm is nullptr, cant load ca, cert, key for tls, send http request failed";
        return CURLE_ABORTED_BY_CALLBACK;
    }
    TslParam *param = static_cast<TslParam *>(parm);
    if (!LoadCaFromMemory(static_cast<SSL_CTX *>(sslctx), param->ca_.GetData(), param->ca_.GetSize())
        || !LoadKeyAndCertFromMemory(static_cast<SSL_CTX *>(sslctx), param->cert_.GetData(), param->cert_.GetSize(),
                                     param->key_.GetData(), param->key_.GetSize())) {
        LOG(ERROR) << "LoadKeyAndCertFromMemory or LoadKeyAndCertFromMemory failed, send http request failed";
        return CURLE_ABORTED_BY_CALLBACK;
    }
    return CURLE_OK;
}

Status CurlHttpClient::TslSend(const std::shared_ptr<HttpRequest> &request, std::shared_ptr<HttpResponse> &response,
                               const TslParam &param)
{
    CURL *curlHandle = curlHandlePool_->Acquire();
    curl_easy_setopt(curlHandle, CURLOPT_CAINFO, NULL);
    curl_easy_setopt(curlHandle, CURLOPT_CAPATH, NULL);
    curl_easy_setopt(curlHandle, CURLOPT_SSL_CTX_DATA, &param);
    curl_easy_setopt(curlHandle, CURLOPT_SSL_CTX_FUNCTION, *SslctxFunction);
    return SendRequest(request, response, curlHandle);
}

Status CurlHttpClient::SendRequest(const std::shared_ptr<HttpRequest> &request, std::shared_ptr<HttpResponse> &response,
                                   CURL *curlHandle)
{
    bool cleanUp = false;
    Raii curlResourceGiveBack([&]() { curlHandlePool_->Release(curlHandle, cleanUp); });
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(curlHandle != nullptr, K_RUNTIME_ERROR,
                                         "The curlHandle pool has been shutdown, send request failed");
    curl_slist *list = nullptr;
    request->BuildRequest();
    Raii raii([&request] { request->ClearSensitiveInfo(); });
    AddContentLengthHeader(request);
    HandleHeader(request, list);

    /**
     * disable the Expect header,because:
     * By default libcurl auto add Header: "Expect: 100-continue" to have a server check the request's headers,
     * actually, we can send request directly.
     */
    list = curl_slist_append(list, "Expect:");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(list != nullptr, K_RUNTIME_ERROR,
                                         "curl list point is nullptr, send http message failed");

    Raii freeCurlList([&]() { curl_slist_free_all(list); });

    ConfigCurlDefaultOption(curlHandle);
    curl_easy_setopt(curlHandle, CURLOPT_TIMEOUT_MS, request->GetRequestTimeoutMs());
    curl_easy_setopt(curlHandle, CURLOPT_CONNECTTIMEOUT_MS, request->GetConnectTimeoutMs());
    ConfigCurlMethod(curlHandle, request);

    std::string url = request->GetUrl();
    curl_easy_setopt(curlHandle, CURLOPT_URL, url.c_str());

    curl_easy_setopt(curlHandle, CURLOPT_HTTPHEADER, list);

    SetCallBackFunc(curlHandle, request, response);

    char errBuf[CURL_ERROR_SIZE] = { 0 };
    curl_easy_setopt(curlHandle, CURLOPT_ERRORBUFFER, errBuf);
    PerfPoint point(PerfKey::CURL_EASY_PERFORM_HTTP);
    CURLcode res = curl_easy_perform(curlHandle);
    Status rc;
    if (res != CURLcode::CURLE_OK) {
        rc = Status(K_RUNTIME_ERROR, __LINE__, __FILE__,
                    FormatString("Fail to send http request, error reason is:%s,"
                                 " error info is:%s",
                                 curl_easy_strerror(res), errBuf));
    } else {
        int stateCode = HttpResponse::STATUS_CLIENT_ERR;
        curl_easy_getinfo(curlHandle, CURLINFO_RESPONSE_CODE, &stateCode);
        response->SetStatus(stateCode);
    }
    point.Record();
    // use by Raii curlResourceGiveBack above.
    cleanUp = (res != CURLcode::CURLE_OK);
    (void)cleanUp;
    return rc;
}

Status CurlHttpClient::Send(const std::shared_ptr<HttpRequest> &request, std::shared_ptr<HttpResponse> &response)
{
    CURL *curlHandle = curlHandlePool_->Acquire();
    INJECT_POINT("CurlHttpClient.Send.ReleaseCurlHandle", [this, curlHandle]() {
        curlHandlePool_->Release(curlHandle, false);
        return Status::OK();
    });
    return SendRequest(request, response, curlHandle);
}

std::string CurlHttpClient::GetHttpAction(const std::shared_ptr<HttpRequest> &request)
{
    std::string url = request->GetUrl();
    HttpMethod methodName = request->GetMethod();
    return GetHttpMethodName(methodName) + " " + url;
}

std::string CurlHttpClient::GetHttpMethodName(HttpMethod method)
{
    switch (method) {
        case HttpMethod::GET:
            return "GET";
        case HttpMethod::PUT:
            return "PUT";
        case HttpMethod::POST:
            return "POST";
        case HttpMethod::DELETE:
            return "DELETE";
        default:
            return "";
    }
}
}  // namespace datasystem
