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

/** Description: Implements routed Exist orchestration for the object client. */

#include "datasystem/client/object_cache/exist_handler.h"

#include <algorithm>
#include <chrono>
#include <future>
#include <map>
#include <thread>
#include <unordered_set>
#include <utility>

#include <nlohmann/json.hpp>

#include "datasystem/common/metrics/kv_metrics.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
namespace object_cache {
namespace {

static constexpr int32_t EXIST_MAX_REDIRECT_RETRY = 1;
static constexpr int32_t EXIST_MAX_CONNECTION_PROBE_RETRY = 1;
static constexpr int32_t EXIST_MAX_REROUTE_RETRY = 1;
static constexpr int32_t EXIST_CONNECTION_PROBE_TIMEOUT_MS = 1000;
static constexpr int32_t EXIST_RETRY_BACKOFF_MS = 10;
static constexpr size_t EXIST_RAW_AND_NAMESPACED_KEY_VARIANTS = 2;
static const char *const EXIST_NAMESPACE_SEPARATOR = "$";
static const char *const EXIST_REDIRECTS_FIELD = "exist_redirects";
static const char *const EXIST_REDIRECT_ADDRESS_FIELD = "address";
static const char *const EXIST_REDIRECT_KEYS_FIELD = "keys";

struct ExistCallResult {
    Status rc;
    std::vector<bool> exists;
};

bool IsExistConnectionError(const Status &rc)
{
    return rc.GetCode() == K_RPC_UNAVAILABLE || rc.GetCode() == K_RPC_DEADLINE_EXCEEDED
           || rc.GetCode() == K_CLIENT_WORKER_DISCONNECT;
}

int32_t GetExistConnectionProbeTimeoutMs(int32_t requestTimeoutMs)
{
    return std::min(requestTimeoutMs, EXIST_CONNECTION_PROBE_TIMEOUT_MS);
}

std::string StripLeadingSpaces(const std::string &value)
{
    auto offset = value.find_first_not_of(" \t\r\n");
    if (offset == std::string::npos) {
        return "";
    }
    return value.substr(offset);
}

std::string BuildExistNamespaceKey(const std::string &tenantId, const std::string &key)
{
    if (tenantId.empty()) {
        return key;
    }
    return tenantId + EXIST_NAMESPACE_SEPARATOR + key;
}

std::unordered_map<std::string, std::string> BuildExistRedirectKeyMap(const std::vector<std::string> &requestKeys,
                                                                      const std::string &tenantId)
{
    std::unordered_map<std::string, std::string> rawKeyByRedirectKey;
    rawKeyByRedirectKey.reserve(requestKeys.size() * EXIST_RAW_AND_NAMESPACED_KEY_VARIANTS);
    for (const auto &key : requestKeys) {
        rawKeyByRedirectKey.emplace(key, key);
        if (!tenantId.empty()) {
            rawKeyByRedirectKey.emplace(BuildExistNamespaceKey(tenantId, key), key);
        }
    }
    return rawKeyByRedirectKey;
}

std::unordered_map<std::string, std::vector<size_t>> BuildExistKeyIndexes(const std::vector<std::string> &keys)
{
    std::unordered_map<std::string, std::vector<size_t>> keyIndexes;
    for (size_t index = 0; index < keys.size(); ++index) {
        keyIndexes[keys[index]].emplace_back(index);
    }
    return keyIndexes;
}

Status FillExistResults(const std::vector<std::string> &keys, const std::vector<bool> &batchExists,
                        const std::unordered_map<std::string, std::vector<size_t>> &keyIndexes,
                        std::vector<bool> &exists)
{
    CHECK_FAIL_RETURN_STATUS(batchExists.size() == keys.size(), K_RUNTIME_ERROR,
                             FormatString("Exist result size %zu does not match request size %zu.", batchExists.size(),
                                          keys.size()));
    for (size_t keyIndex = 0; keyIndex < keys.size(); ++keyIndex) {
        auto iter = keyIndexes.find(keys[keyIndex]);
        CHECK_FAIL_RETURN_STATUS(iter != keyIndexes.end(), K_RUNTIME_ERROR, "Exist result key index missing.");
        for (auto resultIndex : iter->second) {
            exists[resultIndex] = batchExists[keyIndex];
        }
    }
    return Status::OK();
}

ExistCallResult DoExistTransportCall(const std::shared_ptr<IExistTransport> &transport, const HostPort &worker,
                                     const std::vector<std::string> &keys, const ExistHandlerRequest &request,
                                     int32_t requestTimeoutMs)
{
    client::TransportExistRequest rpcRequest(keys, request.queryL2Cache, request.isLocal, requestTimeoutMs,
                                             request.clientId, request.tenantId, request.token);
    client::TransportExistResult result;
    Status rc = transport->Exist(worker, rpcRequest, result);
    if (rc.IsError()) {
        return { rc, {} };
    }
    return { Status::OK(), std::move(result.exists) };
}

ExistCallResult DoExistTransportCallWithConnectionRetry(const std::shared_ptr<IExistTransport> &transport,
                                                        const HostPort &worker, const std::vector<std::string> &keys,
                                                        const ExistHandlerRequest &request)
{
    ExistCallResult result = DoExistTransportCall(transport, worker, keys, request, request.requestTimeoutMs);
    if (!IsExistConnectionError(result.rc) || request.requestTimeoutMs < EXIST_CONNECTION_PROBE_TIMEOUT_MS) {
        return result;
    }
    for (int32_t connectionRetries = 0; connectionRetries < EXIST_MAX_CONNECTION_PROBE_RETRY; ++connectionRetries) {
        result = DoExistTransportCall(transport, worker, keys, request,
            GetExistConnectionProbeTimeoutMs(request.requestTimeoutMs));
        if (!IsExistConnectionError(result.rc)) {
            return result;
        }
        METRIC_INC(metrics::KvMetricId::CLIENT_EXIST_CONNECTION_RETRY_TOTAL);
        std::this_thread::sleep_for(std::chrono::milliseconds(EXIST_RETRY_BACKOFF_MS));
    }
    return result;
}

Status RunExistRedirectGroups(const std::shared_ptr<IExistTransport> &transport,
                              const std::vector<ExistRedirectGroup> &redirectGroups,
                              const ExistHandlerRequest &request, const std::shared_ptr<ThreadPool> &taskPool,
                              const std::unordered_map<std::string, std::vector<size_t>> &keyIndexes,
                              std::vector<bool> &exists)
{
    std::vector<std::future<ExistCallResult>> futures;
    futures.reserve(redirectGroups.size());
    RETURN_RUNTIME_ERROR_IF_NULL(taskPool);
    for (const auto &group : redirectGroups) {
        try {
            futures.emplace_back(taskPool->Submit([transport, worker = group.worker, keys = group.keys, &request]() {
                return DoExistTransportCallWithConnectionRetry(transport, worker, keys, request);
            }));
        } catch (const std::exception &e) {
            for (auto &f : futures) {
                if (f.valid()) {
                    (void)f.wait();
                }
            }
            return Status(K_RUNTIME_ERROR, FormatString("Submit Exist redirect task failed: %s", e.what()));
        }
    }
    for (size_t index = 0; index < futures.size(); ++index) {
        ExistCallResult result = futures[index].get();
        RETURN_IF_NOT_OK(result.rc);
        RETURN_IF_NOT_OK(FillExistResults(redirectGroups[index].keys, result.exists, keyIndexes, exists));
    }
    return Status::OK();
}

}  // namespace

Status ExistRedirectResolver::ResolveStructuredRedirect(const Status &rc, const std::vector<std::string> &requestKeys,
                                                        const std::string &tenantId,
                                                        ExistRedirectResolution &resolution) const
{
    resolution = {};
    return ParseRedirectJson(StripLeadingSpaces(rc.GetExtra()), requestKeys, tenantId, resolution);
}

Status ExistRedirectResolver::ParseRedirectJson(const std::string &extra, const std::vector<std::string> &requestKeys,
                                                const std::string &tenantId,
                                                ExistRedirectResolution &resolution) const
{
    if (extra.empty() || extra.front() != '{') {
        return Status::OK();
    }
    auto json = nlohmann::json::parse(extra, nullptr, false);
    CHECK_FAIL_RETURN_STATUS(!json.is_discarded(), K_INVALID, "Parse Exist structured redirect extra failed.");
    if (!json.is_object() || !json.contains(EXIST_REDIRECTS_FIELD)) {
        return Status::OK();
    }
    resolution.parsedStructuredRedirect = true;
    const auto &redirects = json[EXIST_REDIRECTS_FIELD];
    CHECK_FAIL_RETURN_STATUS(redirects.is_array(), K_INVALID, "Exist redirect field is not an array.");
    return BuildRedirectGroups(requestKeys, tenantId, redirects.dump(), resolution);
}

Status ExistRedirectResolver::BuildRedirectGroups(const std::vector<std::string> &requestKeys,
                                                  const std::string &tenantId, const std::string &redirectsJson,
                                                  ExistRedirectResolution &resolution) const
{
    auto redirects = nlohmann::json::parse(redirectsJson, nullptr, false);
    CHECK_FAIL_RETURN_STATUS(!redirects.is_discarded(), K_INVALID, "Parse Exist redirect JSON failed.");
    auto rawKeyByRedirectKey = BuildExistRedirectKeyMap(requestKeys, tenantId);
    std::unordered_map<std::string, HostPort> redirectByKey;
    redirectByKey.reserve(requestKeys.size());
    std::unordered_set<std::string> coveredKeys;
    coveredKeys.reserve(requestKeys.size());
    HostPort fallbackWorker;
    bool hasFallbackWorker = false;
    for (const auto &redirect : redirects) {
        CHECK_FAIL_RETURN_STATUS(redirect.is_object(), K_INVALID, "Exist redirect item is not an object.");
        CHECK_FAIL_RETURN_STATUS(redirect.contains(EXIST_REDIRECT_ADDRESS_FIELD), K_INVALID,
                                 "Exist redirect address is missing.");
        CHECK_FAIL_RETURN_STATUS(redirect.contains(EXIST_REDIRECT_KEYS_FIELD), K_INVALID,
                                 "Exist redirect keys are missing.");
        CHECK_FAIL_RETURN_STATUS(redirect[EXIST_REDIRECT_ADDRESS_FIELD].is_string(), K_INVALID,
                                 "Exist redirect address is not a string.");
        CHECK_FAIL_RETURN_STATUS(redirect[EXIST_REDIRECT_KEYS_FIELD].is_array(), K_INVALID,
                                 "Exist redirect keys are not an array.");
        HostPort worker;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker.ParseString(redirect[EXIST_REDIRECT_ADDRESS_FIELD].get<std::string>()),
                                         "Parse Exist redirect worker address failed");
        if (!hasFallbackWorker) {
            fallbackWorker = worker;
            hasFallbackWorker = true;
        }
        for (const auto &key : redirect[EXIST_REDIRECT_KEYS_FIELD]) {
            CHECK_FAIL_RETURN_STATUS(key.is_string(), K_INVALID, "Exist redirect key is not a string.");
            auto rawKeyIt = rawKeyByRedirectKey.find(key.get<std::string>());
            if (rawKeyIt == rawKeyByRedirectKey.end()) {
                continue;
            }
            redirectByKey[rawKeyIt->second] = worker;
            coveredKeys.emplace(rawKeyIt->second);
        }
    }
    return AssembleGroups(requestKeys, redirectByKey, coveredKeys, hasFallbackWorker, fallbackWorker, resolution);
}

Status ExistRedirectResolver::AssembleGroups(const std::vector<std::string> &requestKeys,
    const std::unordered_map<std::string, HostPort> &redirectByKey,
    const std::unordered_set<std::string> &coveredKeys, bool hasFallbackWorker,
    const HostPort &fallbackWorker, ExistRedirectResolution &resolution) const
{
    std::map<std::string, ExistRedirectGroup> orderedGroups;
    for (const auto &key : requestKeys) {
        auto it = redirectByKey.find(key);
        if (it == redirectByKey.end()) {
            continue;
        }
        auto workerText = it->second.ToString();
        auto &group = orderedGroups[workerText];
        group.worker = it->second;
        group.keys.emplace_back(key);
    }
    if (orderedGroups.empty()) {
        CHECK_FAIL_RETURN_STATUS(hasFallbackWorker, K_INVALID, "Exist structured redirect has no matched keys.");
        resolution.redirectGroups.emplace_back(ExistRedirectGroup{ fallbackWorker, requestKeys });
        return Status::OK();
    }
    resolution.redirectGroups.reserve(orderedGroups.size());
    for (auto &item : orderedGroups) {
        resolution.redirectGroups.emplace_back(std::move(item.second));
    }
    for (const auto &key : requestKeys) {
        if (coveredKeys.find(key) == coveredKeys.end()) {
            resolution.missingKeys.emplace_back(key);
        }
    }
    return Status::OK();
}

Status ExistRedirectResolver::ParseAddressOnlyRedirect(const Status &rc, HostPort &worker) const
{
    std::string address = rc.GetExtra();
    auto delimiter = address.find(',');
    if (delimiter != std::string::npos) {
        address = address.substr(0, delimiter);
    }
    CHECK_FAIL_RETURN_STATUS(!address.empty(), K_RUNTIME_ERROR, "Exist redirect address is empty.");
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(worker.ParseString(address), "Parse Exist redirect worker address failed");
    return Status::OK();
}

ExistHandler::ExistHandler(std::shared_ptr<IExistRouting> routing, std::shared_ptr<IExistTransport> transport,
                           std::shared_ptr<ThreadPool> taskPool)
    : routing_(std::move(routing)), transport_(std::move(transport)), taskPool_(std::move(taskPool))
{
}

Status ExistHandler::Run(const ExistHandlerRequest &request, std::vector<bool> &exists)
{
    RETURN_RUNTIME_ERROR_IF_NULL(routing_);
    RETURN_RUNTIME_ERROR_IF_NULL(transport_);
    auto keyIndexes = BuildExistKeyIndexes(request.keys);
    exists.assign(request.keys.size(), false);
    Status rc;
    for (int32_t rerouteRetries = 0; rerouteRetries <= EXIST_MAX_REROUTE_RETRY; ++rerouteRetries) {
        rc = RunSelectedWorkers(request, keyIndexes, exists);
        if (rc.IsOk() || !IsExistConnectionError(rc) || rerouteRetries >= EXIST_MAX_REROUTE_RETRY) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(EXIST_RETRY_BACKOFF_MS));
    }
    return rc;
}

Status ExistHandler::RunSelectedWorkers(const ExistHandlerRequest &request,
                                        const std::unordered_map<std::string, std::vector<size_t>> &keyIndexes,
                                        std::vector<bool> &exists)
{
    std::unordered_map<HostPort, std::vector<std::string>> groups;
    RETURN_IF_NOT_OK(routing_->SelectWorkers(request.keys, client::SelectStrategy::HASH_RING_AFFINITY, groups));
    std::vector<std::pair<HostPort, std::vector<std::string>>> orderedGroups(groups.begin(), groups.end());
    std::sort(orderedGroups.begin(), orderedGroups.end(), [](const auto &lhs, const auto &rhs) {
        return lhs.first.ToString() < rhs.first.ToString();
    });

    for (const auto &group : orderedGroups) {
        HostPort worker = group.first;
        int32_t redirectRetries = 0;
        while (true) {
            ExistCallResult result =
                DoExistTransportCallWithConnectionRetry(transport_, worker, group.second, request);
            Status rc = result.rc;
            if (rc.IsOk()) {
                RETURN_IF_NOT_OK(FillExistResults(group.second, result.exists, keyIndexes, exists));
                break;
            }
            bool redirected = false;
            if (rc.GetCode() == K_NOT_OWNER && !rc.GetExtra().empty()
                && redirectRetries < EXIST_MAX_REDIRECT_RETRY) {
                RETURN_IF_NOT_OK(HandleRedirect(rc, worker, group.second, request, keyIndexes, exists,
                                                redirectRetries, redirected));
                if (redirected) {
                    break;
                }
                continue;
            }
            if (IsExistConnectionError(rc)) {
                routing_->UpdateState(worker, K_CLIENT_WORKER_DISCONNECT);
            }
            return rc;
        }
    }
    return Status::OK();
}

Status ExistHandler::HandleRedirect(const Status &rc, HostPort &worker, const std::vector<std::string> &workerKeys,
                                    const ExistHandlerRequest &request,
                                    const std::unordered_map<std::string, std::vector<size_t>> &keyIndexes,
                                    std::vector<bool> &exists, int32_t &redirectRetries, bool &redirected)
{
    redirected = false;
    ++redirectRetries;
    METRIC_INC(metrics::KvMetricId::CLIENT_EXIST_REDIRECT_TOTAL);
    ExistRedirectResolution resolution;
    RETURN_IF_NOT_OK(redirectResolver_.ResolveStructuredRedirect(rc, workerKeys, request.tenantId, resolution));
    if (!resolution.parsedStructuredRedirect) {
        RETURN_IF_NOT_OK(redirectResolver_.ParseAddressOnlyRedirect(rc, worker));
        return Status::OK();
    }
    if (!resolution.missingKeys.empty()) {
        resolution.redirectGroups.emplace_back(ExistRedirectGroup{ worker, std::move(resolution.missingKeys) });
    }
    RETURN_IF_NOT_OK(RunExistRedirectGroups(transport_, resolution.redirectGroups, request, taskPool_, keyIndexes,
        exists));
    redirected = true;
    return Status::OK();
}

}  // namespace object_cache
}  // namespace datasystem
