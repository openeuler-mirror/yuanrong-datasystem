/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Module responsible for managing the redirect metadata logic on the master.
 */
#ifndef DATASYSTEM_MASTER_METADATA_REDIRECT_HELPER_H
#define DATASYSTEM_MASTER_METADATA_REDIRECT_HELPER_H

#include <algorithm>
#include <cstdint>
#include <iomanip>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include <google/protobuf/reflection.h>
#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/utils.pb.h"
#include "datasystem/worker/worker_topology_references.h"

DS_DECLARE_bool(enable_redirect);

namespace datasystem {
namespace master {
using TbbMigratingTable = tbb::concurrent_hash_map<ImmutableString, bool>;

class MetadataRedirectHelper {
public:
    MetadataRedirectHelper(worker::WorkerTopologyReferences *cm) : topologyEngine_(cm)
    {
    }

    virtual ~MetadataRedirectHelper();

    /**
     * @brief Check whether the item (object or stream) is being migrated.
     * @param[in] id The item identifier to be migrated.
     * @return Returns true if migration is in progress.
     */
    virtual bool ItemIsMigrating(const std::string &id);

    /**
     * @brief Shutdown the metadata redirect helper module.
     */
    virtual void Shutdown();

    template <typename Api, typename Req>
    using ConvertRedirectInfo2NewReqFunc =
        std::function<Status(const RedirectMetaInfo &redirectMetaInfos, Req &req, std::shared_ptr<Api> &api)>;

    template <typename Api, typename Req, typename Rsp>
    using SendReqAndHandleRspExceptRedirctInfoFunc =
        std::function<Status(std::shared_ptr<Api> &&api, Req &&req, Rsp &rsp)>;

    template <typename Api, typename Req, typename Rsp>
    static Status RetryForRedirict(
        std::shared_ptr<Api> &&api, Req &&req, Rsp &rsp, const std::string &redirictInfoName,
        const SendReqAndHandleRspExceptRedirctInfoFunc<Api, Req, Rsp> &sendReqAndHandleRspExceptRedirctInfoFunc,
        const ConvertRedirectInfo2NewReqFunc<Api, Req> &convertRedirectInfo2NewReqFunc)
    {
        RedirectRetryState state;
        return RetryForRedirectInternal(std::move(api), std::move(req), rsp, redirictInfoName,
                                        sendReqAndHandleRspExceptRedirctInfoFunc,
                                        convertRedirectInfo2NewReqFunc, state);
    }

    /**
     * @brief Clean migrating items.
     * @param[in] keys The keys to be clean.
     */
    void CleanMigratingItems(const std::vector<std::string> &keys)
    {
        for (const auto &key : keys) {
            migratingItems_.erase(key);
        }
    }

private:
    static constexpr size_t MAX_REDIRECT_HOPS = 16;

    struct RedirectRetryState {
        size_t hops{ 0 };
        uint64_t latestTopologyVersion{ 0 };
        std::unordered_set<std::string> visitedTargets;
    };

    static Status RecordRedirectStep(const RedirectMetaInfo &info, RedirectRetryState &state)
    {
        CHECK_FAIL_RETURN_STATUS(!info.redirect_meta_address().empty(), K_INVALID,
                                 "metadata redirect target is empty");
        CHECK_FAIL_RETURN_STATUS(state.hops < MAX_REDIRECT_HOPS, K_TRY_AGAIN,
                                 "metadata redirect hop budget exhausted");
        const uint64_t version = info.topology_version();
        CHECK_FAIL_RETURN_STATUS(state.latestTopologyVersion == 0 || version >= state.latestTopologyVersion,
                                 K_TRY_AGAIN, "metadata redirect topology version rolled back");
        const std::string targetKey = std::to_string(version) + "|" + info.redirect_meta_address();
        CHECK_FAIL_RETURN_STATUS(state.visitedTargets.emplace(targetKey).second, K_TRY_AGAIN,
                                 "metadata redirect loop detected");
        state.latestTopologyVersion = std::max(state.latestTopologyVersion, version);
        ++state.hops;
        return Status::OK();
    }

    template <typename Api, typename Req, typename Rsp>
    static Status RetryForRedirectInternal(
        std::shared_ptr<Api> &&api, Req &&req, Rsp &rsp, const std::string &redirectInfoName,
        const SendReqAndHandleRspExceptRedirctInfoFunc<Api, Req, Rsp> &sendRequest,
        const ConvertRedirectInfo2NewReqFunc<Api, Req> &convertRedirect, RedirectRetryState &state)
    {
        do {
            rsp.Clear();
            RETURN_IF_NOT_OK(sendRequest(std::move(api), std::move(req), rsp));
        } while (rsp.meta_is_moving());
        auto field = rsp.GetDescriptor()->FindFieldByName(redirectInfoName);
        if (field == nullptr) {
            return Status::OK();
        }
        auto infos = rsp.GetReflection()->template GetRepeatedFieldRef<RedirectMetaInfo>(rsp, field);
        for (const auto &info : infos) {
            RETURN_IF_NOT_OK(RecordRedirectStep(info, state));
            std::shared_ptr<Api> redirectApi;
            Req redirectReq;
            RETURN_IF_NOT_OK(convertRedirect(info, redirectReq, redirectApi));
            return RetryForRedirectInternal(std::move(redirectApi), std::move(redirectReq), rsp,
                                            redirectInfoName, sendRequest, convertRedirect, state);
        }
        return Status::OK();
    }

protected:

    /**
     * @brief Check if the metadata is available for given id.
     * @param[in] id The item identifier.
     * @return Returns true if meta is found in meta table.
     */
    virtual bool MetaIsFound(const std::string &id) = 0;

    /**
     * @brief Check if item (object or stream) need to redirect or not.
     * @param[in] id The id of the item to check.
     * @param[out] redirect Redirect or not.
     * @param[out] newAddr If need to redirect, the new meta address of the item.
     * @param[out] topologyVersion Topology version that authorized the redirect.
     */
    virtual void CheckNeedToRedirectOrNot(const std::string &id, bool &redirect, std::string &newAddr,
                                          uint64_t &topologyVersion);

    /**
     * @brief If need to redirect, group item ids by new meta address.
     * @param[in] ids The item ids to redirect.
     * @param[out] redirectMap Redirect ids group by new meta address.
     */
    virtual void GroupRedirctObjectByNewMetaAddr(
        std::vector<std::string> &ids,
        std::map<std::pair<uint64_t, std::string>, std::vector<std::string>> &redirectMap);

    /**
     * @brief If need redirect, fill redirect info.
     * @tparam Rsp Redirect response.
     * @param[out] response Response info.
     * @param[in] id The id of the item to redirect.
     * @param[out] redirect Need redirect or not.
     */
    template <typename Rsp>
    void FillRedirectResponseInfo(Rsp &response, const std::string &id, bool &redirect)
    {
        std::string newMetaAddr;
        if (!redirect || !FLAGS_enable_redirect) {
            redirect = false;
            return;
        }
        uint64_t topologyVersion = 0;
        CheckNeedToRedirectOrNot(id, redirect, newMetaAddr, topologyVersion);
        INJECT_POINT("redirect.create.update.copy.meta", [&newMetaAddr, &redirect](std::string addr) {
            newMetaAddr = addr;
            redirect = true;
            return;
        });
        if (redirect) {
            RedirectMetaInfo *info = response.mutable_info();
            info->set_redirect_meta_address(newMetaAddr);
            info->set_topology_version(topologyVersion);
            info->add_change_meta_ids(id);
            bool isMoving = ItemIsMigrating(id);
            INJECT_POINT("meta.moving", [&isMoving]() {
                isMoving = true;
                return;
            });
            response.set_meta_is_moving(isMoving);
            return;
        }
    }

    /**
     * @brief If need redirect, fill redirect infos.
     * @tparam Rsp Redirect response.
     * @param[out] rsp Response infos.
     * @param[in,out] ids The ids of the items to process in this node.
     * @param[in] redirect Need redirect or not.
     */
    template <typename Rsp>
    void FillRedirectResponseInfos(Rsp &rsp, std::vector<std::string> &ids, bool redirect)
    {
        if (!redirect || !FLAGS_enable_redirect) {
            redirect = false;
            return;
        }
        std::map<std::pair<uint64_t, std::string>, std::vector<std::string>> redirectQueryObjKeys;
        GroupRedirctObjectByNewMetaAddr(ids, redirectQueryObjKeys);
        bool isMoving = false;
        for (const auto &redirectInfo : redirectQueryObjKeys) {
            RedirectMetaInfo *info = rsp.add_info();
            info->set_topology_version(redirectInfo.first.first);
            info->set_redirect_meta_address(redirectInfo.first.second);
            for (auto &id : redirectInfo.second) {
                VLOG(1) << FormatString("redirect id : %s, redirect meta address: %s", id,
                                        redirectInfo.first.second);
                bool tempIsMoving = ItemIsMigrating(id);
                INJECT_POINT("metas.moving", [&tempIsMoving]() {
                    tempIsMoving = true;
                    return;
                });
                if (tempIsMoving) {
                    isMoving = true;
                    rsp.set_meta_is_moving(isMoving);
                    return;
                }
                info->add_change_meta_ids(id);
            }
        }
    }

    TbbMigratingTable migratingItems_;
    worker::WorkerTopologyReferences *topologyEngine_ = nullptr;
};
}  // namespace master
}  // namespace datasystem
#endif  // DATASYSTEM_MASTER_METADATA_REDIRECT_HELPER_H
