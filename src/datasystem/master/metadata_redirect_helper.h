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
#include <atomic>
#include <cstdint>
#include <functional>
#include <iomanip>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <google/protobuf/reflection.h>
#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/immutable_string/immutable_string.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/cluster/routing/placement_facade.h"
#include "datasystem/protos/utils.pb.h"

DS_DECLARE_bool(enable_redirect);

namespace datasystem {
namespace master {
using TbbMigratingTable = tbb::concurrent_hash_map<ImmutableString, bool>;

/**
 * @brief One metadata redirect decision after applying the Master deployment mode.
 */
struct MetaRedirectDecision {
    bool redirect{ false };
    bool moving{ false };
    std::string targetAddress;
    uint64_t topologyVersion{ 0 };
};

class MetadataRedirectHelper {
public:
    /**
     * @brief Bind metadata deployment mode and a non-owned placement capability once.
     * @param[in] placement Required in distributed metadata mode and outlives this helper.
     * @param[in] centralizedMetadata Whether one configured metadata Master owns every key.
     * @param[in] metadataAddress Configured centralized metadata Master address.
     */
    MetadataRedirectHelper(const cluster::PlacementFacade *placement, bool centralizedMetadata,
                           HostPort metadataAddress);

    /**
     * @brief Destroy the helper; the placement capability remains non-owned.
     */
    virtual ~MetadataRedirectHelper() = default;

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
     * @return K_OK or placement availability/validation status.
     */
    virtual Status CheckNeedToRedirectOrNot(const std::string &id, bool &redirect, std::string &newAddr,
                                            uint64_t &topologyVersion);

    /**
     * @brief If need to redirect, group item ids by new meta address.
     * @param[in,out] ids Item ids retained for local processing.
     * @param[out] redirectMap Redirect ids group by new meta address.
     * @param[out] moving Whether at least one item must wait for metadata handoff.
     * @return K_OK or placement availability/validation status.
     */
    virtual Status GroupRedirctObjectByNewMetaAddr(
        std::vector<std::string> &ids,
        std::map<std::pair<uint64_t, std::string>, std::vector<std::string>> &redirectMap, bool &moving);

    /**
     * @brief Evaluate one key using the prebound Master metadata deployment mode.
     * @param[in] key Binary-safe metadata key.
     * @param[out] decision Structured redirect decision; unchanged on failure.
     * @return K_OK or placement availability/validation status.
     */
    Status EvaluateMetadataRedirect(std::string_view key, MetaRedirectDecision &decision) const;

    /**
     * @brief Evaluate multiple metadata keys against one topology Snapshot.
     * @param[in] keys Binary-safe metadata keys.
     * @param[out] decisions Structured redirect decisions; unchanged on failure.
     * @return K_OK or placement availability/validation status.
     */
    Status EvaluateMetadataRedirectBatch(const std::vector<std::string_view> &keys,
                                         std::vector<MetaRedirectDecision> &decisions) const;

    /**
     * @brief Apply local metadata availability to one topology redirect decision.
     * @param[in] id Metadata key.
     * @param[in,out] decision Topology decision refined with local migration state.
     * @return K_OK or injected test status.
     */
    Status ApplyMetadataAvailability(const std::string &id, MetaRedirectDecision &decision);

    /**
     * @brief Resolve one metadata owner using the prebound Master metadata deployment mode.
     * @param[in] key Binary-safe metadata key.
     * @param[out] owner Metadata owner; unchanged on failure.
     * @return K_OK or placement/address status.
     */
    Status ResolveMetadataOwner(std::string_view key, HostPort &owner) const;

    /**
     * @brief Return whether metadata uses one configured centralized Master.
     * @return True in centralized metadata mode.
     */
    bool IsCentralizedMetadata() const noexcept
    {
        return centralizedMetadata_;
    }

    /**
     * @brief If need redirect, fill redirect info.
     * @tparam Rsp Redirect response.
     * @param[out] response Response info.
     * @param[in] id The id of the item to redirect.
     * @param[out] redirect Need redirect or not.
     * @return K_OK or placement availability/validation status.
     */
    template <typename Rsp>
    Status FillRedirectResponseInfo(Rsp &response, const std::string &id, bool &redirect)
    {
        if (!redirect || !FLAGS_enable_redirect) {
            redirect = false;
            return Status::OK();
        }
        MetaRedirectDecision decision;
        RETURN_IF_NOT_OK(EvaluateMetadataRedirect(id, decision));
        RETURN_IF_NOT_OK(ApplyMetadataAvailability(id, decision));
        INJECT_POINT("redirect.create.update.copy.meta", [&decision](std::string addr) {
            decision.targetAddress = std::move(addr);
            decision.redirect = true;
            return Status::OK();
        });
        INJECT_POINT("meta.moving", [&decision]() {
            decision.moving = true;
            return Status::OK();
        });
        redirect = decision.redirect || decision.moving;
        if (decision.moving) {
            response.set_meta_is_moving(true);
            return Status::OK();
        }
        if (!decision.redirect) {
            return Status::OK();
        }
        RedirectMetaInfo *info = response.mutable_info();
        info->set_redirect_meta_address(decision.targetAddress);
        info->set_topology_version(decision.topologyVersion);
        info->add_change_meta_ids(id);
        return Status::OK();
    }

    /**
     * @brief If need redirect, fill redirect infos.
     * @tparam Rsp Redirect response.
     * @param[out] rsp Response infos.
     * @param[in,out] ids The ids of the items to process in this node.
     * @param[in] redirect Need redirect or not.
     * @return K_OK or placement availability/validation status.
     */
    template <typename Rsp>
    Status FillRedirectResponseInfos(Rsp &rsp, std::vector<std::string> &ids, bool redirect)
    {
        if (!redirect || !FLAGS_enable_redirect) {
            redirect = false;
            return Status::OK();
        }
        std::map<std::pair<uint64_t, std::string>, std::vector<std::string>> redirectQueryObjKeys;
        bool isMoving = false;
        RETURN_IF_NOT_OK(GroupRedirctObjectByNewMetaAddr(ids, redirectQueryObjKeys, isMoving));
        INJECT_POINT("metas.moving", [&isMoving]() {
            isMoving = true;
            return Status::OK();
        });
        if (isMoving) {
            rsp.set_meta_is_moving(true);
            return Status::OK();
        }
        for (const auto &redirectInfo : redirectQueryObjKeys) {
            RedirectMetaInfo *info = rsp.add_info();
            info->set_topology_version(redirectInfo.first.first);
            info->set_redirect_meta_address(redirectInfo.first.second);
            for (auto &id : redirectInfo.second) {
                info->add_change_meta_ids(id);
            }
        }
        return Status::OK();
    }

    TbbMigratingTable migratingItems_;
    std::atomic<const cluster::PlacementFacade *> placement_{ nullptr };
    const bool centralizedMetadata_;
    const HostPort metadataAddress_;
};
}  // namespace master
}  // namespace datasystem
#endif  // DATASYSTEM_MASTER_METADATA_REDIRECT_HELPER_H
