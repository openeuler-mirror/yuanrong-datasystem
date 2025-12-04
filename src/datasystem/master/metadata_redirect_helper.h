/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Module responsible for managing the redirect metadata logic on the master.
 */
#ifndef DATASYSTEM_MASTER_METADATA_REDIRECT_HELPER_H
#define DATASYSTEM_MASTER_METADATA_REDIRECT_HELPER_H

#include <cstdint>
#include <iomanip>
#include <list>
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
#include "datasystem/worker/cluster_manager/etcd_cluster_manager.h"

DS_DECLARE_bool(enable_redirect);

namespace datasystem {
namespace master {
using TbbMigratingTable = tbb::concurrent_hash_map<ImmutableString, bool>;

class MetadataRedirectHelper {
public:
    MetadataRedirectHelper(EtcdClusterManager *cm) : etcdCM_(cm)
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
        do {
            rsp.Clear();
            RETURN_IF_NOT_OK(sendReqAndHandleRspExceptRedirctInfoFunc(std::move(api), std::move(req), rsp));
        } while (rsp.meta_is_moving());

        auto descriptor = rsp.GetDescriptor();
        auto reflection = rsp.GetReflection();
        auto fieldPointer = descriptor->FindFieldByName(redirictInfoName);
        if (fieldPointer == nullptr) {
            return Status::OK();
        }
        auto redirectMetaInfos = reflection->template GetRepeatedFieldRef<RedirectMetaInfo>(rsp, fieldPointer);
        for (const auto &redirectMetaInfo : redirectMetaInfos) {
            std::shared_ptr<Api> redirctApi;
            Req redirctReq;
            RETURN_IF_NOT_OK(convertRedirectInfo2NewReqFunc(redirectMetaInfo, redirctReq, redirctApi));
            return RetryForRedirict(std::move(redirctApi), std::move(redirctReq), rsp, redirictInfoName,
                                    sendReqAndHandleRspExceptRedirctInfoFunc, convertRedirectInfo2NewReqFunc);
        }
        return Status::OK();
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
     */
    virtual void CheckNeedToRedirectOrNot(const std::string &id, bool &redirect, std::string &newAddr);

    /**
     * @brief If need to redirect, group item ids by new meta address.
     * @param[in] ids The item ids to redirect.
     * @param[out] redirectMap Redirect ids group by new meta address.
     */
    virtual void GroupRedirctObjectByNewMetaAddr(
        std::vector<std::string> &ids, std::unordered_map<std::string, std::vector<std::string>> &redirectMap);

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
            VLOG(1) << "receive redirect object: " << id;
            redirect = false;
            return;
        }
        CheckNeedToRedirectOrNot(id, redirect, newMetaAddr);
        INJECT_POINT("redirect.create.update.copy.meta", [&newMetaAddr, &redirect](std::string addr) {
            newMetaAddr = addr;
            redirect = true;
            return;
        });
        if (redirect) {
            RedirectMetaInfo *info = response.mutable_info();
            info->set_redirect_meta_address(newMetaAddr);
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
     * @param rsp[out] Response infos.
     * @param ids[in/out] The ids of the items to process in this node.
     * @param isMoving[out] If meta is moving.
     * @param redirect[in] Need redirect or not.
     */
    template <typename Rsp>
    void FillRedirectResponseInfos(Rsp &rsp, std::vector<std::string> &ids, bool redirect)
    {
        if (!redirect || !FLAGS_enable_redirect) {
            redirect = false;
            return;
        }
        std::unordered_map<std::string, std::vector<std::string>> redirectQueryObjKeys;
        GroupRedirctObjectByNewMetaAddr(ids, redirectQueryObjKeys);
        bool isMoving = false;
        for (const auto &redirectInfo : redirectQueryObjKeys) {
            RedirectMetaInfo *info = rsp.add_info();
            info->set_redirect_meta_address(redirectInfo.first);
            for (auto &id : redirectInfo.second) {
                VLOG(1) << FormatString("redirect id : %s, redirect meta address: %s", id, redirectInfo.first);
                bool tempIsMoving = ItemIsMigrating(id);
                INJECT_POINT("metas.moving", [&tempIsMoving]() {
                    tempIsMoving = true;
                    return;
                });
                if (tempIsMoving) {
                    isMoving = true;
                    rsp.set_meta_is_moving(isMoving);
                    return;
                } else {
                    info->add_change_meta_ids(id);
                }
            }
        }
    }

    TbbMigratingTable migratingItems_;
    EtcdClusterManager *etcdCM_ = nullptr;
};
}  // namespace master
}  // namespace datasystem
#endif  // DATASYSTEM_MASTER_METADATA_REDIRECT_HELPER_H
