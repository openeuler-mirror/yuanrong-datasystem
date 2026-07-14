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
#include "datasystem/master/metadata_redirect_helper.h"

namespace datasystem {
namespace master {
MetadataRedirectHelper::~MetadataRedirectHelper()
{
}

void MetadataRedirectHelper::Shutdown()
{
    topologyEngine_ = nullptr;
}

void MetadataRedirectHelper::CheckNeedToRedirectOrNot(const std::string &id, bool &needRedirect,
                                                      std::string &newMetaAddr, uint64_t &topologyVersion)
{
    bool routeRedirect = false;
    bool moving = false;
    if (topologyEngine_ != nullptr) {
        auto rc = worker::EvaluateTopologyRedirect(topologyEngine_, id, routeRedirect, moving, newMetaAddr,
                                                   &topologyVersion);
        if (rc.IsError()) {
            routeRedirect = false;
            moving = false;
            newMetaAddr.clear();
        }
    }
    if (moving) {
        needRedirect = true;
        newMetaAddr.clear();
        return;
    }

    if (MetaIsFound(id)) {
        if (ItemIsMigrating(id)) {
            INJECT_POINT("CheckNeedToRedirectOrNot.delay", []() {});
            LOG(WARNING) << FormatString("meta is moving, need redirect id %s, redirect meta address %s", id,
                                         newMetaAddr);
            needRedirect = true;
            return;
        }
        needRedirect = false;
        return;
    }
    needRedirect = routeRedirect;
    if (needRedirect) {
        LOG(WARNING) << FormatString("meta need redirect, id: %s, meta address %s", id, newMetaAddr);
    }
}

void MetadataRedirectHelper::GroupRedirctObjectByNewMetaAddr(
    std::vector<std::string> &ids,
    std::map<std::pair<uint64_t, std::string>, std::vector<std::string>> &redirectMap)
{
    for (auto id = ids.begin(); id != ids.end();) {
        bool tempRedirect = true;
        std::string newMetaAddr;
        uint64_t topologyVersion = 0;
        CheckNeedToRedirectOrNot(*id, tempRedirect, newMetaAddr, topologyVersion);
        INJECT_POINT("redirect.query.delete", [&newMetaAddr, &tempRedirect](std::string addr) {
            newMetaAddr = addr;
            tempRedirect = true;
        });
        if (tempRedirect) {
            const auto target = std::make_pair(topologyVersion, newMetaAddr);
            auto iter = redirectMap.find(target);
            if (iter == std::end(redirectMap)) {
                std::vector<std::string> idList({ *id });
                redirectMap.emplace(target, std::move(idList));
            } else {
                iter->second.push_back(*id);
            }
            id = ids.erase(id);
        } else {
            ++id;
        }
    }
}

bool MetadataRedirectHelper::ItemIsMigrating(const std::string &id)
{
    TbbMigratingTable::const_accessor accessor;
    if (migratingItems_.find(accessor, id)) {
        return true;
    }
    bool redirect = false;
    bool moving = false;
    std::string targetAddress;
    return topologyEngine_ != nullptr
           && worker::EvaluateTopologyRedirect(topologyEngine_, id, redirect, moving, targetAddress).IsOk()
           && moving;
}
}  // namespace master
}  // namespace datasystem
