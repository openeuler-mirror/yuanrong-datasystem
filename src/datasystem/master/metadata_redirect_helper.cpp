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
#include "datasystem/master/metadata_redirect_helper.h"

namespace datasystem {
namespace master {
MetadataRedirectHelper::~MetadataRedirectHelper()
{
}

void MetadataRedirectHelper::Shutdown()
{
    etcdCM_ = nullptr;
}

void MetadataRedirectHelper::CheckNeedToRedirectOrNot(const std::string &id, bool &needRedirect,
                                                      std::string &newMetaAddr)
{
    HostPort masterAddr;
    needRedirect = etcdCM_->NeedRedirect(id, masterAddr);

    if (MetaIsFound(id)) {
        if (ItemIsMigrating(id)) {
            INJECT_POINT("CheckNeedToRedirectOrNot.delay", []() {});
            newMetaAddr = masterAddr.ToString();
            LOG(WARNING) << FormatString("meta is moving, need redirect id %s, redirect meta address %s", id,
                                         newMetaAddr);
            needRedirect = true;
            return;
        }
        needRedirect = false;
        return;
    }
    if (needRedirect) {
        newMetaAddr = masterAddr.ToString();
        LOG(WARNING) << FormatString("meta need redirect, id: %s, meta address %s", id, newMetaAddr);
    }
}

void MetadataRedirectHelper::GroupRedirctObjectByNewMetaAddr(
    std::vector<std::string> &ids, std::unordered_map<std::string, std::vector<std::string>> &redirectMap)
{
    for (auto id = ids.begin(); id != ids.end();) {
        bool tempRedirect = true;
        std::string newMetaAddr;
        CheckNeedToRedirectOrNot(*id, tempRedirect, newMetaAddr);
        INJECT_POINT("redirect.query.delete", [&newMetaAddr, &tempRedirect](std::string addr) {
            newMetaAddr = addr;
            tempRedirect = true;
        });
        if (tempRedirect == true) {
            auto iter = redirectMap.find(newMetaAddr);
            if (iter == std::end(redirectMap)) {
                std::vector<std::string> idList({ *id });
                redirectMap.insert(std::make_pair(newMetaAddr, std::move(idList)));
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
    auto found = migratingItems_.find(accessor, id);
    return found;
}
}  // namespace master
}  // namespace datasystem
