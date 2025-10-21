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
 * Description: Defines operations related to gflag.
 */
#include "datasystem/common/util/version.h"

#include <re2/re2.h>

namespace datasystem {
void CheckClientGitHash(const std::string &clientGitHash)
{
    std::string severGitHash = GetGitHash();
    if (severGitHash != clientGitHash) {
        std::string errorMsg = "The commit id of client does not match that of server. client commit id is: "
                               + clientGitHash + ", server commit id is: " + severGitHash;
        const size_t HASHCODE_MAXSIZE = 73;
        if (!MatchGitHash(clientGitHash) || clientGitHash.size() > HASHCODE_MAXSIZE) {
            errorMsg += " ; Get invalid git hash!";
        }
        LOG(WARNING) << errorMsg;
    }
}

bool MatchGitHash(const std::string &gitHash)
{
    static const re2::RE2 re(R"((\[[0-9a-zA-Z]+\])\s{0,2}(\[[0-9\-\s\:\+]+\]))");
    if (re2::RE2::FullMatch(gitHash, re)) {
        return true;
    }
    return false;
}

std::string GetGitHash()
{
    std::string gitHash = GIT_HASH;
    static re2::RE2 re("(\\[[a-zA-Z0-9]+\\]) (\\[[0-9\\-\\s\\:\\+]+\\])");

    std::string hash;
    std::string time;

    if (RE2::FullMatch(gitHash, re, &hash, &time)) {
        // Do not return the name for privacy and security purpose.
        return hash + time;
    }
    return "[UNKNOWN VERSION]";
}
}  // namespace datasystem