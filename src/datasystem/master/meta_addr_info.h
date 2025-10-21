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
 * Description: Implement meta address information.
 */

#ifndef DATASYSTEM_MASTER_META_ADDR_INFO_H
#define DATASYSTEM_MASTER_META_ADDR_INFO_H
#include <sstream>

#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/thread_local.h"

namespace datasystem {
class MetaAddrInfo {
public:
    MetaAddrInfo() = default;
    MetaAddrInfo(HostPort addr, std::string dbName) : addr_(std::move(addr)), dbName_(std::move(dbName))
    {
    }
    size_t GetHash() const
    {
        return std::hash<HostPort>()(addr_) ^ std::hash<std::string>()(dbName_);
    }

    bool operator==(const MetaAddrInfo &other) const
    {
        return this->addr_ == other.addr_ && this->dbName_ == other.dbName_;
    }

    bool operator!=(const MetaAddrInfo &other) const
    {
        return !(*this == other);
    }

    const HostPort &GetAddressAndSaveDbName() const
    {
        g_MetaRocksDbName = dbName_;
        return addr_;
    }

    const HostPort &GetAddress() const
    {
        return addr_;
    }

    const std::string &GetDbName() const
    {
        return dbName_;
    }

    void SetAddress(HostPort addr)
    {
        addr_ = std::move(addr);
    }

    void SetDbName(std::string dbName)
    {
        dbName_ = dbName;
    }

    std::string ToString() const
    {
        std::stringstream ss;
        ss << "addr:" << addr_.ToString();
        if (!dbName_.empty()) {
            ss << ", db name:" << dbName_;
        }
        return ss.str();
    }

    void MarkMetaIsFromOtherAz()
    {
        isFromOtherAz_ = true;
    }

    bool IsFromOtherAz() const
    {
        return isFromOtherAz_;
    }

    void Clear()
    {
        addr_.Clear();
        dbName_.clear();
        isFromOtherAz_ = false;
    }

private:
    HostPort addr_;
    std::string dbName_;
    bool isFromOtherAz_ = false;
};
}  // namespace datasystem

namespace std {
template <>
struct hash<datasystem::MetaAddrInfo> {
    size_t operator()(const datasystem::MetaAddrInfo &info) const
    {
        return info.GetHash();
    }
};
}  // namespace std
#endif
