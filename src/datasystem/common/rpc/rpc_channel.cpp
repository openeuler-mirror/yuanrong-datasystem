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
 * Description: Rpc channel description.
 * This file does not have any connection.
 */
#include <sstream>
#include "datasystem/common/rpc/rpc_channel.h"
#include "datasystem/common/rdma/urma_manager_wrapper.h"

namespace datasystem {
RpcChannel::RpcChannel(std::string zmqEndPoint, const RpcCredential &cred)
    : endPoint_(std::move(zmqEndPoint)), cred_(cred)
{
}

RpcChannel::RpcChannel(const HostPort &destAddr, const RpcCredential &cred)
    : endPoint_(TcpipEndPoint(destAddr)), cred_(cred), destAddr_(destAddr)
{
}

RpcChannel::~RpcChannel() = default;

std::string RpcChannel::UnixSocketPath(const std::string &socketFileDir, const HostPort &localAddress)
{
    std::string unixSocket =
        "ipc://" + socketFileDir + "/" + localAddress.Host() + "_" + std::to_string(localAddress.Port());
    return unixSocket;
}

void RpcChannel::SetServiceUdsEnabled(const std::string &svcName, const std::string &sockName)
{
    std::lock_guard<std::mutex> lock(udsMux_);
    udsCfg_[svcName] = sockName;
}

std::string RpcChannel::GetServiceSockName(const std::string &svcName)
{
    std::lock_guard<std::mutex> lock(udsMux_);
    auto it = udsCfg_.find(svcName);
    return it == udsCfg_.end() ? "" : it->second;
}

void RpcChannel::SetServiceTcpDirect(const std::string &svcName)
{
    std::lock_guard<std::mutex> lock(udsMux_);
    tcpDirect_[svcName] = true;
}

bool RpcChannel::GetServiceTcpDirect(const std::string &svcName)
{
    std::lock_guard<std::mutex> lock(udsMux_);
    auto it = tcpDirect_.find(svcName);
    return it == tcpDirect_.end() ? false : it->second;
}

void RpcChannel::SetServiceConnectPoolSize(const std::string &svcName, size_t sz)
{
    std::lock_guard<std::mutex> lock(udsMux_);
    connectPoolSize_[svcName] = std::max<size_t>(sz, 1);
}

size_t RpcChannel::GetServiceConnectPoolSize(const std::string &svcName)
{
    std::lock_guard<std::mutex> lock(udsMux_);
    auto it = connectPoolSize_.find(svcName);
    return it == connectPoolSize_.end() ? 1 : it->second;
}

std::string RpcChannel::TcpipEndPoint(const HostPort &localAddress)
{
    return std::string("tcp://") + localAddress.ToString();
}

const std::string &RpcChannel::GetZmqEndPoint() const
{
    return endPoint_;
}

const HostPort &RpcChannel::GetHostPort() const
{
    return destAddr_;
}

void RpcChannel::SetLocalInfo(const HostPort &localAddress)
{
    (void)localAddress;
#ifdef USE_URMA
    if (UrmaManager::IsUrmaEnabled()) {
        auto &mgr = UrmaManager::Instance();
        localUrmaInfo_ = std::make_unique<UrmaInfo>();
        localUrmaInfo_->eid = mgr.GetEid();
        localUrmaInfo_->uasid = mgr.GetUasid();
        localUrmaInfo_->jfr_ids = mgr.GetJfrIds();
        localUrmaInfo_->localAddress_ = localAddress;
        LOG(INFO) << localUrmaInfo_->ToString();
    }
#endif
}

#ifdef USE_URMA
void RpcChannel::GetLocalUrmaInfo(RpcChannel::UrmaInfo &out) const
{
    if (localUrmaInfo_) {
        out.eid = localUrmaInfo_->eid;
        out.uasid = localUrmaInfo_->uasid;
        out.jfr_ids = localUrmaInfo_->jfr_ids;
        out.localAddress_ = localUrmaInfo_->localAddress_;
    }
}

std::string RpcChannel::UrmaInfo::ToString() const
{
    std::stringstream oss;
    oss << localAddress_.ToString() << " urma info. eid ";
    // eid is not really printable as a string. So we will dump its context in hex
    urma_eid_t e;
    if (UrmaManager::StrToEid(eid, e).IsOk()) {
        char s[URMA_EID_STR_LEN + 1];
        int ret = sprintf_s(s, URMA_EID_STR_LEN + 1, EID_FMT, EID_ARGS(e));
        if (ret == -1) {
            LOG(WARNING) << "sprintf_s eid in EID_FMT failed";
            oss << eid;
        } else {
            oss << s;
        }
    } else {
        oss << eid;
    }
    oss << " uasid " << uasid << " jfr_id [";
    bool first = true;
    for (auto jfr_id : jfr_ids) {
        if (first) {
            first = false;
        } else {
            oss << " ";
        }
        oss << jfr_id;
    }
    oss << "]";
    return oss.str();
}
#endif
}  // namespace datasystem
