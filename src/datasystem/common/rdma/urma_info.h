/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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

#ifndef DATASYSTEM_COMMON_RDMA_URMA_INFO_H
#define DATASYSTEM_COMMON_RDMA_URMA_INFO_H

#include <string>
#include <vector>

#include <ub/umdk/urma/urma_api.h>
#ifdef URMA_OVER_UB
#include <ub/umdk/urma/urma_ubagg.h>
#endif

#include "datasystem/common/rdma/rdma_util.h"
#include "datasystem/common/util/net_util.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/meta_zmq.pb.h"
#include "datasystem/protos/utils.pb.h"

namespace datasystem {
struct UrmaJfrInfo {
    std::string eid;
    uint32_t uasid{ 0 };
    std::vector<uint32_t> jfrIds;
    HostPort localAddress;
#ifdef URMA_OVER_UB
    std::vector<urma_bond_id_info_out_t> bondInfos;
#endif

    std::string ToString() const;

    static std::string EidToFmtStr(const urma_eid_t &eid);
#ifdef URMA_OVER_UB
    static void UrmaBondIdInfoToProto(const urma_bond_id_info_out &info, JfrBondInfo &proto);
    static Status UrmaBondIdInfoFromProto(const JfrBondInfo &proto, urma_bond_id_info_out &info);
#endif

    template <class Proto>
    void ToProto(Proto &proto) const
    {
        proto.set_eid(eid);
        proto.set_uasid(uasid);
        for (auto jfrId : jfrIds) {
            proto.add_jfr_ids(jfrId);
        }
        proto.mutable_address()->set_host(localAddress.Host());
        proto.mutable_address()->set_port(localAddress.Port());
#ifdef URMA_OVER_UB
        if (GetUrmaMode() == UrmaMode::UB) {
            for (auto &bondInfo : bondInfos) {
                auto info = proto.add_bond_infos();
                UrmaBondIdInfoToProto(bondInfo, *info);
            }
        }
#endif
    }

    template <class Proto>
    Status FromProto(const Proto &proto)
    {
        eid = proto.eid();
        uasid = proto.uasid();
        for (auto jfrId : proto.jfr_ids()) {
            jfrIds.emplace_back(jfrId);
        }
        localAddress = HostPort(proto.address().host(), proto.address().port());
#ifdef URMA_OVER_UB
        if (GetUrmaMode() == UrmaMode::UB) {
            auto size = proto.bond_infos_size();
            for (int i = 0; i < size; i++) {
                bondInfos.emplace_back();
                RETURN_IF_NOT_OK(UrmaBondIdInfoFromProto(proto.bond_infos(i), bondInfos[i]));
            }
        }
#endif
        return Status::OK();
    }
};

struct UrmaSeg {
    urma_seg_t raw;
    static void ToProto(const urma_seg_t &seg, UrmaSegPb &proto);
    static Status FromProto(const UrmaSegPb &proto, urma_seg_t &seg);
    static std::string ToString(const urma_seg_t &seg);

    std::string ToString();
    void ToProto(UrmaSegPb &proto) const;
    Status FromProto(const UrmaSegPb &proto);
};

#ifdef URMA_OVER_UB
struct UrmaBondSegInfo {
    urma_bond_seg_info_out raw;

    std::string ToString();
    void ToProto(UrmaBondSegInfoPb &proto) const;
    Status FromProto(const UrmaBondSegInfoPb &proto);
};
#endif

}  // namespace datasystem

#endif
