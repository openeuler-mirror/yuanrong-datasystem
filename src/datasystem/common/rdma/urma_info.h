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

    std::string ToString() const;

    static std::string EidToFmtStr(const urma_eid_t &eid);

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
}  // namespace datasystem

#endif
