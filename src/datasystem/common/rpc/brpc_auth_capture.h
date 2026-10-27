/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#ifndef DATASYSTEM_COMMON_RPC_BRPC_AUTH_CAPTURE_H
#define DATASYSTEM_COMMON_RPC_BRPC_AUTH_CAPTURE_H

#include <string>

#include <brpc/checksum.h>
#include <brpc/controller.h>
#include <brpc/data_factory.h>

#include "datasystem/common/ak_sk/signature.h"

namespace datasystem {
inline constexpr auto RPC_AUTH_CHECKSUM_TYPE = static_cast<brpc::ChecksumType>(1004);

struct RpcRequestProof {
    bool captured = false;
    AkSkDigest digest{};
    uint64_t timestampUs = 0;
    std::string accessKey;
    std::string signature;
};

struct RpcRequestCapture : RpcRequestProof {
    const brpc::Controller *owner = nullptr;
};

class BrpcAuthCapture final : public brpc::DataFactory {
public:
    ~BrpcAuthCapture() override = default;
    void *CreateData() const override;
    void DestroyData(void *data) const override;
    bool ResetData(void *data) const override;
};

// Only the shared BRPC cold initialization may first register the handler.
Status RegisterRpcAuthChecksumColdOnce();
Status ComputeRpcRequestDigest(uint64_t timestampUs, const butil::IOBuf &body, AkSkDigest &out);
Status CaptureRpcRequest(const brpc::ChecksumIn &input, RpcRequestCapture &capture);
const RpcRequestCapture *GetRpcRequestCapture(brpc::Controller &controller);
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_BRPC_AUTH_CAPTURE_H
