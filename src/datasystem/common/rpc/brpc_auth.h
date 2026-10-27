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

#ifndef DATASYSTEM_COMMON_RPC_BRPC_AUTH_H
#define DATASYSTEM_COMMON_RPC_BRPC_AUTH_H

#include <initializer_list>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include <brpc/channel.h>
#include <brpc/serialized_request.h>

#include "datasystem/common/ak_sk/signature.h"
#include "datasystem/common/rpc/brpc_factory.h"
#include "datasystem/common/rpc/brpc_auth_capture.h"

namespace datasystem {
class AkSkManager;

class RpcMethodMap {
public:
    RpcMethodMap() = default;
    ~RpcMethodMap() = default;
    using ServicePair = std::pair<const google::protobuf::ServiceDescriptor *,
                                  const google::protobuf::ServiceDescriptor *>;
    static Status Create(std::initializer_list<ServicePair> services, std::shared_ptr<const RpcMethodMap> &out);
    const google::protobuf::MethodDescriptor *Find(const google::protobuf::MethodDescriptor *method) const;
private:
    std::unordered_map<const google::protobuf::MethodDescriptor *, const google::protobuf::MethodDescriptor *> methods_;
};

struct RpcSigningBinding {
    std::shared_ptr<Signature> credentials;
    std::shared_ptr<const RpcMethodMap> methods;
};

// BRPC's registry is not thread-safe. Register before starting any RPC server
// or exposing authenticated channels to concurrent requests.
Status RegisterRpcAuthCapture();
Status VerifyRpcRequest(AkSkManager &manager, const RpcRequestProof *proof);

class BrpcAuthService final : public google::protobuf::Service {
public:
    ~BrpcAuthService() override = default;
    using Authenticate = std::function<Status(const google::protobuf::MethodDescriptor &, brpc::Controller &,
                                               google::protobuf::Message &)>;
    static Status Create(google::protobuf::Service &delegate,
                         const google::protobuf::ServiceDescriptor &descriptor, Authenticate authenticate,
                         std::unique_ptr<BrpcAuthService> &out);
    const google::protobuf::ServiceDescriptor *GetDescriptor() override { return descriptor_; }
    const google::protobuf::Message &GetRequestPrototype(
        const google::protobuf::MethodDescriptor *method) const override;
    const google::protobuf::Message &GetResponsePrototype(
        const google::protobuf::MethodDescriptor *method) const override;
    void CallMethod(const google::protobuf::MethodDescriptor *method, google::protobuf::RpcController *controller,
                    const google::protobuf::Message *request, google::protobuf::Message *response,
                    google::protobuf::Closure *done) override;
private:
    BrpcAuthService(google::protobuf::Service &delegate, const google::protobuf::ServiceDescriptor &descriptor,
                     Authenticate authenticate)
        : delegate_(delegate), descriptor_(&descriptor), authenticate_(std::move(authenticate)) {}
    google::protobuf::Service &delegate_;
    const google::protobuf::ServiceDescriptor *descriptor_;
    Authenticate authenticate_;
};

class BrpcAuthChannel final : public brpc::Channel {
public:
    explicit BrpcAuthChannel(RpcSigningBinding binding) : binding_(std::move(binding)) {}
    ~BrpcAuthChannel() override = default;
    void CallMethod(const google::protobuf::MethodDescriptor *method, google::protobuf::RpcController *controller,
                    const google::protobuf::Message *request, google::protobuf::Message *response,
                    google::protobuf::Closure *done) override;
private:
    friend Status CreateBrpcAuthChannel(const BrpcChannelConfig &, RpcSigningBinding,
                                       std::unique_ptr<brpc::Channel> &);
    using Serializer = void (*)(butil::IOBuf *, brpc::Controller *, const google::protobuf::Message *);
    static void SerializeAuthenticated(butil::IOBuf *out, brpc::Controller *controller,
                                       const google::protobuf::Message *message);
    Serializer serializer_ = nullptr;
    RpcSigningBinding binding_;
};

Status CreateBrpcAuthChannel(const BrpcChannelConfig &config, RpcSigningBinding binding,
                             std::unique_ptr<brpc::Channel> &out);
}  // namespace datasystem
#endif  // DATASYSTEM_COMMON_RPC_BRPC_AUTH_H
