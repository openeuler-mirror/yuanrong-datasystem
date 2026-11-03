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

#include "datasystem/common/rpc/brpc_auth.h"

#include "datasystem/common/inject/inject_point.h"

#include <cerrno>
#include <new>
#include <brpc/closure_guard.h>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/request_context.h"

namespace datasystem {
namespace {
constexpr char ACCESS_KEY_FIELD[] = "ds.auth.ak";
constexpr char TIMESTAMP_FIELD[] = "ds.auth.ts";
constexpr char SIGNATURE_FIELD[] = "ds.auth.sig";
using Serializer = void (*)(butil::IOBuf *, brpc::Controller *, const google::protobuf::Message *);

struct RpcAuthRequestCarrier final : brpc::SerializedRequest {
    RpcAuthRequestCarrier(const google::protobuf::Message *request, const RpcSigningBinding &binding,
                          Serializer serializer, Status status)
        : request(request), binding(binding), serializer(serializer), status(std::move(status)) {}
    RpcAuthRequestCarrier(const RpcAuthRequestCarrier &) = delete;
    RpcAuthRequestCarrier &operator=(const RpcAuthRequestCarrier &) = delete;
    const google::protobuf::Message *request;
    const RpcSigningBinding &binding;
    Serializer serializer;
    Status status;
};

}  // namespace

Status RpcMethodMap::Create(std::initializer_list<ServicePair> services, std::shared_ptr<const RpcMethodMap> &out)
{
    auto methods = std::make_shared<RpcMethodMap>();
    for (const auto &pair : services) {
        const auto *plain = pair.first;
        const auto *auth = pair.second;
        CHECK_FAIL_RETURN_STATUS(plain != nullptr && auth != nullptr &&
                                     auth->full_name() == plain->full_name() + "Auth" &&
                                     plain->method_count() == auth->method_count(),
                                 K_INVALID, "Invalid RPC authentication service mapping.");
        for (int i = 0; i < plain->method_count(); ++i) {
            const auto *from = plain->method(i);
            const auto *to = auth->method(i);
            CHECK_FAIL_RETURN_STATUS(from->name() == to->name() && from->input_type() == to->input_type() &&
                                         from->output_type() == to->output_type() &&
                                         from->client_streaming() == to->client_streaming() &&
                                         from->server_streaming() == to->server_streaming(),
                                     K_INVALID, "Invalid RPC authentication method mapping.");
            CHECK_FAIL_RETURN_STATUS(methods->methods_.emplace(from, to).second, K_INVALID,
                                     "Duplicate RPC authentication method mapping.");
        }
    }
    out = std::move(methods);
    return Status::OK();
}

const google::protobuf::MethodDescriptor *RpcMethodMap::Find(const google::protobuf::MethodDescriptor *method) const
{
    const auto found = methods_.find(method);
    return found == methods_.end() ? nullptr : found->second;
}

Status RegisterRpcAuthCapture()
{
    BrpcChannelFactory::EnsureGlobalInitialized();
    return RegisterRpcAuthChecksumColdOnce();
}

Status VerifyRpcRequest(AkSkManager &manager, const RpcRequestProof *capture)
{
    CHECK_FAIL_RETURN_STATUS(capture != nullptr && capture->captured, K_NOT_AUTHORIZED,
                             "The AK/SK authorized failed.");
    return manager.VerifySignatureFromDigest(capture->signature, capture->timestampUs,
                                             capture->accessKey, capture->digest);
}

Status BrpcAuthService::Create(google::protobuf::Service &delegate,
                               const google::protobuf::ServiceDescriptor &descriptor, Authenticate authenticate,
                               std::unique_ptr<BrpcAuthService> &out)
{
    CHECK_FAIL_RETURN_STATUS(static_cast<bool>(authenticate), K_INVALID, "Missing RPC authenticator.");
    if (delegate.GetDescriptor() != &descriptor) {
        std::shared_ptr<const RpcMethodMap> mapping;
        RETURN_IF_NOT_OK(RpcMethodMap::Create({{delegate.GetDescriptor(), &descriptor}}, mapping));
    }
    out.reset(new BrpcAuthService(delegate, descriptor, std::move(authenticate)));
    return Status::OK();
}

const google::protobuf::Message &BrpcAuthService::GetRequestPrototype(
    const google::protobuf::MethodDescriptor *method) const
{
    return delegate_.GetRequestPrototype(delegate_.GetDescriptor()->method(method->index()));
}

const google::protobuf::Message &BrpcAuthService::GetResponsePrototype(
    const google::protobuf::MethodDescriptor *method) const
{
    return delegate_.GetResponsePrototype(delegate_.GetDescriptor()->method(method->index()));
}

void BrpcAuthService::CallMethod(const google::protobuf::MethodDescriptor *method,
                                 google::protobuf::RpcController *controller,
                                 const google::protobuf::Message *request, google::protobuf::Message *response,
                                 google::protobuf::Closure *done)
{
    brpc::ClosureGuard completion(done);
    auto *cntl = static_cast<brpc::Controller *>(controller);
    if (method == nullptr || request == nullptr || method->service() != descriptor_) {
        cntl->SetFailed("Invalid authenticated RPC request.");
        return;
    }
    ScopedRequestContext context;
    const int64_t timeoutMs = cntl->timeout_ms();
    if (timeoutMs > 0) {
        GetRequestContext()->reqTimeoutDuration.InitWithPositiveTime(timeoutMs);
    }
    Status status;
    try {
        // BRPC owns a mutable request. Stamp verified identity before exposing it to the native adapter.
        status = authenticate_(*method, *cntl, *const_cast<google::protobuf::Message *>(request));
    } catch (const std::bad_alloc &) {
        status = Status(K_OUT_OF_MEMORY, "Failed to authenticate RPC request.");
    }
    if (status.IsOk() && timeoutMs > 0) {
        const auto remaining = GetRequestContext()->reqTimeoutDuration.CalcRemainingTime();
        if (remaining <= 0) {
            status = Status(K_RPC_DEADLINE_EXCEEDED, "RPC deadline exceeded during authentication.");
        } else {
            cntl->set_timeout_ms(remaining);
        }
    }
    if (status.IsError()) {
        cntl->SetFailed(status.GetMsg() + "\x01" "DS_ERR:" +
                        std::to_string(static_cast<int>(status.GetCode())) + "\x02");
        return;
    }
    delegate_.CallMethod(delegate_.GetDescriptor()->method(method->index()), controller, request, response,
                         completion.release());
}

Status CreateBrpcAuthChannel(const BrpcChannelConfig &config, RpcSigningBinding binding,
                             std::unique_ptr<brpc::Channel> &out)
{
    CHECK_FAIL_RETURN_STATUS(binding.credentials != nullptr && binding.methods != nullptr, K_INVALID,
                             "Missing RPC signing credentials or method mapping.");
    RETURN_IF_NOT_OK(RegisterRpcAuthCapture());
    auto channel = std::make_unique<BrpcAuthChannel>(std::move(binding));
    RETURN_IF_NOT_OK(InitializeBrpcChannel(config, *channel));
    channel->serializer_ = channel->_serialize_request;
    channel->_serialize_request = BrpcAuthChannel::SerializeAuthenticated;
    out = std::move(channel);
    return Status::OK();
}

void BrpcAuthChannel::CallMethod(const google::protobuf::MethodDescriptor *method,
                                 google::protobuf::RpcController *controller,
                                 const google::protobuf::Message *request, google::protobuf::Message *response,
                                 google::protobuf::Closure *done)
{
    const auto *authenticated = binding_.methods->Find(method);
    Status status;
    if (authenticated == nullptr) {
        status = Status(K_INVALID, "Missing RPC authentication method mapping.");
    }
    RpcAuthRequestCarrier carrier(request, binding_, serializer_, std::move(status));
    brpc::Channel::CallMethod(authenticated != nullptr ? authenticated : method, controller, &carrier, response, done);
}

void BrpcAuthChannel::SerializeAuthenticated(butil::IOBuf *out, brpc::Controller *controller,
                                             const google::protobuf::Message *message)
{
    if (controller->Failed()) {
        return;
    }
    const auto *carrier = dynamic_cast<const RpcAuthRequestCarrier *>(message);
    auto prepare = [&]() -> Status {
        CHECK_FAIL_RETURN_STATUS(carrier != nullptr && carrier->serializer != nullptr && carrier->request != nullptr,
                                 K_INVALID, "Invalid RPC authentication request carrier.");
        RETURN_IF_NOT_OK(carrier->status);
        AkSkData key;
        RETURN_IF_NOT_OK(carrier->binding.credentials->SnapshotClientKey(key));
        CHECK_FAIL_RETURN_STATUS(!key.accessKey.empty() && !key.secretKey.Empty(), K_INVALID,
                                 "Missing RPC signing credentials.");
        auto timestamp = static_cast<uint64_t>(GetSystemClockTimeStampUs());
#ifdef WITH_TESTS
        INJECT_POINT("AkSk.SetTimestamp", [&timestamp](uint64_t value) {
            timestamp = value;
            return Status::OK();
        });
#endif
        controller->set_request_checksum_type(brpc::CHECKSUM_TYPE_NONE);
        carrier->serializer(out, controller, carrier->request);
        if (controller->Failed()) {
            return Status::OK();
        }
        AkSkDigest digest{};
        RETURN_IF_NOT_OK(ComputeRpcRequestDigest(timestamp, *out, digest));
        std::string signature;
        RETURN_IF_NOT_OK(Signature::GetSignatureFromDigest(key, digest, signature));
        auto &fields = *controller->request_user_fields();
        fields[ACCESS_KEY_FIELD] = key.accessKey;
        fields[TIMESTAMP_FIELD] = std::to_string(timestamp);
        fields[SIGNATURE_FIELD] = std::move(signature);
        controller->set_request_checksum_type(RPC_AUTH_CHECKSUM_TYPE);
        return Status::OK();
    };
    try {
        const auto status = prepare();
        if (status.IsError()) {
            controller->SetFailed(status.GetMsg() + "\x01" "DS_ERR:" +
                                  std::to_string(static_cast<int>(status.GetCode())) + "\x02");
        }
    } catch (const std::bad_alloc &) {
        controller->SetFailed(ENOMEM, "Failed to allocate RPC authentication request data.");
    }
}
}  // namespace datasystem
