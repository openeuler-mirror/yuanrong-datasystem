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

#include <atomic>

#include <brpc/closure_guard.h>
#include <brpc/nonreflectable_message.h>
#include <brpc/server.h>
#include <google/protobuf/descriptor.pb.h>
#include <gtest/gtest.h>

#include "datasystem/common/ak_sk/ak_sk_manager.h"
#include "datasystem/common/rpc/brpc_auth.h"
#include "datasystem/protos/ut_object.pb.h"

namespace datasystem {
namespace test {
namespace {
class CountedRequest final : public brpc::NonreflectableMessage<CountedRequest> {
public:
    CountedRequest() = default;
    explicit CountedRequest(const google::protobuf::Message &message) : message_(&message) {}
    void MergeFrom(const CountedRequest &other) override { message_ = other.message_; }
    size_t ByteSizeLong() const override { return message_->ByteSizeLong(); }
    uint8_t *_InternalSerialize(uint8_t *target, google::protobuf::io::EpsCopyOutputStream *stream) const override
    {
        ++encodes;
        return message_->_InternalSerialize(target, stream);
    }
    mutable unsigned encodes = 0;
private:
    const google::protobuf::Message *message_ = nullptr;
};

class AuthProbe final : public google::protobuf::Service {
public:
    AuthProbe() : pool_(google::protobuf::DescriptorPool::generated_pool())
    {
        google::protobuf::FileDescriptorProto file;
        file.set_name("rpc_byte_auth_test.proto");
        file.set_package("rpc_byte_auth_test");
        file.add_dependency(TestReqPbV1::descriptor()->file()->name());
        for (const auto *name : {"Probe", "ProbeAuth"}) {
            auto *service = file.add_service();
            service->set_name(name);
            auto *method = service->add_method();
            method->set_name("Echo");
            method->set_input_type(".datasystem.TestReqPbV1");
            method->set_output_type(".datasystem.TestRspPbV1");
        }
        file_ = pool_.BuildFile(file);
    }
    const google::protobuf::ServiceDescriptor *Plain() const { return file_->service(0); }
    const google::protobuf::ServiceDescriptor *Authenticated() const { return file_->service(1); }
    const google::protobuf::ServiceDescriptor *GetDescriptor() override { return Plain(); }
    const google::protobuf::Message &GetRequestPrototype(const google::protobuf::MethodDescriptor *) const override
    {
        return TestReqPbV1::default_instance();
    }
    const google::protobuf::Message &GetResponsePrototype(const google::protobuf::MethodDescriptor *) const override
    {
        return TestRspPbV1::default_instance();
    }
    void CallMethod(const google::protobuf::MethodDescriptor *, google::protobuf::RpcController *base,
                    const google::protobuf::Message *request, google::protobuf::Message *,
                    google::protobuf::Closure *done) override
    {
        brpc::ClosureGuard guard(done);
        auto *controller = static_cast<brpc::Controller *>(base);
        const auto *capture = GetRpcRequestCapture(*controller);
        if (capture == nullptr) {
            ADD_FAILURE() << "Authenticated request lost its capture";
            return;
        }
        butil::IOBuf reencoded;
        reencoded.append(request->SerializeAsString());
        AkSkDigest digest{};
        EXPECT_TRUE(ComputeRpcRequestDigest(capture->timestampUs, reencoded, digest).IsOk());
        EXPECT_NE(digest, capture->digest);
        verified.fetch_add(1);
    }
    AkSkManager manager;
    std::atomic<unsigned> verified{0};
private:
    google::protobuf::DescriptorPool pool_;
    const google::protobuf::FileDescriptor *file_ = nullptr;
};

class BrpcAuthTest : public ::testing::Test {
protected:
    void SetUp() override
    {
        ASSERT_TRUE(RegisterRpcAuthCapture().IsOk());
        ASSERT_TRUE(service_.manager.SetServerAkSk(
            AkSkType::SYSTEM, "test-only-access", SensitiveValue("test-only-secret")).IsOk());
        const auto authenticate =
            [this](const google::protobuf::MethodDescriptor &, brpc::Controller &controller,
                   google::protobuf::Message &) {
                return VerifyRpcRequest(service_.manager, GetRpcRequestCapture(controller));
            };
        ASSERT_TRUE(BrpcAuthService::Create(service_, *service_.Plain(), authenticate, plain_).IsOk());
        ASSERT_TRUE(BrpcAuthService::Create(service_, *service_.Authenticated(), authenticate, authenticated_).IsOk());
        ASSERT_EQ(server_.AddService(plain_.get(), brpc::SERVER_DOESNT_OWN_SERVICE), 0);
        ASSERT_EQ(server_.AddService(authenticated_.get(), brpc::SERVER_DOESNT_OWN_SERVICE), 0);
        brpc::ServerOptions options;
        options.session_local_data_factory = &factory_;
        ASSERT_EQ(server_.Start("127.0.0.1:0", &options), 0);
        ASSERT_TRUE(RpcMethodMap::Create({{service_.Plain(), service_.Authenticated()}}, methods_).IsOk());
        provider_ = std::make_shared<Signature>("test-only-access", SensitiveValue("test-only-secret"));
        BrpcChannelConfig config;
        config.endpoint = butil::endpoint2str(server_.listen_address()).c_str();
        config.timeout_ms = 1000;
        config.max_retry = 0;
        ASSERT_TRUE(CreateBrpcAuthChannel(config, {provider_, methods_}, channel_).IsOk());
    }
    void TearDown() override
    {
        server_.Stop(0);
        server_.Join();
    }
    BrpcAuthCapture factory_;
    AuthProbe service_;
    std::unique_ptr<BrpcAuthService> plain_;
    std::unique_ptr<BrpcAuthService> authenticated_;
    brpc::Server server_;
    std::shared_ptr<Signature> provider_;
    std::shared_ptr<const RpcMethodMap> methods_;
    std::unique_ptr<brpc::Channel> channel_;
};
}  // namespace

TEST_F(BrpcAuthTest, EncodesOnceAndVerifiesRawBytesAcrossSchemas)
{
    butil::IOBuf body;
    body.append("test-only-body");
    brpc::Controller malformed;
    auto &fields = *malformed.request_user_fields();
    fields["ds.auth.ak"] = std::string(4096, 'a');
    fields["ds.auth.sig"] = std::string(65, 's');
    fields["ds.auth.ts"] = "1";
    RpcRequestCapture rejected;
    const brpc::ChecksumIn input{&body, &malformed};
    EXPECT_EQ(CaptureRpcRequest(input, rejected).GetCode(), K_NOT_AUTHORIZED);
    EXPECT_TRUE(rejected.accessKey.empty());
    EXPECT_TRUE(rejected.signature.empty());
    EXPECT_FALSE(rejected.captured);
    fields["ds.auth.ak"] = "test-only-access";
    fields["ds.auth.sig"] = std::string(4096, 's');
    EXPECT_EQ(CaptureRpcRequest(input, rejected).GetCode(), K_NOT_AUTHORIZED);
    EXPECT_TRUE(rejected.accessKey.empty());
    EXPECT_TRUE(rejected.signature.empty());
    EXPECT_FALSE(rejected.captured);
    TestReqPbV2 request;
    request.set_filed1(1);
    request.set_filed5(5);
    request.set_auth_req(2);
    unsigned verified = 0;
    for (const auto compression : {brpc::COMPRESS_TYPE_NONE, brpc::COMPRESS_TYPE_GZIP}) {
        SCOPED_TRACE(compression);
        CountedRequest counted(request);
        TestRspPbV1 response;
        brpc::Controller controller;
        controller.set_request_compress_type(compression);
        channel_->CallMethod(service_.Plain()->method(0), &controller, &counted, &response, nullptr);
        EXPECT_FALSE(controller.Failed()) << controller.ErrorText();
        EXPECT_EQ(counted.encodes, 1);
        EXPECT_EQ(service_.verified.load(), ++verified);
    }
    ASSERT_TRUE(provider_->SetClientAkSk("test-only-access", SensitiveValue("test-only-invalid-secret")).IsOk());
    TestRspPbV1 response;
    brpc::Controller controller;
    CountedRequest invalid(request);
    channel_->CallMethod(service_.Plain()->method(0), &controller, &invalid, &response, nullptr);
    EXPECT_TRUE(controller.Failed());
    EXPECT_EQ(invalid.encodes, 1);
    EXPECT_EQ(service_.verified.load(), verified);
    brpc::Channel plain;
    brpc::ChannelOptions options;
    options.timeout_ms = 1000;
    options.max_retry = 0;
    ASSERT_EQ(plain.Init(server_.listen_address(), &options), 0);
    brpc::Controller unsignedCall;
    TestReqPbV1 unsignedRequest;
    plain.CallMethod(service_.Plain()->method(0), &unsignedCall, &unsignedRequest, &response, nullptr);
    EXPECT_TRUE(unsignedCall.Failed());
    EXPECT_EQ(service_.verified.load(), verified);
}

}  // namespace test
}  // namespace datasystem
