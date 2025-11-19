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
 * Description: Manages the grpc channel and encapsulates the sending function.
 */
#ifndef DATASYSTEM_COMMON_KVSTORE_ETCD_GRPC_SESSION_H
#define DATASYSTEM_COMMON_KVSTORE_ETCD_GRPC_SESSION_H

#include <cstddef>
#include <memory>
#include <string>
#include <thread>
#include <unordered_set>

#include <grpcpp/grpcpp.h>
#include <tbb/concurrent_hash_map.h>

#include "datasystem/common/log/log.h"
#include "datasystem/common/encrypt/secret_manager.h"
#include "datasystem/common/kvstore/etcd/etcd_constants.h"
#include "datasystem/common/metrics/metrics_vector/metrics_blocking_vector.h"
#include "datasystem/common/log/access_recorder.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/format.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/rpc_util.h"
#include "datasystem/common/util/ssl_authorization.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/util/validator.h"
#include "datasystem/utils/sensitive_value.h"
#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/signal/signal.h"
#include "datasystem/utils/status.h"
#include "datasystem/common/util/template_util.h"

DS_DECLARE_string(encrypt_kit);
DS_DECLARE_bool(enable_etcd_auth);
DS_DECLARE_string(etcd_target_name_override);
DS_DECLARE_string(etcd_ca);
DS_DECLARE_string(etcd_cert);
DS_DECLARE_string(etcd_key);
DS_DECLARE_string(etcd_passphrase_path);

namespace datasystem {
static constexpr int SEND_RPC_TIMEOUT_MS_DEFAULT = 50000;
struct RouterClientCurveKit {
    std::string etcdCa;
    SensitiveValue etcdCert;
    SensitiveValue etcdKey;
    const std::string etcdNameOverride;
};

class GrpcSessionBase {
public:
    static void SetIsKeepAliveTimeoutHandler(std::function<bool()> &&isKeepAliveTimeoutHandler)
    {
        isKeepAliveTimeoutHandler_ = std::move(isKeepAliveTimeoutHandler);
    }

protected:
    static std::function<bool()> isKeepAliveTimeoutHandler_;
};

template <typename T>
class GrpcSession : public GrpcSessionBase {
public:
    /**
     * @brief Creating an RPC session class.
     * @param[in] addresses Destination host port.
     * @param[in] etcdCa Root etcd certificate, optional parameters.
     * @param[in] etcdCert Etcd certificate chain, optional parameters.
     * @param[in] etcdKey Etcd private key, optional parameters.
     * @param[in] targetNameOverride etcd DNS name, optional parameters.
     * @param[out] rpcSession an RPC session.
     * @return Status of the call.
     */
    static Status CreateSession(const std::string &addresses, std::unique_ptr<GrpcSession> &rpcSession,
                                const std::string &etcdCa = "", const SensitiveValue &etcdCert = "",
                                const SensitiveValue &etcdKey = "", const std::string &targetNameOverride = "")
    {
        // for routeClient
        if (!etcdCa.empty() && !etcdCert.Empty() && !etcdKey.Empty()) {
            return CreateSessionWithTls(addresses, etcdCa, etcdCert, etcdKey, targetNameOverride, rpcSession);
        }

        if (!FLAGS_enable_etcd_auth) {
            rpcSession = std::move(CreateSessionWithoutTls(addresses));
            return Status::OK();
        }

        LOG_IF(WARNING, FLAGS_encrypt_kit == ENCRYPT_KIT_PLAINTEXT)
            << "Etcd auth is enabled but sensitive information is not encrypted, avoid use in production environments.";

        RETURN_IF_NOT_OK(ReadEtcdCertAndCreateSession(addresses, FLAGS_etcd_ca, FLAGS_etcd_cert, FLAGS_etcd_key,
                                                          FLAGS_etcd_target_name_override, FLAGS_etcd_passphrase_path,
                                                          true, rpcSession));

        return Status::OK();
    }

    static void SetCommonChannelArgs(grpc::ChannelArguments &args)
    {
        args.SetMaxSendMessageSize(std::numeric_limits<int>::max());
        args.SetMaxReceiveMessageSize(std::numeric_limits<int>::max());
        static constexpr int initialTimeoutMs = 1'000;
        static constexpr int reconnectTimeoutMs = 5'000;
        args.SetInt(GRPC_ARG_INITIAL_RECONNECT_BACKOFF_MS, initialTimeoutMs);
        args.SetInt(GRPC_ARG_MIN_RECONNECT_BACKOFF_MS, reconnectTimeoutMs);
        args.SetInt(GRPC_ARG_MAX_RECONNECT_BACKOFF_MS, reconnectTimeoutMs);
    }

    /**
     * @brief Creating an RPC session without TLS class.
     * @param[in] addresses Destination host port.
     * @return GrpcSession object.
     */
    static std::unique_ptr<GrpcSession> CreateSessionWithoutTls(const std::string &addresses)
    {
        // Stub create.
        grpc::ChannelArguments args;
        SetCommonChannelArgs(args);
        std::string prefix = GetConnectPrefix(addresses);
        std::shared_ptr<grpc::Channel> channel =
            grpc::CreateCustomChannel(prefix + addresses, grpc::InsecureChannelCredentials(), args);
        std::unique_ptr<typename T::Stub> rpcStub = T::NewStub(channel);
        // Rpc create.
        auto session = std::make_unique<GrpcSession>(addresses, std::move(rpcStub));
        LOG(INFO) << "Create rpc session. Prefix: " << prefix << ", dst address: " << addresses;
        return session;
    }

    /**
     * @brief Creating an RPC session class with client certificate.
     * @param[in] addresses Destination host port.
     * @param[in] ca the CA.
     * @param[in] cert the cert.
     * @param[in] key the private key.
     * @param[in] targetNameOverride SSL target name.
     * @param[out] rpcSession an RPC session.
     * @return Status of the call.
     */
    static Status CreateSessionWithTls(const std::string &addresses, const std::string &ca, const SensitiveValue &cert,
                                       const SensitiveValue &key, const std::string &targetNameOverride,
                                       std::unique_ptr<GrpcSession> &rpcSession)
    {
        grpc::ChannelArguments args;
        SetCommonChannelArgs(args);
        grpc::SslCredentialsOptions sslCredOpt{ .pem_root_certs = ca,
                                                .pem_private_key = { key.GetData(), key.GetSize() },
                                                .pem_cert_chain = { cert.GetData(), cert.GetSize() } };

        Raii cleanKey([&sslCredOpt] {
            std::fill(sslCredOpt.pem_private_key.begin(), sslCredOpt.pem_private_key.end(), 0);
            std::fill(sslCredOpt.pem_cert_chain.begin(), sslCredOpt.pem_cert_chain.end(), 0);
        });
        std::shared_ptr<grpc::ChannelCredentials> creds = grpc::SslCredentials(sslCredOpt);
        CHECK_FAIL_RETURN_STATUS(creds != nullptr, K_RUNTIME_ERROR, "GRPC SslCredentials could not be created");
        if (!targetNameOverride.empty()) {
            args.SetSslTargetNameOverride(targetNameOverride);
        }
        std::string prefix = GetConnectPrefix(addresses);
        std::shared_ptr<grpc::Channel> channel = grpc::CreateCustomChannel(prefix + addresses, creds, args);
        std::unique_ptr<typename T::Stub> rpcStub = T::NewStub(channel);
        // Rpc create.
        rpcSession = std::make_unique<GrpcSession>(addresses, std::move(rpcStub));
        LOG(INFO) << "Create rpc session with client certificate. Prefix: " << prefix << ", dst address: " << addresses;
        return Status::OK();
    }

    GrpcSession(const std::string &addresses, std::unique_ptr<typename T::Stub> stub)
        : addresses_(addresses), stub_(std::move(stub)){};

    ~GrpcSession() = default;

    typename T::Stub *Stub()
    {
        return stub_.get();
    }

    DEFINE_MEMBER_FUNCTION_CHECKER(lease);

    /**
     * @brief Send RPC messages synchronously.
     * @param[in] methodName Method name.
     * @param[in] req Request proto msg.
     * @param[out] rsp Response proto msg.
     * @param[in] stubFunc Grpc func.
     * @param[in] timeoutSeconds Timeout, in seconds. The default value is 50.
     * @param[in] asyncElapse The time this object being in the async queue.
     * @return Status of the call.
     */
    template <typename Request, typename Response,
              typename StubFunc = grpc::Status (*)(grpc::ClientContext *, const Request &, Response *)>
    Status SendRpc(const std::string &methodName, const Request &req, Response &rsp, StubFunc stubFunc,
                   int dataSize = 0, int32_t timeoutMs = SEND_RPC_TIMEOUT_MS_DEFAULT, uint64_t asyncElapse = 0)
    {
        INJECT_POINT("etcd.sendrpc", [&timeoutMs](int32_t timeout) {
            timeoutMs = timeout;
            return Status::OK();
        });
        auto checker = [&methodName, &req]() -> Status {
            if constexpr (HasMemberFunc_lease<Request>::value) {
                CHECK_FAIL_RETURN_STATUS(
                    !(methodName == "Put::etcd_kv_Put" && req.key().find(ETCD_CLUSTER_TABLE) != std::string::npos
                      && req.lease() == 0),
                    K_INVALID, "The key written to the cluster table must be bound to a lease");
            }
            return Status::OK();
        };
        auto func = [&](int32_t rpcTimeoutMs) {
            RETURN_IF_NOT_OK(checker());
            std::string preMsg = "[" + methodName + "] ";
            grpc::ClientContext context;
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(rpcTimeoutMs));
            contextMap_.emplace(&context, false);
            AccessRecorder requestOutPoint(GetEtcdReqRecorderKey(methodName));
            grpc::Status status = (stub_.get()->*stubFunc)(&context, req, &rsp);
            INJECT_POINT("grpc_session.SendRpc.EtcdRequestFailed", [&status]() {
                status = grpc::Status(grpc::StatusCode::UNKNOWN, "Mock etcd operation error.");
                return Status::OK();
            });
            RequestParam reqParam;
            reqParam.outReq = req.key();
            auto returnCode = static_cast<int>(status.error_code());
            requestOutPoint.Record(returnCode, std::to_string(dataSize), reqParam, status.error_message(), asyncElapse);
            requestStatusVec_.BlockingEmplaceBackCode(returnCode);
            contextMap_.erase(&context);
            if (!status.ok()) {
                if (IsTermSignalReceived() && isKeepAliveTimeoutHandler_ && isKeepAliveTimeoutHandler_()) {
                    RETURN_STATUS(K_RETRY_IF_LEAVING, "During worker exit, avoid accessing etcd if etcd fails.");
                } else {
                    RETURN_STATUS(K_RPC_UNAVAILABLE, preMsg + "Send rpc failed: (" + std::to_string(returnCode) + ") "
                                                         + status.error_message());
                }
            }
            return Status::OK();
        };
        INJECT_POINT("SendRpc.Failed.isKeepAliveTimeoutHandler");
        if (IsTermSignalReceived() && isKeepAliveTimeoutHandler_ && isKeepAliveTimeoutHandler_()) {
            RETURN_STATUS(K_RETRY_IF_LEAVING, "During worker exit, avoid accessing etcd if etcd fails.");
        }
        int defaultTimeoutMs = 10000;  // 10s
        return RetryOnError(
            timeoutMs, std::move(func), []() { return Status::OK(); }, { K_RPC_UNAVAILABLE }, defaultTimeoutMs);
    }

    /**
     * @brief Async send RPC messages synchronously.
     * @param[in] methodName methon name
     * @param[in] req request proto msg
     * @param[in] rsp response proto msg
     * @param[in] stubFunc grpc func
     * @param[in] timeoutMs request timeout in milliseconds
     * @return Status of the call
     */
    template <typename Request, typename Response,
              typename StubFunc = grpc::Status (*)(grpc::ClientContext *, const Request &, Response *)>
    Status AsyncSendRpc(const std::string &methodName, const Request &req, Response &rsp, StubFunc stubFunc,
                        uint64_t timeoutMs = 0)
    {
        std::string preMsg = "[" + addresses_ + "," + methodName + "] ";
        grpc::ClientContext context;
        grpc::CompletionQueue cq;
        grpc::Status status;
        if (timeoutMs > 0) {
            context.set_deadline(std::chrono::system_clock::now() + std::chrono::milliseconds(timeoutMs));
        }
        cqMap_.emplace(&cq, &context);

        std::unique_ptr<AccessRecorder> requestOutPoint = nullptr;
        auto recoredKey = GetEtcdReqRecorderKey(methodName);
        if (recoredKey != AccessRecorderKey::DS_ETCD_UNKNOWN) {
            requestOutPoint = std::make_unique<AccessRecorder>(recoredKey);
        }

        auto rspReader = (stub_.get()->*stubFunc)(&context, req, &cq);
        rspReader->Finish(&rsp, &status, (void *)this);
        // wait for response.
        void *tag = nullptr;
        bool ok = false;
        cq.Next(&tag, &ok);
        if (requestOutPoint != nullptr) {
            RequestParam reqParam;
            reqParam.outReq = req.ShortDebugString();
            auto returnCode = static_cast<int>(status.error_code());
            auto outResp = status.ok() ? rsp.ShortDebugString() : status.error_message();
            requestOutPoint->Record(returnCode, "0", reqParam, outResp, 0);
        }
        CHECK_FAIL_RETURN_STATUS(tag == (void *)this, StatusCode::K_RUNTIME_ERROR, "not equal tag");
        cqMap_.erase(&cq);
        RETURN_OK_IF_TRUE(status.ok());
        RETURN_STATUS_LOG_ERROR(K_RPC_UNAVAILABLE, preMsg + "Send rpc failed:" + status.error_message());
    }

    /**
     * @brief Shutdown rpc session and let blocked RPC request exit.
     */
    void Shutdown()
    {
        for (const auto &pair : contextMap_) {
            auto *context = (grpc::ClientContext *)pair.first;
            if (context != nullptr) {
                context->TryCancel();
            }
        }
        for (const auto &pair : cqMap_) {
            auto *cq = (grpc::CompletionQueue *)pair.first;
            if (cq != nullptr) {
                cq->Shutdown();
            }

            auto *context = (grpc::ClientContext *)pair.second;
            if (context != nullptr) {
                context->TryCancel();
            }
        }
    }

    /**
     * @brief Get grpc connect prefix
     * @param[in] addresses The ip list ready to connect.
     * @return return "ipv4:///" if addresses are ipv4 host format.
     */
    static std::string GetConnectPrefix(const std::string &addresses)
    {
        char delimiter = ',';
        std::stringstream sstream(addresses);
        std::string word;
        int numIPv6 = 0;
        // Apparently addresses can contain more than one address since this is a loop.
        // If at least one of the address is in the IPv6 format, return v6 prefix.
        while (std::getline(sstream, word, delimiter)) {
            if (Validator::ValidateHostPortString("GrpcAddress", word)) {
                // Host is valid. Check if it was a IPv6.
                if (Validator::IsValidIPv6HostPortString("GrpcAddress", word)) {
                    ++numIPv6;
                }
                continue;
            } else {
                return "";
            }
        }
        return (numIPv6 == 0) ? "ipv4:///" : "ipv6:///";
    }

    /**
     * @brief Reads the etcd certificate and establishes a connection with etcd.
     * @param[in] addresses Etcd address.
     * @param[in] etcdCaPath Root etcd certificate path.
     * @param[in] etcdCertPath Etcd certificate chain path.
     * @param[in] etcdKeyPath Etcd private key path.
     * @param[in] targetNameOverride Etcd DNS name.
     * @param[in] etcdPassphrasePath Etcd passphrase path.
     * @param[in] isContentEncrypted Check whether the certificate text is encrypted.
     * @param[out] rpcSession An RPC session.
     * @return Status of the call.
     */
    static Status ReadEtcdCertAndCreateSession(const std::string &addresses, const std::string &etcdCaPath,
                                               const std::string &etcdCertPath, const std::string &etcdKeyPath,
                                               const std::string &targetNameOverride,
                                               const std::string &etcdPassphrasePath, bool isContentEncrypted,
                                               std::unique_ptr<GrpcSession> &rpcSession)
    {
        SensitiveValue caCert;
        SensitiveValue clientCert;
        SensitiveValue clientPrivateKey;
        if (!etcdPassphrasePath.empty()) {
            SensitiveValue passphrase;
            // decrypt passphrase.
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ReadText(etcdPassphrasePath, passphrase, isContentEncrypted),
                                             "Etcd passphrase decrypt failed.");
            // decrypt client private key.
            RETURN_IF_NOT_OK(DecryptRSAPrivateKeyToMemoryInPemFormat(etcdKeyPath, passphrase, clientPrivateKey));
        } else {
            // decrypt client private key.
            RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ReadText(etcdKeyPath, clientPrivateKey, isContentEncrypted),
                                             "Etcd private key decrypt failed.");
        }
        // read ca cert
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ReadText(etcdCaPath, caCert, false), "Etcd ca cert decrypt failed.");
        // read client cert
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(ReadText(etcdCertPath, clientCert, false), "Etcd client cert decrypt failed.");
        return CreateSessionWithTls(addresses, std::string(caCert.GetData(), caCert.GetSize()), clientCert,
                                    clientPrivateKey, targetNameOverride, rpcSession);
    }

    /**
     * @brief Obtains the success rate of all etcd requests. BlockingGetRate will clean vector.
     * @return The success rate string.
     */
    std::string BlockingGetSuccessRateAndClean()
    {
        return requestStatusVec_.BlockingGetRateToStringAndClean();
    }

private:
    /**
     * @brief This method is used to obtain different AccessRecorderKey for different etcd request types.
     * @param[in] methodName The type of etcd request.
     * @return AccessRecorderKey to different etcd requests.
     */
    AccessRecorderKey GetEtcdReqRecorderKey(const std::string &methodName)
    {
        static std::map<std::string, AccessRecorderKey> requestMethods = {
            { "DropTable::etcd_kv_DeleteRange", AccessRecorderKey::DS_ETCD_DROPTABLE },
            { "Put::etcd_kv_Put", AccessRecorderKey::DS_ETCD_PUT },
            { "Get::etcd_kv_Range", AccessRecorderKey::DS_ETCD_GET },
            { "GetAll::etcd_kv_Range", AccessRecorderKey::DS_ETCD_GETALL },
            { "PrefixSearch::etcd_kv_Range", AccessRecorderKey::DS_ETCD_PREFIXSEARCH },
            { "Delete::etcd_kv_DeleteRange", AccessRecorderKey::DS_ETCD_DELETE },
            { "LeaseGrant", AccessRecorderKey::DS_ETCD_LEASE_GRANT }
        };
        auto iter = requestMethods.find(methodName);
        if (iter == requestMethods.end()) {
            return AccessRecorderKey::DS_ETCD_UNKNOWN;
        }
        return requestMethods[methodName];
    }

    /**
     * @brief Reading a plaintext or ciphertext certificate.
     * @param[in] path Indicates the certificate path to be read.
     * @param[in] isTextEncrypted Check whether the certificate text is encrypted.
     * @param[out] value Certificate characters.
     * @return Status of the call.
     */
    static Status ReadText(const std::string &path, SensitiveValue &value, bool isTextEncrypted = true)
    {
        std::string textStr;
        RETURN_IF_NOT_OK(ReadFileToString(path, textStr));
        Raii cleanStr([&textStr] {
            while (!textStr.empty()) {
                textStr.pop_back();
            }
        });

        if (isTextEncrypted) {
            std::unique_ptr<char[]> text;
            int textSize;
            RETURN_IF_NOT_OK(SecretManager::Instance()->Decrypt(textStr, text, textSize));
            value = SensitiveValue(std::move(text), textSize);
            return Status::OK();
        }

        value = SensitiveValue(textStr);
        return Status::OK();
    }

    std::string addresses_;
    std::unique_ptr<typename T::Stub> stub_;
    tbb::concurrent_hash_map<void *, bool> contextMap_;
    tbb::concurrent_hash_map<void *, void *> cqMap_;
    MetricsBlockingVector requestStatusVec_;
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_KVSTORE_ETCD_GRPC_SESSION_H
