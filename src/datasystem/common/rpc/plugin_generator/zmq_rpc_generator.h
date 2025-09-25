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
 * Description: Zmq Plugin Generator.
 */
#ifndef DATASYSTEM_COMMON_RPC_ZMQ_PLUGIN_H
#define DATASYSTEM_COMMON_RPC_ZMQ_PLUGIN_H
#include <map>
#include <memory>

#include <google/protobuf/compiler/code_generator.h>
#include <google/protobuf/compiler/plugin.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/printer.h>
#include <google/protobuf/io/zero_copy_stream.h>

#include "datasystem/common/util/strings_util.h"
#include "datasystem/protos/rpc_option.pb.h"

namespace compiler = google::protobuf::compiler;
namespace io = google::protobuf::io;

/**
 * Custom plugin for ZMQ. Used by protoc
 */
namespace datasystem {
static std::unordered_set<std::string> CRITICAL_FUNCTIONS{ "Publish",    "Get",       "Create",
                                                           "CreateMeta", "QueryMeta", "GetObjectRemote" };
static std::unordered_set<std::string> CRITICAL_ASYNC_FUNCTIONS{ "GetObjectRemote" };
static const std::string exchangeJfrMethodName = "ExchangeJfr";

class ZmqRpcGenerator : public compiler::CodeGenerator {
public:
    explicit ZmqRpcGenerator(bool stubHeaderOnly = false) : stubHeaderOnly(stubHeaderOnly){};
    ~ZmqRpcGenerator() override = default;

    /**
     * @brief This method implement the abstract method to parse the Service descriptor and generates two
     * files. See datasystem/common/rpc/zmq/demo/README.md for details.
     * @param[in] file Pb FileDescriptor.
     * @param[in] parameter Pb parameter.
     * @param[in] generatorCtx Pb compiler context.
     * @param[out] error Error msg.
     * @return True if successful.
     */
    bool Generate(const google::protobuf::FileDescriptor *file, const std::string &parameter,
                  compiler::GeneratorContext *generatorCtx, std::string *error) const override;

private:
    /**
     * @brief Strip the suffix from a file name.
     * @param[in,out] filename Name of the file.
     * @param[in] suffix Suffix to be stripped.
     * @return True if successful.
     */
    static bool StripSuffix(std::string &filename, const std::string &suffix)
    {
        auto n = filename.find_last_of(suffix);
        if (n != std::string::npos && (n + 1) == filename.length()) {
            filename.resize(filename.size() - suffix.size());
            return true;
        }
        return false;
    }

    /**
     * @brief Detect if a service has a method that override the channel number
     * @param svc
     * @return the override channel number. 0 otherwise
     */
    static uint64_t HasChannelOption(const google::protobuf::ServiceDescriptor &svc);

    /**
     * @brief Detect if a stub requires multi session support
     * @param svc
     * @return true when multi_session_option is set to 1. false otherwise
     */
    static bool MultiSessionEnabled(const google::protobuf::ServiceDescriptor &svc);

    /**
     * @brief Detect if the method enables urma and need urma related support
     * @param svc
     * @return true when urma_enabled_option is set to 1. false otherwise
     */
    static bool UrmaEnabled(const google::protobuf::MethodDescriptor &method);

    /**
     * @brief Detect if a method has a payload option.
     * @param[in] method Pb method.
     * @return True if has payload option.
     */
    static bool HasPayloadSendOption(const google::protobuf::MethodDescriptor &method);
    static bool HasPayloadRecvOption(const google::protobuf::MethodDescriptor &method);

    /**
     * @brief Detect if the method requires us to expose the unary zmq socket.
     * @param[in] method Pb method.
     * @return True if needed unary socket.
     */
    static bool UnarySocketNeeded(const google::protobuf::MethodDescriptor &method);

    /**
     * @brief Append a suffix to the method name for generating RpcServiceMethod subclass.
     * @param[in] methodName Method name string.
     * @return The method name for generating the subclass.
     */
    static std::string MethodSvcClassName(const std::string &methodName);

    /**
     * @brief Generate the subclass inherited from RpcServiceMethod.
     * @param[out] output Code output stream.
     * @param[in] svc Pb Service Descriptor.
     * @param[in] indent Indent chars.
     */
    static void GenerateMethodClass(io::Printer &printer, const google::protobuf::ServiceDescriptor &svc);

    /**
     * @brief Generate the method used in the constructor of both Service and Stub classes.
     * @param[out] output Code output stream.
     * @param[in] svc Pb Service Descriptor.
     * @param[in] indent Indent chars.
     */
    static void GenerateInitMethodMapDecl(io::Printer &printer);
    static void GenerateInitMethodMapDef(io::Printer &printer, const google::protobuf::ServiceDescriptor &svc,
                                         const std::string &indent, const std::string &stub);

    /**
     * @brief It lists all the virtual functions that the Impl class must provide.
     * @param[out] output Code output stream.
     * @param[in] svc Pb Service Descriptor.
     * @param[in] indent Indent chars.
     */
    static void ListVirtualFunctions(io::Printer &printer, const google::protobuf::ServiceDescriptor &svc);

    /**
     * @brief Implement the virtual function Call Method.
     * @param[out] output Code output stream.
     * @param[in] svc Pb Service Descriptor.
     * @param[in] indent Indent chars.
     */
    static void ImplementZmqCallMethodDecl(io::Printer &printer);
    static void ImplementZmqCallMethodDef(io::Printer &printer, const google::protobuf::ServiceDescriptor &svc,
                                          const std::string &indent, const std::string &svcName);
    static void ImplementZmqDirectCallMethodDef(io::Printer &printer, const google::protobuf::ServiceDescriptor &svc,
                                                const std::string &indent, const std::string &svcName);
    static void ImplementCallMethodNoStream(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                            int methodIndex, const std::string &indent, bool enableMsgQ = true);
    static void ImplementCallMethodClientStream(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                                int methodIndex, const std::string &indent);
    static void ImplementCallMethodServerStream(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                                int methodIndex, const std::string &indent);
    static void ImplementCallMethodStream(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                          int methodIndex, const std::string &indent);
    static void ImplementCallMethodUnarySocket(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                               int methodIndex, const std::string &indent, bool enableMsgQ = true);

    /**
     * @brief Implement stub api for both sides streaming.
     * @param[out] output Code output stream.
     * @param[in] method Pb method descriptor.
     * @param[in] methodIndex Method index.
     * @param[in] indent Indent chars.
     */
    static void ImplementStubStreamingDecl(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                           const std::string &indent);
    static void ImplementStubStreamingDef(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                          int methodIndex, const std::string &indent, const std::string &stub);

    /**
     * @brief Implement stub api for client streaming. Server side isn't streaming.
     * @param[out] output Code output stream.
     * @param[in] method Pb method descriptor.
     * @param[in] methodIndex Method index.
     * @param[in] indent Indent chars.
     */
    static void ImplementStubClientStreamingDecl(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                                 const std::string &indent);
    static void ImplementStubClientStreamingDef(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                                int methodIndex, const std::string &indent, const std::string &stub);

    /**
     * @brief Implement the stub api for only server side is streaming. Client side isn't streaming.
     * @param[out] output Code output stream.
     * @param[in] method Pb method descriptor.
     * @param[in] methodIndex Method index.
     * @param[in] indent Indent chars.
     */
    static void ImplementStubServerStreamingDecl(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                                 const std::string &indent);
    static void ImplementStubServerStreamingDef(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                                int methodIndex, const std::string &indent, const std::string &stub);

    /**
     * @brief Implement the stub async write api. No side is streaming.
     * @param[out] output Code output stream.
     * @param[in] method Pb method descriptor.
     * @param[in] methodIndex Method index.
     * @param[in] indent Indent chars.
     */
    static void ImplementStubAsyncWriteDecl(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                            const std::string &indent);
    static void ImplementStubAsyncWriteDef(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                           int methodIndex, const std::string &indent, const std::string &stub);

    /**
     * @brief Implement the stub api for async read. No side is streaming.
     * @param[out] output Code output stream.
     * @param[in] method Pb method descriptor.
     * @param[in] methodIndex Method index.
     * @param[in] indent Indent chars.
     */
    static void ImplementStubAsyncReadDecl(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                           const std::string &indent);

    static void ImplementStubAsyncReadDef(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                          int methodIndex, const std::string &indent, const std::string &stub);

    /**
     * @brief Implement the stub api for sync write/read. No side is streaming.
     * @param[out] output Code output stream.
     * @param[in] method Pb method descriptor.
     * @param[in] methodIndex Method index.
     * @param[in] indent Indent chars.
     */
    static void ImplementStubNoStreamDecl(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                          const std::string &indent);
    static void ImplementStubNoStreamDecl2(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                           const std::string &indent);
    static void ImplementStubNoStreamDef(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                         int methodIndex, const std::string &indent, const std::string &stub);
    static void ImplementStubNoStreamDefHelper(std::string &impl, const google::protobuf::MethodDescriptor &method);
    static void ImplementStubNoStreamDef2(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                          int methodIndex, const std::string &indent, const std::string &stub);
    static void ImplStubNoStreamShortDecl(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                          const std::string &indent);
    static void ImplStubNoStreamShortDef(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                         int methodIndex, const std::string &indent, const std::string &stub);

    /**
     * @brief Implement all the Stub API.
     * @param[out] output Code output stream.
     * @param[in] svc Pb Service Descriptor.
     * @param[in] indent Indent chars.
     */
    static void ImplementStubApiDecl(io::Printer &printer, const google::protobuf::ServiceDescriptor &svc,
                                     const std::string &indent);

    static void ImplementGenericStubApiDef(io::Printer &printer, const google::protobuf::ServiceDescriptor &svc,
                                           const std::string &indent);

    static void ImplementStubApiDef(io::Printer &printer, const google::protobuf::ServiceDescriptor &svc,
                                    const std::string &indent);
    static void ImplementGenericStubConstructor(io::Printer &printer, const google::protobuf::ServiceDescriptor &svc,
                                                const std::string &indent);
    static void ImplementGenericStubOtherFuncDecl(io::Printer &printer);
    static void ImplementGenericStubOtherFuncDef(io::Printer &printer, const std::string &stub);

    static void GetSessionHelper(const google::protobuf::MethodDescriptor &method, std::string &impl);
    static void ImplementGenericStubNoStreamDef(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                                const std::string &indent, const std::string &stub);
    static void ImplementGenericStubNoStreamDef2(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                                 const std::string &indent, const std::string &stub);
    static void ImplGenericStubNoStreamShortDef(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                                const std::string &indent, const std::string &stub);
    static void ImplementGenericStubAsyncWriteDef(io::Printer &printer,
                                                  const google::protobuf::MethodDescriptor &method,
                                                  const std::string &indent, const std::string &stub);
    static void ImplementGenericStubAsyncReadDef(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                                 const std::string &indent, const std::string &stub);
    static void ImplementGenericStubStreamingDef(io::Printer &printer, const google::protobuf::MethodDescriptor &method,
                                                 const std::string &indent, const std::string &stub);
    static void ImplementGenericStubClientStreamingDef(io::Printer &printer,
                                                       const google::protobuf::MethodDescriptor &method,
                                                       const std::string &indent, const std::string &stub);
    static void ImplementGenericStubServerStreamingDef(io::Printer &printer,
                                                       const google::protobuf::MethodDescriptor &method,
                                                       const std::string &indent, const std::string &stub);
    /**
     * @brief Generate prolog and include.
     * @param[out] output Code output stream.
     * @param[in] file Pb FileDescriptor.
     */
    void GenerateServicePrologue(io::Printer &printer, const google::protobuf::FileDescriptor &file) const;
    void GenerateServiceCppPrologue(io::Printer &printer, const google::protobuf::FileDescriptor &file) const;
    void GenerateStubPrologue(io::Printer &printer, const google::protobuf::FileDescriptor &file) const;

    /**
     * @brief Full package Service name
     * @param[out] output Code output stream.
     * @param[in] svcName Pb Service Descriptor.
     * @param[in] indent Indent chars.
     */
    void GenerateSvcName(io::Printer &printer, const std::string &svcName, const std::string &indent,
                         bool isOverride = true) const;

    /**
     * @brief Generate the abstract Service class.
     * @param[out] output Code output stream.
     * @param[in] svc Pb Service Descriptor.
     * @param[in] indent Indent chars.
     */
    void GenerateServiceClass(io::Printer &printer, const google::protobuf::ServiceDescriptor &svc,
                              const std::string &indent) const;

    /**
     * @brief Generate the Generic Stub class for zmq.
     * @param[out] output Code output stream.
     * @param[in] svc Pb Service Descriptor.
     * @param[in] indent Indent chars.
     */
    void GenerateGenericStubClass(io::Printer &printer, const google::protobuf::ServiceDescriptor &svc,
                                  const std::string &indent) const;

    /**
     * @brief Generate the Stub class.
     * @param[out] output Code output stream.
     * @param[in] svc Pb Service Descriptor.
     * @param[in] indent Indent chars.
     */
    void GenerateStubClass(io::Printer &printer, const google::protobuf::ServiceDescriptor &svc,
                           const std::string &indent) const;

    /**
     * @brief Error checking. Some options can't mix together.
     * @param[in] file Pb FileDescriptor.
     * @param[out] error Error message string.
     * @return
     */
    static bool ErrorChecking(const google::protobuf::FileDescriptor &file, std::string &error);

    /**
     * @brief Generate the header file.
     * @param[in] file Pb FileDescriptor.
     * @param[in] generatorCtx Pb compiler context.
     */
    void CreateStubHeader(const google::protobuf::FileDescriptor &file, compiler::GeneratorContext *generatorCtx) const;
    void CreateServiceHeader(const google::protobuf::FileDescriptor &file,
                             compiler::GeneratorContext *generatorCtx) const;

    /**
     * @brief Generate the cpp file.
     * @param[in] file Pb FileDescriptor.
     * @param[in] generatorCtx Pb compiler context.
     */
    void CreateStubCpp(const google::protobuf::FileDescriptor &file, compiler::GeneratorContext *generatorCtx) const;
    void CreateServiceCpp(const google::protobuf::FileDescriptor &file, compiler::GeneratorContext *generatorCtx) const;

    /**
     * @brief Parse the name space of the proto file.
     * @param[in] file Pb FileDescriptor.
     */
    void ParseNameSpace(const google::protobuf::FileDescriptor &file) const;
    std::string UseNameSpace(const std::string &packageName) const;

    /**
     * @brief Instantiate template classes
     * @param[in] file Pb FileDescriptor.
     */
    void InstantiateTemplate(io::Printer &printer, const google::protobuf::FileDescriptor &file) const;
    void InstantiateService(io::Printer &printer, const google::protobuf::ServiceDescriptor &svc) const;

    static std::string OptionalPayload(const google::protobuf::MethodDescriptor &method, const std::string &prefix,
                                       const std::string &pauloadSend, const std::string &payloadRecv,
                                       const std::string &suffix);

    static const std::string PREFIX;
    static const std::string ENDIF;
    mutable std::string namespaceBegin;
    mutable std::string namespaceEnd;
    mutable std::string fileName;
    mutable std::string packageName;
    const bool stubHeaderOnly;
};

}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_RPC_ZMQ_PLUGIN_H
