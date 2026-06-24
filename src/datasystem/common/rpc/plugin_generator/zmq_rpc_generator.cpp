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
 * Description: Unified datasystem protoc plugin entry point.
 *
 * Despite the "zmq" prefix in the filename, this file is the single protoc
 * plugin executable that generates ALL RPC framework adapters:
 * - ZMQ stubs (legacy)
 * - brpc adapters + stubs (CreateBrpcAdapterHeader/Cpp + CreateBrpcStubHeader/Cpp)
 * - IRPC pure virtual interfaces (CreateInterfaceHeader)
 *
 * The "three-output pattern" (V8) generates ZMQ + brpc + IRPC in one pass so
 * proto changes propagate to all frameworks atomically. File rename to
 * rpc_generator.cpp or splitting brpc logic to a separate file is tracked
 * as follow-up #7 in docs/brpc-pr-a-followup-items.md.
 */
#include "datasystem/common/rpc/plugin_generator/zmq_rpc_generator.h"

namespace datasystem {

const std::string ZmqRpcGenerator::PREFIX = "    ";
const std::string ZmqRpcGenerator::ENDIF = "#endif\n";

bool ZmqRpcGenerator::Generate(const google::protobuf::FileDescriptor *file, const std::string &parameter,
                               compiler::GeneratorContext *generatorCtx, std::string *error) const
{
    (void)parameter;
    if (error == nullptr || file == nullptr || generatorCtx == nullptr) {
        return false;
    }
    // Early exit for conflicting options.
    bool rc = ErrorChecking(*file, *error);
    if (!rc) {
        return rc;
    }

    fileName = file->name();
    StripSuffix(fileName, ".proto");

    // Parse the name space if any.
    ParseNameSpace(*file);

    // Generate the stub header file.
    CreateStubHeader(*file, generatorCtx);

    if (stubHeaderOnly) {
        return true;
    }

    // Generate the service header file.
    CreateServiceHeader(*file, generatorCtx);
    // Generate the stub cpp file.
    CreateStubCpp(*file, generatorCtx);
    // Generate the service cpp file.
    CreateServiceCpp(*file, generatorCtx);

    // --- V8: three-output pattern ---
    // Generate the IRPC pure virtual interface (.irpc.pb.h).
    CreateInterfaceHeader(*file, generatorCtx);
    // Generate the brpc adapter header (.brpc.pb.h).
    CreateBrpcAdapterHeader(*file, generatorCtx);
    // Generate the brpc adapter cpp (.brpc.pb.cc).
    CreateBrpcAdapterCpp(*file, generatorCtx);
    // Generate the brpc client stub header (.brpc.stub.pb.h).
    CreateBrpcStubHeader(*file, generatorCtx);
    // Generate the brpc client stub cpp (.brpc.stub.pb.cc).
    CreateBrpcStubCpp(*file, generatorCtx);
    return true;
}

void ZmqRpcGenerator::ParseNameSpace(const google::protobuf::FileDescriptor &file) const
{
    packageName = file.package();
    if (!packageName.empty()) {
        size_t pos = 0;
        do {
            size_t n = packageName.find('.', pos);
            if (n != std::string::npos) {
                namespaceBegin.append("namespace " + packageName.substr(pos, n - pos) + " {\n");
                namespaceEnd.append("} // namespace " + packageName.substr(pos, n - pos) + "\n");
                pos = n + 1;
            } else {
                namespaceBegin.append("namespace " + packageName.substr(pos) + " {\n");
                namespaceEnd.append("} // namespace " + packageName.substr(pos) + "\n");
                pos = n;
            }
        } while (pos != std::string::npos);
    }
}

std::string ZmqRpcGenerator::UseNameSpace(const std::string &packageName) const
{
    std::stringstream oss;
    oss << "using namespace ";
    size_t pos = 0;
    do {
        size_t n = packageName.find('.', pos);
        if (n != std::string::npos) {
            oss << packageName.substr(pos, n - pos) << "::";
            pos = n + 1;
        } else {
            oss << packageName.substr(pos) << ";\n";
            pos = n;
        }
    } while (pos != std::string::npos);
    return oss.str();
}

bool ZmqRpcGenerator::ErrorChecking(const google::protobuf::FileDescriptor &file, std::string &error)
{
    if (file.options().cc_generic_services()) {
        error =
            "cpp zmq proto compiler plugin does not work with generic "
            "services. To generate cpp zmq APIs, please set \""
            "cc_generic_service = false\".";
        return false;
    }
    for (auto i = 0; i < file.service_count(); ++i) {
        auto *svc = file.service(i);
        for (auto j = 0; j < svc->method_count(); ++j) {
            auto *method = svc->method(j);
            // If we need unary socket option, then neither request/response method can be set as 'stream'.
            if (UnarySocketNeeded(*method) && (method->client_streaming() || method->server_streaming())) {
                error = "unary_socket_option can not mix with stream support.";
                return false;
            }
        }
    }
    return true;
}

void ZmqRpcGenerator::GenerateSvcName(io::Printer &printer, const std::string &svcName, const std::string &indent,
                                      bool isOverride) const
{
    // Implement the override function ServiceName.
    std::map<std::string, std::string> vars;
    vars["indent"] = indent;
    vars["svc_name"] = svcName;
    vars["package"] = packageName;
    vars["override"] = isOverride ? "override " : "";
    vars["static"] = isOverride ? "" : "static ";
    vars["const"] = isOverride ? "const " : "";
    printer.Print(
        vars, "$indent$$static$std::string FullServiceName() $const$$override${ return \"$package$.$svc_name$\"; }\n");
    printer.Print(vars, "$indent$$static$std::string ServiceName() $const$$override${ return \"$svc_name$\"; }\n");
}

bool ZmqRpcGenerator::HasPayloadSendOption(const google::protobuf::MethodDescriptor &method)
{
    auto &options = method.options();
    auto val = options.GetExtension(datasystem::send_payload_option);
    return val;
}

bool ZmqRpcGenerator::UnarySocketNeeded(const google::protobuf::MethodDescriptor &method)
{
    auto &options = method.options();
    return options.GetExtension(datasystem::unary_socket_option);
}

std::string ZmqRpcGenerator::MethodSvcClassName(const std::string &methodName)
{
    const std::string methodSuffix("SvcMethod");
    return methodName + methodSuffix;
}

void ZmqRpcGenerator::GenerateInitMethodMapDecl(io::Printer &printer)
{
    printer.PrintRaw("    void InitMethodMap();\n");
}

void ZmqRpcGenerator::GenerateInitMethodMapDef(io::Printer &printer, const google::protobuf::ServiceDescriptor &svc,
                                               const std::string &indent, const std::string &stub)
{
    std::map<std::string, std::string> vars;
    vars["indent1"] = indent;
    vars["stub"] = stub;
    printer.Print(vars, "void $stub$::InitMethodMap() {\n");
    for (auto j = 0; j < svc.method_count(); ++j) {
        if (svc.method(j) == nullptr) {
            continue;
        }
        auto &method = *(svc.method(j));
        std::string methodObj = "methodObj" + std::to_string(j);
        vars["methodObj"] = methodObj;
        vars["className"] = MethodSvcClassName(method.name());
        vars["methodIndex"] = std::to_string(j);
        std::string impl =
            "$indent1$std::shared_ptr<::datasystem::RpcServiceMethod> $methodObj$ = std::make_shared<$className$>();\n"
            "$indent1$$methodObj$->Init();\n"
            "$indent1$methodMap_.insert({$methodIndex$, std::move($methodObj$)});\n";
        printer.Print(vars, impl.c_str());
    }
    printer.PrintRaw("}\n");
}

bool ZmqRpcGenerator::HasPayloadRecvOption(const google::protobuf::MethodDescriptor &method)
{
    auto &options = method.options();
    auto val = options.GetExtension(datasystem::recv_payload_option);
    return val;
}

std::string ZmqRpcGenerator::OptionalPayload(const google::protobuf::MethodDescriptor &method,
                                             const std::string &prefix, const std::string &payloadSend,
                                             const std::string &payloadRecv, const std::string &suffix)
{
    std::string impl = prefix;
    if (HasPayloadSendOption(method)) {
        impl += payloadSend;
    }
    if (HasPayloadRecvOption(method)) {
        impl += payloadRecv;
    }
    impl += suffix;
    return impl;
}

uint64_t ZmqRpcGenerator::HasChannelOption(const google::protobuf::ServiceDescriptor &svc)
{
    // If one of the methods has a payload, assign it a different channel
    bool hasPayload = false;
    for (auto i = 0; i < svc.method_count(); ++i) {
        auto *m = svc.method(i);
        if (HasPayloadRecvOption(*m) || HasPayloadSendOption(*m)) {
            hasPayload = true;
            break;
        }
    }
    uint64_t defaultChannel = hasPayload ? 1 : 0;
    // Now check if the user wants an explicit channel number.
    auto &options = svc.options();
    uint64_t val = options.GetExtension(datasystem::channel_number_option);
    return std::max<uint64_t>(val, defaultChannel);
}

bool ZmqRpcGenerator::MultiSessionEnabled(const google::protobuf::ServiceDescriptor &svc)
{
    return svc.options().GetExtension(datasystem::multi_session_option);
}

bool ZmqRpcGenerator::UrmaEnabled(const google::protobuf::MethodDescriptor &method)
{
    return method.options().GetExtension(datasystem::urma_enabled_option);
}

// --- V8: IRPC pure virtual interface ---

void ZmqRpcGenerator::CreateInterfaceHeader(const google::protobuf::FileDescriptor &file,
                                            compiler::GeneratorContext *generatorCtx) const
{
    std::unique_ptr<io::ZeroCopyOutputStream> outputFile(generatorCtx->Open(fileName + ".irpc.pb.h"));
    io::Printer printer(outputFile.get(), '$');

    GenerateInterfacePrologue(printer, file);
    printer.PrintRaw(namespaceBegin);

    for (auto i = 0; i < file.service_count(); ++i) {
        auto *svc = file.service(i);
        GenerateInterface(printer, *svc, PREFIX);
    }

    printer.PrintRaw(namespaceEnd);
    printer.PrintRaw(ENDIF);
}

void ZmqRpcGenerator::GenerateInterfacePrologue(io::Printer &printer,
                                                const google::protobuf::FileDescriptor &file) const
{
    std::map<std::string, std::string> vars;
    vars["full_file_name"] = file.name();
    vars["file_name"] = fileName;
    std::string fileNameHeader = fileName;
    std::replace(fileNameHeader.begin(), fileNameHeader.end(), '/', '_');
    vars["file_name_header"] = fileNameHeader;
    std::string impl =
        "// Generated by the protocol buffer compiler.  DO NOT EDIT!\n"
        "// source: $full_file_name$\n"
        "#ifndef DATASYSTEM_PROTO_IRPC_$file_name_header$_INTERFACE_H\n"
        "#define DATASYSTEM_PROTO_IRPC_$file_name_header$_INTERFACE_H\n"
        "#include <memory>\n"
        "#include <vector>\n";
    if (file.message_type_count() > 0) {
        impl += "#include \"$file_name$.pb.h\"\n";
    }
    impl +=
        "#include \"datasystem/common/rpc/rpc_server_stream_base.h\"\n"
        "#include \"datasystem/common/rpc/rpc_message.h\"\n"
        "#include \"datasystem/utils/status.h\"\n";
    printer.Print(vars, impl.c_str());
    for (auto k = 0; k < file.dependency_count(); ++k) {
        auto depend = file.dependency(k)->name();
        bool isAProto = StripSuffix(depend, ".proto");
        if (isAProto) {
            vars["depend"] = depend;
            printer.Print(vars, "#include \"$depend$.pb.h\"\n");
        }
    }
    printer.PrintRaw("#include <string>\n");
}

void ZmqRpcGenerator::GenerateInterface(io::Printer &printer, const google::protobuf::ServiceDescriptor &svc,
                                        const std::string &indent) const
{
    (void)indent;
    const std::string &svcName = svc.name();
    std::map<std::string, std::string> vars;
    vars["svc_name"] = svcName;
    vars["interface_name"] = "I" + svcName;

    std::string impl =
        "class $interface_name$ {\n"
        "public:\n"
        "    virtual ~$interface_name$() = default;\n";
    printer.Print(vars, impl.c_str());

    // Generate the pure virtual method signatures (reuse ListVirtualFunctions pattern).
    printer.PrintRaw("    // Service methods\n");
    ListVirtualFunctions(printer, svc);

    printer.PrintRaw("};\n");
}

}  // namespace datasystem
