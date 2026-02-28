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
 * Description: Helper functions to load curve public/private key pair.
 */
#include "datasystem/common/rpc/rpc_auth_key_manager.h"

#include <nlohmann/json.hpp>
#include <securec.h>

#include "datasystem/common/encrypt/secret_manager.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/raii.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/validator.h"

DS_DECLARE_bool(enable_curve_zmq);
DS_DECLARE_string(curve_key_dir);

namespace datasystem {
const std::unordered_map<std::string, RawSvcToKeyMapping> DEFAULT_SERVICE_MAPPINGS{
    { WORKER_SERVER_NAME,
      { { "WorkerService", { "client.key", "worker.key" } },
        { "WorkerOCService", { "client.key", "worker.key" } },
        { "WorkerWorkerOCService", { "worker.key" } },
        { "WorkerWorkerSCService", { "worker.key" } },
        { "WorkerWorkerTransportService", { "worker.key" } },
        { "ClientWorkerSCService", { "client.key", "worker.key" } },
        { "MasterWorkerSCService", { "master.key", "worker.key" } },
        { "MasterWorkerOCService", { "master.key", "worker.key" } },
        { "GenericService", { "client.key" } },
        { "MasterService", { "worker.key" } },
        { "MasterOCService", { "worker.key" } },
        { "MasterSCService", { "worker.key" } } } }
};

static Status LoadKey(const std::string &filePath, std::unique_ptr<char[]> &keyContent)
{
    // keyContent is ciphertext.
    std::ifstream ifs(filePath);
    if (!ifs.is_open()) {
        RETURN_STATUS(StatusCode::K_IO_ERROR, "Cannot open file");
    }
    std::stringstream buffer;
    buffer << ifs.rdbuf();
    ifs.close();
    std::string keyString = buffer.str();
    Raii clearRaii([&keyString]() { ClearStr(keyString); });
    if (FLAGS_enable_curve_zmq) {
        // Decrypt ciphertext once.
        int keyContentSize;
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(SecretManager::Instance()->Decrypt(keyString, keyContent, keyContentSize),
                                         "Decrypt ciphertext failed!");
    }
    return Status::OK();
}

static Status LoadPublicKey(const std::string &keyFilePrefix, std::unique_ptr<char[]> &keyContent)
{
    VLOG(RPC_LOG_LEVEL) << "Loading public key from file " << std::endl;
    return LoadKey(keyFilePrefix + PUBLIC_KEY_EXT, keyContent);
}

static Status LoadPrivateKey(const std::string &keyFilePrefix, std::unique_ptr<char[]> &keyContent)
{
    VLOG(RPC_LOG_LEVEL) << "Loading private key from file " << std::endl;
    return LoadKey(keyFilePrefix + PRIVATE_KEY_EXT, keyContent);
}

static Status LoadAuthorizedKeys(const std::string &serverName, std::vector<std::unique_ptr<char[]>> &keyContent,
                                 SvcToKeyMapping &svcMapping)
{
    std::vector<std::string> filePaths;
    std::unordered_map<std::string, const char *> pathToKeyMapping;
    std::string filePathSuffix = FLAGS_curve_key_dir + "/" + serverName + AUTHORIZED_DIR_SUFFIX + "/";
    auto pathPattern = filePathSuffix + "*" + PUBLIC_KEY_EXT;
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::ValidatePathString("pathPattern", pathPattern), K_INVALID,
                                         "Check path failed");
    RETURN_IF_NOT_OK(Glob(pathPattern, filePaths));
    for (const auto &filePath : filePaths) {
        std::unique_ptr<char[]> clientPublicKey;
        VLOG(RPC_LOG_LEVEL) << "Loading authorized clients" << std::endl;
        RETURN_IF_NOT_OK(LoadKey(filePath, clientPublicKey));
        (void)pathToKeyMapping.emplace(filePath, clientPublicKey.get());
        keyContent.emplace_back(std::move(clientPublicKey));
    }
    VLOG(RPC_LOG_LEVEL) << "Load the service name to public key mapping";
    RawSvcToKeyMapping rawSvcMapping;
    std::ifstream svcMappingIfs(filePathSuffix + SVC_MAPPING_FILE_NAME);
    if (!svcMappingIfs.is_open()) {
        VLOG(RPC_LOG_LEVEL) << "Cannot open service.mapping file, use the default mapping";
        rawSvcMapping = DEFAULT_SERVICE_MAPPINGS.find(serverName)->second;
    } else {
        nlohmann::json mapping = nlohmann::json::parse(svcMappingIfs, nullptr, false);
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!mapping.is_discarded(), StatusCode::K_INVALID, "Parse json error");
        rawSvcMapping = mapping.get<RawSvcToKeyMapping>();
    }
    for (auto &kv : rawSvcMapping) {
        AuthKeySet keySet;
        for (auto &filename : kv.second) {
            auto iter = pathToKeyMapping.find(filePathSuffix + filename);
            if (iter != pathToKeyMapping.end()) {
                keySet.emplace(iter->second);
            } else {
                LOG(WARNING) << filename << " on the service mapping is not found";
            }
        }
        svcMapping.emplace(kv.first, std::move(keySet));
    }
    return Status::OK();
}

static Status LoadServerKeysHelper(const std::string &serverName)
{
    // Load server keys if possible and store to RpcAuthKeyManager singleton for later.
    RpcAuthKeys authKeys = {};
    for (const auto &server : SERVER_TYPES) {
        std::unique_ptr<char[]> serverPublicKey;
        std::string filePathPrefix = FLAGS_curve_key_dir + "/" + server;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(Validator::ValidatePathString("filePathPrefix", filePathPrefix), K_INVALID,
                                             "Check path failed");
        Status rc = LoadPublicKey(filePathPrefix, serverPublicKey);
        if (rc.IsOk()) {
            if (server == serverName) {
                // The server component might need to connect to other components,
                // so copy the server keys as client keys here.
                std::unique_ptr<char[]> clientPublicKey;
                RETURN_IF_NOT_OK(RpcAuthKeyManager::CopyCurveAuthKey(serverPublicKey.get(), clientPublicKey));
                RETURN_IF_NOT_OK(authKeys.SetClientPublicKey(clientPublicKey));
                std::unique_ptr<char[]> clientPrivateKey;
                RETURN_IF_NOT_OK(LoadPrivateKey(filePathPrefix, clientPrivateKey));
                RETURN_IF_NOT_OK(authKeys.SetClientPrivateKey(clientPrivateKey));
            }
            RETURN_IF_NOT_OK(authKeys.SetServerKey(server, serverPublicKey));
        } else if (server == serverName) {
            RETURN_STATUS_LOG_ERROR(rc.GetCode(), rc.ToString());
        }
    }
    RpcAuthKeyManager::Instance().SetKeys(authKeys);
    return Status::OK();
}

Status RpcAuthKeyManager::ServerLoadKeys(const std::string &serverName, RpcCredential &cred)
{
    if (!FLAGS_enable_curve_zmq) {
        return Status::OK();
    }
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(SERVER_TYPES.find(serverName) != SERVER_TYPES.end(), K_INVALID,
                                         "Invalid server component name");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!FLAGS_curve_key_dir.empty(), K_INVALID, "curve_key_dir must be set!");

    RETURN_IF_NOT_OK(LoadServerKeysHelper(serverName));

    // Set server private key
    const RpcAuthKeys &authKeys = RpcAuthKeyManager::Instance().GetKeys();
    cred.SetAuthCurveServer(authKeys.GetClientPrivateKey());

    // Load public keys for all authorized clients from file
    // and allow clients with these public keys to connect.
    // And also load the service name to public key mapping for whitelisting.
    auto authHandler = std::make_unique<ZmqAuthHandler>();
    std::vector<std::unique_ptr<char[]>> authorizedClients;
    SvcToKeyMapping svcMapping;
    RETURN_IF_NOT_OK(LoadAuthorizedKeys(serverName, authorizedClients, svcMapping));
    for (const auto &clientPublicKey : authorizedClients) {
        authHandler->ConfigCurve(clientPublicKey.get());
    }
    RpcAuthKeyManager::Instance().SetAuthHandler(authHandler);
    RpcAuthKeyManager::Instance().SetSvcMapping(svcMapping);
    RpcAuthKeyManager::Instance().SetAuthorizedClients(authorizedClients);
    FLAGS_curve_key_dir.clear();
    return Status::OK();
}

void RpcAuthKeyManager::SetAuthHandler(std::unique_ptr<datasystem::ZmqAuthHandler> &authHandler)
{
    authHandler_ = std::move(authHandler);
}

bool RpcAuthKeyManager::HasAuthHandler()
{
    return authHandler_ != nullptr;
}

Status RpcAuthKeyManager::InitAuthHandler(const std::shared_ptr<datasystem::ZmqContext> &ctx)
{
    return authHandler_->Init(ctx);
}

Status RpcAuthKeyManager::StartAuthHandler()
{
    RETURN_IF_EXCEPTION_OCCURS(thrdPool_ = std::make_unique<ThreadPool>(1, 0, "RpcAuth"));
    auto func = [this] {
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG(authHandler_->WorkerEntry(), "ZmqAuthHandler WorkerEntry failed");
        return Status::OK();
    };
    authHandlerThrd_ = std::make_unique<std::future<Status>>(thrdPool_->Submit(func));
    return Status::OK();
}

void RpcAuthKeyManager::StopAuthHandler()
{
    authHandler_->Stop();
    if (authHandlerThrd_) {
        authHandlerThrd_->wait();
        authHandlerThrd_.reset();
    }
    authHandler_.reset();
}

void RpcAuthKeyManager::SetSvcMapping(SvcToKeyMapping &svcMapping)
{
    svcMapping_ = std::move(svcMapping);
}

const SvcToKeyMapping &RpcAuthKeyManager::GetSvcMapping()
{
    return svcMapping_;
}
}  // namespace datasystem
