/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
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
 * Description: encrypt management by sts
 */
#include "datasystem/common/encrypt/sts_kit.h"

#ifdef BUILD_STS_KIT
#include <iostream>
#include <pwd.h>

#include "datasystem/common/flags/flags.h"

#include "sts/Sts.h"
#include "sts/api/StsClient.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/uri.h"
#include "datasystem/common/util/validator.h"

DS_DEFINE_string(sts_server_domain, "", "the address of sts server.");
DS_DEFINE_string(sts_config_path, "", "microservice sts config file path.");

/**
 * sts_loader_local normally is false, means sts sdk connect to sts server to load sts's encrypt info, not load
 * locally.
 *
 * when set true, sts sdk will load sts's encrypt info from local, it is only use in the scenario that sts server
 * unavailable for a very long time, that datasystem can be started independently from the sts server.
 *
 * by the way, the precondition that the sts sdk can be load sts's encrypt info locally is that the sts sdk must connect
 * at least once to sts server before.
 */
DS_DEFINE_bool(sts_loader_local, false, "init sts from local cache info.");

DS_DEFINE_validator(sts_server_domain, &Validator::ValidateHostPortIPv4);
DS_DEFINE_validator(sts_config_path, &Validator::ValidateRealPath);

namespace datasystem {

/**
 * sts check log level whether is enable callback function
 * @param[in] logLevel log level
 * @return true if log level enable, otherwise false
 */
static bool StsCheckLogLevelCallBack(::Sts::LogLevel logLevel)
{
    // enable log level for sts sdk, corresponding to LOG_DEBUG, LOG_INFO, LOG_WARN, LOG_ERROR, LOG_FATAL
    static bool logSwitch[] = { false, true, true, true, true };
    return logSwitch[(size_t)logLevel];
}

/**
 * sts sdk log callback function
 * @param[in] msg sts sdk log msg
 */
static void StsWriteLogCallBack(const std::string &msg)
{
    LOG(INFO) << msg;
}

StsKit::StsKit() : EncryptKit()
{
}

StsKit::~StsKit() noexcept
{
}

Status StsKit::LoadSecretKeys()
{
    ::Sts::LoggerRegistry::GetInstance().InitLogger(StsCheckLogLevelCallBack, StsWriteLogCallBack);

    std::string cacheFolder;
    RETURN_IF_NOT_OK(GetStsCacheFolder(cacheFolder));

    ::Sts::Config conf(FLAGS_sts_server_domain, FLAGS_sts_config_path, cacheFolder, FLAGS_sts_loader_local);
    auto retCode = ::Sts::ClientManager::getInstance().init(conf);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(retCode == ::Sts::RetCode::Success, K_RUNTIME_ERROR,
                                         FormatString("Failed to init sts sdk, sts sdk return code %d.", retCode));

    sdkLoaded_ = true;
    return Status::OK();
}

Status StsKit::GenerateRootKey()
{
    return Status::OK();
}

Status StsKit::Decrypt(const std::string &cipher, std::unique_ptr<char[]> &plainText, int &outSize)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(!cipher.empty(), K_INVALID, "The cipher text can't be empty.");
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(sdkLoaded_, K_RUNTIME_ERROR, "The sts sdk has not been initialized.");

    std::vector<unsigned char> decryptContent;
    auto res = ::Sts::ClientManager::getInstance().getStsClient()->DecryptSensitiveConfig(cipher, decryptContent);
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(res == ::Sts::RetCode::Success, K_RUNTIME_ERROR,
                                         FormatString("Sts sdk failed to decrypt, sts sdk return code %d.", res));

    outSize = decryptContent.size();
    plainText = std::make_unique<char[]>(outSize + 1);
    int ret = memcpy_s(plainText.get(), outSize + 1, decryptContent.data(), outSize);
    CHECK_FAIL_RETURN_STATUS(ret == EOK, StatusCode::K_RUNTIME_ERROR,
                             FormatString("Copy text failed, the memcpy_s return: %d", ret));
    ret = memset_s(decryptContent.data(), sizeof(decryptContent.data()), 0, sizeof(decryptContent.data()));
    CHECK_FAIL_RETURN_STATUS(ret == EOK, StatusCode::K_RUNTIME_ERROR,
                             FormatString("Memset failed, the memset_s return: %d", ret));
    return Status::OK();
}

Status StsKit::Encrypt(const std::string &plaintText, std::string &cipher)
{
    (void)plaintText;
    (void)cipher;
    RETURN_STATUS_LOG_ERROR(K_INVALID, "Invalid invoke, sts sdk not support encrypt now.");
}

Status StsKit::DestroyKeyFactor()
{
    return Status::OK();
}

Status StsKit::DestroyRootKey()
{
    return Status::OK();
}

bool StsKit::CheckPreCondition()
{
    return !FLAGS_sts_server_domain.empty()
           && Validator::ValidateHostPortIPv4("sts_server_domain", FLAGS_sts_server_domain)
           && !FLAGS_sts_config_path.empty() && Validator::ValidateRealPath("sts_config_path", FLAGS_sts_config_path);
}

Status StsKit::GenerateAllKeyComponent(const std::string &k1Path, const std::string &k2Path, const std::string &k3Path,
                                       const std::string &saltPath)
{
    (void)k1Path;
    (void)k2Path;
    (void)k3Path;
    (void)saltPath;
    RETURN_STATUS_LOG_ERROR(K_INVALID, "Invalid invoke, sts kit not support this operation.");
}

Status StsKit::GetPKCS12Passphrase(std::unique_ptr<char[]> &passphrase, int &outSize)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(sdkLoaded_, K_RUNTIME_ERROR, "The sts sdk has not been initialized.");
    std::string pass = ::Sts::ClientManager::getInstance().getStsClient()->GetPassphrase();

    outSize = pass.size();
    passphrase = std::make_unique<char[]>(outSize + 1);
    int ret = memcpy_s(passphrase.get(), outSize + 1, pass.data(), outSize);
    CHECK_FAIL_RETURN_STATUS(ret == EOK, StatusCode::K_RUNTIME_ERROR,
                             FormatString("Get sts passphrase memcpy failed, the memcpy_s return: %d", ret));

    ClearStr(pass);
    return Status::OK();
}

Status StsKit::GetPKCS12Path(std::string &stsP12Path)
{
    CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(sdkLoaded_, K_RUNTIME_ERROR, "The sts sdk has not been initialized.");
    stsP12Path = ::Sts::ClientManager::getInstance().getStsClient()->GetKeyStorePath();
    return Status::OK();
}

Status StsKit::GetStsCacheFolder(std::string &cacheFolder)
{
    RETURN_IF_NOT_OK_PRINT_ERROR_MSG(Uri::GetHomeDir(cacheFolder),
                                     FormatString("Get sts cache folder failed, errno is %d", errno));
    cacheFolder.append("/.sts");
    return Status::OK();
}

}  // namespace datasystem
#endif