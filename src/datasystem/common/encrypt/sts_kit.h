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
 * Description: encrypt  management by sts
 */
#ifndef DATASYSTEM_COMMON_ENCRYPT_STS_KIT_H
#define DATASYSTEM_COMMON_ENCRYPT_STS_KIT_H

#include "datasystem/common/encrypt/encrypt_kit.h"

namespace datasystem {
class StsKit : public EncryptKit {
public:
    StsKit();
    ~StsKit() override;

    Status LoadSecretKeys() override;

    Status GenerateRootKey() override;

    Status Decrypt(const std::string &cipher, std::unique_ptr<char[]> &plainText, int &outSize) override;

    Status Encrypt(const std::string &plaintText, std::string &cipher) override;

    Status DestroyKeyFactor() override;

    Status DestroyRootKey() override;

    bool CheckPreCondition() override;

    bool IsActive() override
    {
        return sdkLoaded_;
    }

    Status GenerateAllKeyComponent(const std::string &k1Path, const std::string &k2Path, const std::string &k3Path,
                                   const std::string &saltPath) override;

    Status GetPKCS12Passphrase(std::unique_ptr<char[]> &passphrase, int &outSize) override;

    Status GetPKCS12Path(std::string &stsP12Path) override;

private:
    /**
     * @brief get sts cache folder, the folder will cache the sts kek json file.
     * @param[out] cacheFolder cache folder is ~/.sts/
     * @return Status of the call.
     */
    Status GetStsCacheFolder(std::string &cacheFolder);

    bool sdkLoaded_{ false };
};
}  // namespace datasystem

#endif  // DATASYSTEM_COMMON_ENCRYPT_STS_KIT_H
