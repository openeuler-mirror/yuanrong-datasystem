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
 * Description: Add and close socket fd.
 */

#ifndef DATASYSTEM_FD_MANAGER_H
#define DATASYSTEM_FD_MANAGER_H

#include <map>
#include <memory>
#include <mutex>
#include <shared_mutex>
#include <unordered_set>

#include "datasystem/common/util/status_helper.h"

namespace datasystem {
class FdManager {
public:
    /**
     * @brief Singleton mode, obtaining instance.
     * @return Reference of ClientManager.
     */
    static FdManager *Instance();

    /**
     * @brief Passkey idiom.
     * @note This ensure no one can call the FdManager constructor
     */
    class Token {
    public:
        ~Token() = default;

    private:
        friend FdManager *FdManager::Instance();
        Token() = default;
    };

    explicit FdManager(Token);
    ~FdManager();

    /**
     * @brief Add client fd.
     * @param[in] socketFd Socket fd of the client.
     * @return Status of the call.
     */
    Status AddFd(int socketFd);

    /**
     * @brief Close client socketFd.
     * @param[in] socketFd Socket fd of the client.
     * @return Status of the call.
     */
    Status CloseFd(int socketFd);

    /**
     * @brief Close all client socketFds.
     * @return Status of the call.
     */
    Status CloseAllFds();

    /**
     * @brief Shutdown a (tcp/uds) socket and track its last shutdown time
     * @param[in] fd
     */
    void ShutdownAndTrack(int fd);

    /**
     * @brief When a (tcp/uds) socket is reused by OS, check its last shutdown time
     * @param[in] fd
     * @param[in] threshold in millisecond
     * @param[in] sleepMS. Sleep time in  millisecond if last shutdown time is within the threshold
     * @return If safe to reuse. Otherwise sleep at least threshold number of ms
     */
    Status WaitForReuse(int fd, int threshold, int sleepMS);

private:
    static std::once_flag init_;
    static std::unique_ptr<FdManager> instance_;
    std::mutex mutex_;
    std::unordered_set<int> fds_;
    std::map<int, uint64_t> reuse_;
};
}  // namespace datasystem

#endif  // DATASYSTEM_FD_MANAGER_H
