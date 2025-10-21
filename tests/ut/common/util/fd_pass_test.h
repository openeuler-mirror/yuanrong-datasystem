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
 * Description: fd pass test.
 */
#include "datasystem/common/rpc/unix_sock_fd.h"
#include "datasystem/common/util/fd_pass.h"

#include <gtest/gtest.h>
#include <sys/mman.h>
#include <sys/poll.h>
#include <unistd.h>

#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <thread>

#include "common.h"
#include "datasystem/common/util/status_helper.h"
#include "securec.h"

namespace datasystem {
namespace ut {
constexpr const char* SOCKET_PATH = "/tmp/shm_transfer.sock";
constexpr const char* SHM_CONTENT = "Uzi is known as little Gala";

class SocketClient {
public:
    SocketClient() = default;
    ~SocketClient()
    {
        RETRY_ON_EINTR(close(serverFd_));
    };

    void Connect()
    {
        serverFd_ = socket(AF_UNIX, SOCK_STREAM, 0);
        ASSERT_GE(serverFd_, 0);
        struct sockaddr_un addr;
        ASSERT_EQ(memset_s(&addr, sizeof(addr), 0, sizeof(addr)), EOK);
        addr.sun_family = AF_UNIX;
        ASSERT_EQ(strncpy_s(addr.sun_path, sizeof(addr.sun_path) - 1, SOCKET_PATH, sizeof(addr.sun_path) - 1), EOK);
        ASSERT_EQ(connect(serverFd_, (struct sockaddr*)&addr, sizeof(addr)), 0);
        int waitTimeSec = 2;
        sleep(waitTimeSec);  // wait server accept
    }
    
    int32_t GetServerFd() const
    {
        return serverFd_;
    }

private:
    std::string sockPath_;
    int32_t serverFd_ = -1;
};

class SocketServer {
public:
    SocketServer() = default;

    ~SocketServer()
    {
        stop_ = true;
        if (acceptTrd_.joinable()) {
            acceptTrd_.join();
        }
        RETRY_ON_EINTR(close(clientFd_));
        RETRY_ON_EINTR(close(listenFd_));
    }

    void Init()
    {
        // bind socket fd.
        listenFd_ = socket(AF_UNIX, SOCK_STREAM, 0);
        ASSERT_GE(listenFd_, 0);
        (void)unlink(SOCKET_PATH);
        struct sockaddr_un addr;
        ASSERT_EQ(memset_s(&addr, sizeof(addr), 0, sizeof(addr)), EOK);
        addr.sun_family = AF_UNIX;
        ASSERT_EQ(strncpy_s(addr.sun_path, sizeof(addr.sun_path) - 1, SOCKET_PATH, sizeof(addr.sun_path) - 1), EOK);
        ASSERT_EQ(bind(listenFd_, (struct sockaddr *)&addr, sizeof(addr)), 0);
        ASSERT_EQ(listen(listenFd_, 1), 0);

        // create shm fd.
        std::string fileTemplate = "/dev/shm/test-XXXXXX";
        std::vector<char> fileName(fileTemplate.begin(), fileTemplate.end());
        fileName.push_back('\0');
        fileFd_ = mkstemp(&fileName[0]);
        ASSERT_GE(fileFd_, 0);
        LOG(INFO) << "File descriptor: " << fileFd_;
        ASSERT_EQ(unlink(&fileName[0]), 0);
        ASSERT_EQ(ftruncate(fileFd_, (off_t)1024), 0);
        char *space = (char *)mmap(nullptr, 1024, PROT_READ | PROT_WRITE, MAP_SHARED, fileFd_, 0);
        ASSERT_EQ(memcpy_s(space, 1024, SHM_CONTENT, strlen(SHM_CONTENT)), EOK);

        // accept client connection.
        acceptTrd_ = std::thread(std::bind(&SocketServer::Start, this));
    }

    int GetClientFd() const
    {
        return clientFd_;
    }

    int GetFileFd() const
    {
        return fileFd_;
    }

private:
    void Start()
    {
        while (!stop_) {
            struct pollfd pfd {
                .fd = listenFd_, .events = POLLIN, .revents = 0
            };
            int n = poll(&pfd, 1, RPC_POLL_TIME);
            if (n <= 0) {
                continue;
            }
            int fd = accept(listenFd_, nullptr, 0);
            if (fd > 0) {
                clientFd_ = fd;
            }
        }
    }

    std::thread acceptTrd_;
    std::atomic<int> clientFd_ = -1;
    int listenFd_ = -1;
    std::atomic<bool> stop_ = false;
    int fileFd_ = -1;
};

class FdPassTest : public CommonTest {
public:
    void SetUp() override
    {
        StartServer();
    }

    void TearDown() override
    {
    }

    void StartServer()
    {
        server_ = std::make_unique<SocketServer>();
        server_->Init();
    }

protected:
    std::unique_ptr<SocketServer> server_;
    const uint64_t requestId_ = 1;
};
}  // namespace ut
}  // namespace datasystem
