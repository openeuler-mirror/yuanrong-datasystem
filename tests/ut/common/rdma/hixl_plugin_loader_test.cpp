/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "datasystem/common/rdma/npu/hixl_plugin_loader.h"

#include <dlfcn.h>
#include <fcntl.h>
#include <unistd.h>

#include <fstream>
#include <iomanip>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include <gtest/gtest.h>

#include "datasystem/common/ak_sk/hasher.h"

namespace datasystem {
namespace {

constexpr uint32_t FAKE_MODE_VALID = 0;
constexpr uint32_t FAKE_MODE_REJECT_ABI = 1;
constexpr uint32_t FAKE_MODE_BAD_ABI_VERSION = 2;
constexpr uint32_t FAKE_MODE_SHORT_API_TABLE = 3;
constexpr uint32_t FAKE_MODE_NULL_REQUIRED_FUNCTION = 4;

using FakeSetMode = void (*)(uint32_t);
using FakeReset = void (*)();
using FakeGetCallCount = uint32_t (*)();

std::string Sha256(const std::string &path)
{
    std::ifstream file(path, std::ios::binary);
    EXPECT_TRUE(file.is_open());
    std::ostringstream stream;
    stream << file.rdbuf();
    std::string content = stream.str();
    std::unique_ptr<unsigned char[]> digest;
    unsigned int digestSize = 0;
    EXPECT_TRUE(Hasher().HashSHA256(content.data(), content.size(), digest, digestSize).IsOk());
    std::ostringstream result;
    for (unsigned int i = 0; i < digestSize; ++i) {
        result << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(digest[i]);
    }
    return result.str();
}

class HixlPluginLoaderTest : public testing::Test {
protected:
    void SetUp() override
    {
        handle_ = dlopen(FAKE_HIXL_PLUGIN_PATH, RTLD_NOW | RTLD_LOCAL);
        ASSERT_NE(handle_, nullptr) << dlerror();
        setMode_ = reinterpret_cast<FakeSetMode>(dlsym(handle_, "FakeHixlSetMode"));
        reset_ = reinterpret_cast<FakeReset>(dlsym(handle_, "FakeHixlReset"));
        getCallCount_ = reinterpret_cast<FakeGetCallCount>(dlsym(handle_, "FakeHixlGetApiCallCount"));
        ASSERT_NE(setMode_, nullptr);
        ASSERT_NE(reset_, nullptr);
        ASSERT_NE(getCallCount_, nullptr);
        reset_();
        expectedHash_ = Sha256(FAKE_HIXL_PLUGIN_PATH);
    }

    void TearDown() override
    {
        if (handle_ != nullptr) {
            dlclose(handle_);
        }
    }

    void *handle_ = nullptr;
    FakeSetMode setMode_ = nullptr;
    FakeReset reset_ = nullptr;
    FakeGetCallCount getCallCount_ = nullptr;
    std::string expectedHash_;
};

TEST_F(HixlPluginLoaderTest, LoadsOnceForConcurrentCallers)
{
    setMode_(FAKE_MODE_VALID);
    HixlPluginLoader loader(FAKE_HIXL_PLUGIN_PATH, expectedHash_);
    constexpr size_t THREAD_COUNT = 16;
    std::vector<Status> statuses(THREAD_COUNT);
    std::vector<const DsHixlApi *> apis(THREAD_COUNT, nullptr);
    std::vector<std::thread> threads;
    threads.reserve(THREAD_COUNT);
    for (size_t i = 0; i < THREAD_COUNT; ++i) {
        threads.emplace_back([&loader, &statuses, &apis, i]() { statuses[i] = loader.GetApi(apis[i]); });
    }
    for (auto &thread : threads) {
        thread.join();
    }
    for (size_t i = 0; i < THREAD_COUNT; ++i) {
        EXPECT_TRUE(statuses[i].IsOk()) << statuses[i].ToString();
        EXPECT_NE(apis[i], nullptr);
        EXPECT_EQ(apis[i], apis[0]);
    }
    EXPECT_EQ(getCallCount_(), 1u);
}

TEST_F(HixlPluginLoaderTest, RejectsMissingAndHashMismatch)
{
    const DsHixlApi *api = reinterpret_cast<const DsHixlApi *>(static_cast<uintptr_t>(1));
    HixlPluginLoader missingLoader("/tmp/datasystem-hixl-plugin-does-not-exist.so", expectedHash_);
    Status missingStatus = missingLoader.GetApi(api);
    EXPECT_EQ(missingStatus.GetCode(), K_NOT_SUPPORTED);
    EXPECT_EQ(api, nullptr);

    HixlPluginLoader mismatchLoader(FAKE_HIXL_PLUGIN_PATH, std::string(64, '0'));
    Status mismatchStatus = mismatchLoader.GetApi(api);
    EXPECT_EQ(mismatchStatus.GetCode(), K_NOT_AUTHORIZED);
    EXPECT_EQ(api, nullptr);
    EXPECT_EQ(getCallCount_(), 0u);
}

TEST_F(HixlPluginLoaderTest, RejectsInvalidElfAfterIntegrityCheck)
{
    char path[] = "/tmp/datasystem-hixl-invalid-XXXXXX";
    int fd = mkstemp(path);
    ASSERT_GE(fd, 0);
    constexpr char INVALID_ELF[] = "not-an-elf";
    ASSERT_EQ(write(fd, INVALID_ELF, sizeof(INVALID_ELF)), static_cast<ssize_t>(sizeof(INVALID_ELF)));
    ASSERT_EQ(close(fd), 0);

    const DsHixlApi *api = nullptr;
    HixlPluginLoader loader(path, Sha256(path));
    Status status = loader.GetApi(api);
    EXPECT_EQ(status.GetCode(), K_NOT_SUPPORTED);
    EXPECT_EQ(api, nullptr);
    EXPECT_EQ(unlink(path), 0);
}

class InvalidApiModeTest : public HixlPluginLoaderTest, public testing::WithParamInterface<uint32_t> {
};

TEST_P(InvalidApiModeTest, RejectsInvalidApi)
{
    setMode_(GetParam());
    HixlPluginLoader loader(FAKE_HIXL_PLUGIN_PATH, expectedHash_);
    const DsHixlApi *api = nullptr;
    Status status = loader.GetApi(api);
    EXPECT_EQ(status.GetCode(), K_NOT_SUPPORTED);
    EXPECT_EQ(api, nullptr);
    Status cachedStatus = loader.GetApi(api);
    EXPECT_EQ(cachedStatus.GetCode(), K_NOT_SUPPORTED);
    EXPECT_EQ(api, nullptr);
    EXPECT_EQ(getCallCount_(), 1u);
}

INSTANTIATE_TEST_SUITE_P(AllInvalidApiModes, InvalidApiModeTest,
                         testing::Values(FAKE_MODE_REJECT_ABI, FAKE_MODE_BAD_ABI_VERSION,
                                         FAKE_MODE_SHORT_API_TABLE, FAKE_MODE_NULL_REQUIRED_FUNCTION));

}  // namespace
}  // namespace datasystem
