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

#include <gtest/gtest.h>

#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "datasystem/common/urma_mock/registry/import_endpoint_registry.h"

namespace {

namespace fs = std::filesystem;
constexpr uint64_t TEST_TOKEN = 7;
constexpr uint64_t TEST_REMOTE_VA = 0x1234000;
constexpr uint64_t TEST_SEGMENT_LEN = 4096;
constexpr int TEST_ENDPOINT_PORT = 12345;

std::string ReadFile(const fs::path &path)
{
    std::ifstream in(path);
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

fs::path FindSourceRootFrom(fs::path path)
{
    path = fs::absolute(path);
    if (fs::is_regular_file(path)) {
        path = path.parent_path();
    }
    while (!path.empty()) {
        auto candidate = path / "tests/support/datasystem/common/urma_mock";
        auto cmake = candidate / "CMakeLists.txt";
        if (fs::is_directory(candidate) && ReadFile(cmake).find("abi/mock_abi.cpp") != std::string::npos) {
            return path;
        }
        auto parent = path.parent_path();
        if (parent == path) {
            break;
        }
        path = parent;
    }
    return {};
}

fs::path FindSourceRoot()
{
    std::vector<fs::path> candidates = { fs::path(__FILE__), fs::current_path() };
    if (fs::exists("/proc/self/exe")) {
        candidates.emplace_back(fs::read_symlink("/proc/self/exe"));
    }

    for (const auto &candidate : candidates) {
        auto root = FindSourceRootFrom(candidate);
        if (!root.empty()) {
            return root;
        }
    }
    return {};
}

std::vector<fs::path> SourceFiles(const fs::path &dir)
{
    std::vector<fs::path> files;
    if (!fs::is_directory(dir)) {
        return files;
    }
    for (const auto &entry : fs::recursive_directory_iterator(dir)) {
        if (!entry.is_regular_file()) {
            continue;
        }
        auto ext = entry.path().extension().string();
        if (ext == ".h" || ext == ".cpp") {
            files.emplace_back(entry.path());
        }
    }
    return files;
}

void ExpectNoPattern(const fs::path &dir, const std::vector<std::string> &patterns)
{
    ASSERT_FALSE(dir.empty());
    for (const auto &file : SourceFiles(dir)) {
        auto content = ReadFile(file);
        for (const auto &pattern : patterns) {
            EXPECT_EQ(content.find(pattern), std::string::npos)
                << "Forbidden include pattern '" << pattern << "' found in " << file;
        }
    }
}

}  // namespace

TEST(UrmaMockStructureTest, SourceDoesNotUseOldHardwareMockPath)
{
    auto root = FindSourceRoot();
    ASSERT_FALSE(root.empty());
    ExpectNoPattern(root / "tests/support/datasystem/common/urma_mock", { "datasystem/common/hardware_mock", "hardware_mock" });
}

TEST(UrmaMockStructureTest, SourceUsesAbiCompatName)
{
    auto root = FindSourceRoot();
    ASSERT_FALSE(root.empty());
    ExpectNoPattern(root / "tests/support/datasystem/common/urma_mock", { "urma_api_compat" });
}

TEST(UrmaMockStructureTest, CMakeDoesNotReferenceParentMockSourceDirs)
{
    auto root = FindSourceRoot();
    ASSERT_FALSE(root.empty());
    auto cmake = ReadFile(root / "tests/support/datasystem/common/urma_mock/CMakeLists.txt");
    EXPECT_EQ(cmake.find("../"), std::string::npos);
}

TEST(UrmaMockStructureTest, UsesUrmaSemanticModuleDirs)
{
    auto root = FindSourceRoot();
    ASSERT_FALSE(root.empty());
    auto cmake = ReadFile(root / "tests/support/datasystem/common/urma_mock/CMakeLists.txt");
    for (const auto &source :
         { "abi/mock_abi.cpp", "transport/uds_endpoint_service.cpp", "registry/import_endpoint_registry.cpp",
           "segment/segment_memory.cpp", "segment/segment_view.cpp", "segment/segment_identity.cpp",
           "inject/fault_inject.cpp", "objects/mock_context.cpp", "post_send/post_send_wr.cpp" }) {
        EXPECT_NE(cmake.find(source), std::string::npos) << source;
    }
    EXPECT_EQ(cmake.find("runtime/"), std::string::npos);
}

TEST(UrmaMockStructureTest, SegmentHelpersStayInSegmentModule)
{
    auto root = FindSourceRoot();
    ASSERT_FALSE(root.empty());
    auto mockRoot = root / "tests/support/datasystem/common/urma_mock";
    EXPECT_TRUE(fs::exists(mockRoot / "segment/segment_view.h"));
    EXPECT_TRUE(fs::exists(mockRoot / "segment/segment_view.cpp"));
    EXPECT_TRUE(fs::exists(mockRoot / "segment/segment_identity.h"));
    EXPECT_TRUE(fs::exists(mockRoot / "segment/segment_identity.cpp"));
    EXPECT_FALSE(fs::exists(mockRoot / "urma_segment_resolver.h"));
    EXPECT_FALSE(fs::exists(mockRoot / "urma_segment_resolver.cpp"));
    EXPECT_FALSE(fs::exists(mockRoot / "segment/segment_resolver.h"));
    EXPECT_FALSE(fs::exists(mockRoot / "segment/segment_resolver.cpp"));

    auto transportService = ReadFile(mockRoot / "transport/uds_endpoint_service.cpp");
    EXPECT_EQ(transportService.find("std::string BuildSegmentEndpointKey"), std::string::npos);
}

TEST(UrmaMockStructureTest, DependencyDirectionMatchesLayering)
{
    auto root = FindSourceRoot();
    ASSERT_FALSE(root.empty());
    auto mockRoot = root / "tests/support/datasystem/common/urma_mock";
    ExpectNoPattern(mockRoot / "transport", { "datasystem/worker/", "datasystem/client/", "object_cache", "kv_cache",
                                              "datasystem/common/urma_mock/post_send" });
    ExpectNoPattern(mockRoot / "segment", { "datasystem/common/urma_mock/post_send" });
    ExpectNoPattern(mockRoot / "post_send", { "datasystem/common/urma_mock/transport" });
    ExpectNoPattern(root / "src/datasystem/common/rdma",
                    { "datasystem/common/urma_mock/transport", "datasystem/common/urma_mock/registry",
                      "SegmentEndpointRegistry", "UdsEndpointService" });
}

TEST(UrmaMockStructureTest, PostSendKeepsTransferSnapshotInternal)
{
    auto root = FindSourceRoot();
    ASSERT_FALSE(root.empty());
    auto mockRoot = root / "tests/support/datasystem/common/urma_mock";
    EXPECT_FALSE(fs::exists(mockRoot / "post_send/one_sided_transfer.h"));
    EXPECT_FALSE(fs::exists(mockRoot / "post_send/one_sided_transfer.cpp"));

    auto postSendHeader = ReadFile(mockRoot / "post_send/post_send_wr.h");
    EXPECT_EQ(postSendHeader.find("OneSidedTransfer"), std::string::npos);
    EXPECT_EQ(postSendHeader.find("PostSendSnapshot"), std::string::npos);
}

TEST(UrmaMockStructureTest, BackendDoesNotSelectSegmentImportStrategy)
{
    auto root = FindSourceRoot();
    ASSERT_FALSE(root.empty());
    auto backendOps = ReadFile(root / "tests/support/datasystem/common/urma_mock/urma_mock_backend_ops.cpp");
    EXPECT_EQ(backendOps.find("ImportViaEndpoint"), std::string::npos);
    EXPECT_EQ(backendOps.find("ImportLocal"), std::string::npos);
    EXPECT_EQ(backendOps.find("ImportPosix"), std::string::npos);
    EXPECT_EQ(backendOps.find("HasRegisteredImportEndpoint"), std::string::npos);
}

TEST(ImportEndpointRegistryTest, SupportsTokenClientAndRemoteVaLookup)
{
    using namespace datasystem::urma_mock;
    ImportEndpointRegistry::Instance().Clear();

    ImportEndpoint ep;
    ep.instanceId = "instance-a";
    ep.clientId = "client-a";
    ep.va = TEST_REMOTE_VA;
    ep.len = TEST_SEGMENT_LEN;
    ep.host = "127.0.0.1";
    ep.port = TEST_ENDPOINT_PORT;

    ImportEndpointRegistry::Instance().Register(TEST_TOKEN, ep);
    EXPECT_EQ(ImportEndpointRegistry::Instance().Lookup(TEST_TOKEN).instanceId, "instance-a");
    EXPECT_EQ(ImportEndpointRegistry::Instance().Lookup(TEST_TOKEN, ep.va).port, TEST_ENDPOINT_PORT);

    ImportEndpoint clientEp = ep;
    clientEp.instanceId = "instance-b";
    ImportEndpointRegistry::Instance().Register(TEST_TOKEN, "client-b", clientEp);
    EXPECT_EQ(ImportEndpointRegistry::Instance().Lookup(TEST_TOKEN, "client-b").instanceId, "instance-b");
    EXPECT_TRUE(ImportEndpointRegistry::Instance().Lookup(TEST_TOKEN, "missing-client").instanceId.empty());

    ImportEndpointRegistry::Instance().Clear();
    EXPECT_TRUE(ImportEndpointRegistry::Instance().Lookup(TEST_TOKEN).instanceId.empty());
}
