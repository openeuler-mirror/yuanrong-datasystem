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
 * Description: Test interface for file system.
 */
#include <cstdlib>
#include <thread>
#include <vector>
#include <regex>

#include <fcntl.h>
#include <unistd.h>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/uri.h"

#include "common.h"

DS_DECLARE_string(log_dir);

namespace datasystem {
namespace ut {
class UriTest : public CommonTest {};

TEST_F(UriTest, TestCopyConstructor)
{
    LOG(INFO) << "Test Uri copy constructor";
    {
        Uri uri("/path/to/file");
        Uri another = uri;
        EXPECT_EQ(uri.Path(), another.Path());
        EXPECT_EQ(uri.GetFileName(), another.GetFileName());
        EXPECT_EQ(uri.Validate().GetCode(), another.Validate().GetCode());
        EXPECT_EQ(uri.ValidateFile().GetCode(), another.ValidateFile().GetCode());
        EXPECT_EQ(uri.GetParent(), another.GetParent());
    }
    {
        Uri uri("/path/to/dir/");
        Uri another = uri;
        EXPECT_EQ(uri.Path(), another.Path());
        EXPECT_EQ(uri.GetFileName(), another.GetFileName());
        EXPECT_EQ(uri.Validate().GetCode(), another.Validate().GetCode());
        EXPECT_EQ(uri.ValidateFile().GetCode(), another.ValidateFile().GetCode());
        EXPECT_EQ(uri.GetParent(), another.GetParent());
    }
    {
        Uri uri("/path/to/dir/.");
        Uri another = uri;
        EXPECT_EQ(uri.Path(), another.Path());
        EXPECT_EQ(uri.GetFileName(), another.GetFileName());
        EXPECT_EQ(uri.Validate().GetCode(), another.Validate().GetCode());
        EXPECT_EQ(uri.ValidateFile().GetCode(), another.ValidateFile().GetCode());
        EXPECT_EQ(uri.GetParent(), another.GetParent());
    }
    {
        Uri uri("/path/to/dir/./");
        Uri another = uri;
        EXPECT_EQ(uri.Path(), another.Path());
        EXPECT_EQ(uri.GetFileName(), another.GetFileName());
        EXPECT_EQ(uri.Validate().GetCode(), another.Validate().GetCode());
        EXPECT_EQ(uri.ValidateFile().GetCode(), another.ValidateFile().GetCode());
        EXPECT_EQ(uri.GetParent(), another.GetParent());
    }
    {
        Uri uri("/path/to/dir/..");
        Uri another = uri;
        EXPECT_EQ(uri.Path(), another.Path());
        EXPECT_EQ(uri.GetFileName(), another.GetFileName());
        EXPECT_EQ(uri.Validate().GetCode(), another.Validate().GetCode());
        EXPECT_EQ(uri.ValidateFile().GetCode(), another.ValidateFile().GetCode());
        EXPECT_EQ(uri.GetParent(), another.GetParent());
    }
    {
        Uri uri("/path/to/dir/../");
        Uri another = uri;
        EXPECT_EQ(uri.Path(), another.Path());
        EXPECT_EQ(uri.GetFileName(), another.GetFileName());
        EXPECT_EQ(uri.Validate().GetCode(), another.Validate().GetCode());
        EXPECT_EQ(uri.ValidateFile().GetCode(), another.ValidateFile().GetCode());
        EXPECT_EQ(uri.GetParent(), another.GetParent());
    }
}

TEST_F(UriTest, TestMoveConstructor)
{
    LOG(INFO) << "Test Uri move constructor";
    {
        Uri uri("/path/to/file");
        auto path = uri.Path();
        auto filename = uri.GetFileName();
        auto validCode = uri.Validate().GetCode();
        auto validFileCode = uri.ValidateFile().GetCode();
        auto parent = uri.GetParent();
        Uri another = std::move(uri);
        EXPECT_EQ(path, another.Path());
        EXPECT_EQ(filename, another.GetFileName());
        EXPECT_EQ(validCode, another.Validate().GetCode());
        EXPECT_EQ(validFileCode, another.ValidateFile().GetCode());
        EXPECT_EQ(parent, another.GetParent());
    }
    {
        Uri uri("/path/to/dir");
        auto path = uri.Path();
        auto filename = uri.GetFileName();
        auto validCode = uri.Validate().GetCode();
        auto validFileCode = uri.ValidateFile().GetCode();
        auto parent = uri.GetParent();
        Uri another = std::move(uri);
        EXPECT_EQ(path, another.Path());
        EXPECT_EQ(filename, another.GetFileName());
        EXPECT_EQ(validCode, another.Validate().GetCode());
        EXPECT_EQ(validFileCode, another.ValidateFile().GetCode());
        EXPECT_EQ(parent, another.GetParent());
    }
    {
        Uri uri("/path/to/dir/");
        auto path = uri.Path();
        auto filename = uri.GetFileName();
        auto validCode = uri.Validate().GetCode();
        auto validFileCode = uri.ValidateFile().GetCode();
        auto parent = uri.GetParent();
        Uri another = std::move(uri);
        EXPECT_EQ(path, another.Path());
        EXPECT_EQ(filename, another.GetFileName());
        EXPECT_EQ(validCode, another.Validate().GetCode());
        EXPECT_EQ(validFileCode, another.ValidateFile().GetCode());
        EXPECT_EQ(parent, another.GetParent());
    }
    {
        Uri uri("/path/to/dir/.");
        auto path = uri.Path();
        auto filename = uri.GetFileName();
        auto validCode = uri.Validate().GetCode();
        auto validFileCode = uri.ValidateFile().GetCode();
        auto parent = uri.GetParent();
        Uri another = std::move(uri);
        EXPECT_EQ(path, another.Path());
        EXPECT_EQ(filename, another.GetFileName());
        EXPECT_EQ(validCode, another.Validate().GetCode());
        EXPECT_EQ(validFileCode, another.ValidateFile().GetCode());
        EXPECT_EQ(parent, another.GetParent());
    }
    {
        Uri uri("/path/to/dir/./");
        auto path = uri.Path();
        auto filename = uri.GetFileName();
        auto validCode = uri.Validate().GetCode();
        auto validFileCode = uri.ValidateFile().GetCode();
        auto parent = uri.GetParent();
        Uri another = std::move(uri);
        EXPECT_EQ(path, another.Path());
        EXPECT_EQ(filename, another.GetFileName());
        EXPECT_EQ(validCode, another.Validate().GetCode());
        EXPECT_EQ(validFileCode, another.ValidateFile().GetCode());
        EXPECT_EQ(parent, another.GetParent());
    }
    {
        Uri uri("/path/to/dir/..");
        auto path = uri.Path();
        auto filename = uri.GetFileName();
        auto validCode = uri.Validate().GetCode();
        auto validFileCode = uri.ValidateFile().GetCode();
        auto parent = uri.GetParent();
        Uri another = std::move(uri);
        EXPECT_EQ(path, another.Path());
        EXPECT_EQ(filename, another.GetFileName());
        EXPECT_EQ(validCode, another.Validate().GetCode());
        EXPECT_EQ(validFileCode, another.ValidateFile().GetCode());
        EXPECT_EQ(parent, another.GetParent());
    }
    {
        Uri uri("/path/to/dir/../");
        auto path = uri.Path();
        auto filename = uri.GetFileName();
        auto validCode = uri.Validate().GetCode();
        auto validFileCode = uri.ValidateFile().GetCode();
        auto parent = uri.GetParent();
        Uri another = std::move(uri);
        EXPECT_EQ(path, another.Path());
        EXPECT_EQ(filename, another.GetFileName());
        EXPECT_EQ(validCode, another.Validate().GetCode());
        EXPECT_EQ(validFileCode, another.ValidateFile().GetCode());
        EXPECT_EQ(parent, another.GetParent());
    }
}

TEST_F(UriTest, TestCopyAssignment)
{
    LOG(INFO) << "Test Uri copy assignment";
    {
        Uri uri("/path/to/file");
        Uri another("/xxx");
        another = uri;
        EXPECT_EQ(uri.Path(), another.Path());
        EXPECT_EQ(uri.GetFileName(), another.GetFileName());
        EXPECT_EQ(uri.Validate().GetCode(), another.Validate().GetCode());
        EXPECT_EQ(uri.ValidateFile().GetCode(), another.ValidateFile().GetCode());
        EXPECT_EQ(uri.GetParent(), another.GetParent());
    }
    {
        Uri uri("/path/to/dir/");
        Uri another("/xxx");
        another = uri;
        EXPECT_EQ(uri.Path(), another.Path());
        EXPECT_EQ(uri.GetFileName(), another.GetFileName());
        EXPECT_EQ(uri.Validate().GetCode(), another.Validate().GetCode());
        EXPECT_EQ(uri.ValidateFile().GetCode(), another.ValidateFile().GetCode());
        EXPECT_EQ(uri.GetParent(), another.GetParent());
    }
    {
        Uri uri("/path/to/dir/.");
        Uri another("/xxx");
        another = uri;
        EXPECT_EQ(uri.Path(), another.Path());
        EXPECT_EQ(uri.GetFileName(), another.GetFileName());
        EXPECT_EQ(uri.Validate().GetCode(), another.Validate().GetCode());
        EXPECT_EQ(uri.ValidateFile().GetCode(), another.ValidateFile().GetCode());
        EXPECT_EQ(uri.GetParent(), another.GetParent());
    }
    {
        Uri uri("/path/to/dir/./");
        Uri another("/xxx");
        another = uri;
        EXPECT_EQ(uri.Path(), another.Path());
        EXPECT_EQ(uri.GetFileName(), another.GetFileName());
        EXPECT_EQ(uri.Validate().GetCode(), another.Validate().GetCode());
        EXPECT_EQ(uri.ValidateFile().GetCode(), another.ValidateFile().GetCode());
        EXPECT_EQ(uri.GetParent(), another.GetParent());
    }
    {
        Uri uri("/path/to/dir/..");
        Uri another("/xxx");
        another = uri;
        EXPECT_EQ(uri.Path(), another.Path());
        EXPECT_EQ(uri.GetFileName(), another.GetFileName());
        EXPECT_EQ(uri.Validate().GetCode(), another.Validate().GetCode());
        EXPECT_EQ(uri.ValidateFile().GetCode(), another.ValidateFile().GetCode());
        EXPECT_EQ(uri.GetParent(), another.GetParent());
    }
    {
        Uri uri("/path/to/dir/../");
        Uri another("/xxx");
        another = uri;
        EXPECT_EQ(uri.Path(), another.Path());
        EXPECT_EQ(uri.GetFileName(), another.GetFileName());
        EXPECT_EQ(uri.Validate().GetCode(), another.Validate().GetCode());
        EXPECT_EQ(uri.ValidateFile().GetCode(), another.ValidateFile().GetCode());
        EXPECT_EQ(uri.GetParent(), another.GetParent());
    }
}

TEST_F(UriTest, TestMoveAssignment)
{
    LOG(INFO) << "Test Uri move assignment";
    {
        Uri uri("/path/to/file");
        Uri another("/xxx");
        auto path = uri.Path();
        auto filename = uri.GetFileName();
        auto validCode = uri.Validate().GetCode();
        auto validFileCode = uri.ValidateFile().GetCode();
        auto parent = uri.GetParent();
        another = std::move(uri);
        EXPECT_EQ(path, another.Path());
        EXPECT_EQ(filename, another.GetFileName());
        EXPECT_EQ(validCode, another.Validate().GetCode());
        EXPECT_EQ(validFileCode, another.ValidateFile().GetCode());
        EXPECT_EQ(parent, another.GetParent());
    }
    {
        Uri uri("/path/to/dir");
        Uri another("/xxx");
        auto path = uri.Path();
        auto filename = uri.GetFileName();
        auto validCode = uri.Validate().GetCode();
        auto validFileCode = uri.ValidateFile().GetCode();
        auto parent = uri.GetParent();
        another = std::move(uri);
        EXPECT_EQ(path, another.Path());
        EXPECT_EQ(filename, another.GetFileName());
        EXPECT_EQ(validCode, another.Validate().GetCode());
        EXPECT_EQ(validFileCode, another.ValidateFile().GetCode());
        EXPECT_EQ(parent, another.GetParent());
    }
    {
        Uri uri("/path/to/dir/");
        Uri another("/xxx");
        auto path = uri.Path();
        auto filename = uri.GetFileName();
        auto validCode = uri.Validate().GetCode();
        auto validFileCode = uri.ValidateFile().GetCode();
        auto parent = uri.GetParent();
        another = std::move(uri);
        EXPECT_EQ(path, another.Path());
        EXPECT_EQ(filename, another.GetFileName());
        EXPECT_EQ(validCode, another.Validate().GetCode());
        EXPECT_EQ(validFileCode, another.ValidateFile().GetCode());
        EXPECT_EQ(parent, another.GetParent());
    }
    {
        Uri uri("/path/to/dir/.");
        Uri another("/xxx");
        auto path = uri.Path();
        auto filename = uri.GetFileName();
        auto validCode = uri.Validate().GetCode();
        auto validFileCode = uri.ValidateFile().GetCode();
        auto parent = uri.GetParent();
        another = std::move(uri);
        EXPECT_EQ(path, another.Path());
        EXPECT_EQ(filename, another.GetFileName());
        EXPECT_EQ(validCode, another.Validate().GetCode());
        EXPECT_EQ(validFileCode, another.ValidateFile().GetCode());
        EXPECT_EQ(parent, another.GetParent());
    }
    {
        Uri uri("/path/to/dir/./");
        Uri another("/xxx");
        auto path = uri.Path();
        auto filename = uri.GetFileName();
        auto validCode = uri.Validate().GetCode();
        auto validFileCode = uri.ValidateFile().GetCode();
        auto parent = uri.GetParent();
        another = std::move(uri);
        EXPECT_EQ(path, another.Path());
        EXPECT_EQ(filename, another.GetFileName());
        EXPECT_EQ(validCode, another.Validate().GetCode());
        EXPECT_EQ(validFileCode, another.ValidateFile().GetCode());
        EXPECT_EQ(parent, another.GetParent());
    }
    {
        Uri uri("/path/to/dir/..");
        Uri another("/xxx");
        auto path = uri.Path();
        auto filename = uri.GetFileName();
        auto validCode = uri.Validate().GetCode();
        auto validFileCode = uri.ValidateFile().GetCode();
        auto parent = uri.GetParent();
        another = std::move(uri);
        EXPECT_EQ(path, another.Path());
        EXPECT_EQ(filename, another.GetFileName());
        EXPECT_EQ(validCode, another.Validate().GetCode());
        EXPECT_EQ(validFileCode, another.ValidateFile().GetCode());
        EXPECT_EQ(parent, another.GetParent());
    }
    {
        Uri uri("/path/to/dir/../");
        Uri another("/xxx");
        auto path = uri.Path();
        auto filename = uri.GetFileName();
        auto validCode = uri.Validate().GetCode();
        auto validFileCode = uri.ValidateFile().GetCode();
        auto parent = uri.GetParent();
        another = std::move(uri);
        EXPECT_EQ(path, another.Path());
        EXPECT_EQ(filename, another.GetFileName());
        EXPECT_EQ(validCode, another.Validate().GetCode());
        EXPECT_EQ(validFileCode, another.ValidateFile().GetCode());
        EXPECT_EQ(parent, another.GetParent());
    }
}

TEST_F(UriTest, TestIsRoot)
{
    LOG(INFO) << "Test Uri IsRoot function";

    std::string root = "/";
    Uri rootUri(root);
    EXPECT_TRUE(rootUri.IsRoot());

    std::string specPath = "/////";
    EXPECT_TRUE(Uri(specPath).IsRoot());

    std::string path = "/path/to";
    Uri pathUri(path);
    EXPECT_FALSE(pathUri.IsRoot());

    std::string emptyPath;
    EXPECT_FALSE(Uri(emptyPath).IsRoot());

    std::string invalidPath = "yyds:)";
    EXPECT_FALSE(Uri(invalidPath).IsRoot());
}

TEST_F(UriTest, TestGetFileName)
{
    LOG(INFO) << "Test Uri GetFileName function";

    std::string path = "/path/to/filename";
    Uri pathUri(path);
    EXPECT_EQ(pathUri.GetFileName(), "filename");

    path = "/";
    Uri rootUri(path);
    EXPECT_EQ(rootUri.GetFileName(), "/");

    path = "/path/to/filename.ext";
    Uri extUri(path);
    EXPECT_EQ(extUri.GetFileName(), "filename.ext");

    path = "/path/to/dir/";
    Uri dirUri(path);
    EXPECT_EQ(dirUri.GetFileName(), "dir");
}

TEST_F(UriTest, TestNormalizePath)
{
    LOG(INFO) << "Test Uri NormalizePath function";

    std::string path = "/path/////to//filename";
    Uri pathUri(path);
    pathUri.NormalizePath();
    EXPECT_EQ(pathUri.Path(), "/path/to/filename");

    path = "/foo/./bar/..";
    Uri path1Uri(path);
    path1Uri.NormalizePath();
    EXPECT_EQ(path1Uri.Path(), "/foo");

    path = "/foo/.///bar/../";
    Uri path2Uri(path);
    path2Uri.NormalizePath();
    EXPECT_EQ(path2Uri.Path(), "/foo");

    path = "/foo/../bar";
    Uri path3Uri(path);
    path3Uri.NormalizePath();
    EXPECT_EQ(path3Uri.Path(), "/bar");

    path = "/foo/bar/";
    Uri path4Uri(path);
    path4Uri.NormalizePath();
    EXPECT_EQ(path4Uri.Path(), "/foo/bar");

    path = "/root/..";
    Uri path5Uri(path);
    path5Uri.NormalizePath();
    EXPECT_EQ(path5Uri.Path(), "/");

    path = "/root/../";
    Uri path6Uri(path);
    path6Uri.NormalizePath();
    EXPECT_EQ(path6Uri.Path(), "/");
}

TEST_F(UriTest, TestGetSeparatePath)
{
    LOG(INFO) << "Test Uri GetSeparatePath function";

    std::string path = "/path/to/filename";
    Uri pathUri(path);
    auto res = pathUri.GetSeparatePath();
    EXPECT_EQ(res[0], "/");
    EXPECT_EQ(res[1], "path");
    EXPECT_EQ(res[2], "to");
    EXPECT_EQ(res[3], "filename");
}

TEST_F(UriTest, TestGetParent)
{
    LOG(INFO) << "Test Uri GetParent function";

    std::string path = "/path/to/filename";
    Uri pathUri(path);
    auto res = pathUri.GetParent();
    EXPECT_EQ(res, "/path/to");

    std::string root = "/";
    Uri rootUri(root);
    res = rootUri.GetParent();
    EXPECT_EQ(res, "");
}

TEST_F(UriTest, TestValidateFile)
{
    LOG(INFO) << "Test Uri ValidateFile basic function";

    std::vector<std::string> validPaths = { "/path",
                                            "/path/_to/file-name",
                                            "/path/to/filename",
                                            "/path//to/./filename",
                                            "/path//to//////filename",
                                            "/path/123",
                                            "/324",
                                            "/PATH/TO/FILE",
                                            "/PATH/to/File",
                                            "/path/中文",
                                            "/path/@a",
                                            "/path%",
                                            "/*as" };

    for (const auto &path : validPaths) {
        LOG(INFO) << "Validate the path: " << path;
        DS_EXPECT_OK(Uri(path).ValidateFile());
    }

    std::vector<std::string> invalidPaths = { "a@",       "path",     "/path/>", "/path/t:o", "/path|",
                                              "/path\\",  "/path\"",  "/path<",  "/file/",    "/path/to/dir/",
                                              "/path///", "/root/..", "/root/.", "/root/../", "/root/./" };

    for (const auto &path : invalidPaths) {
        LOG(INFO) << "Invalidate the path: " << path;
        DS_EXPECT_NOT_OK(Uri(path).ValidateFile());
    }

    std::string largePath = "/" + RandomData().GetRandomString(256);
    Uri largeUri(largePath);
    EXPECT_FALSE(largeUri.ValidateFile());

    std::string largeOkPath = "/" + RandomData().GetRandomString(255);
    Uri largeOkUri(largeOkPath);
    EXPECT_TRUE(largeOkUri.ValidateFile());
}

TEST_F(UriTest, TestValidate)
{
    LOG(INFO) << "Test Uri Validate basic function";

    std::vector<std::string> validPaths = { "/path",
                                            "/path/_to/file-name",
                                            "/path/to/filename",
                                            "/path//to/./filename",
                                            "/path//to//////filename",
                                            "/path/123",
                                            "/324",
                                            "/PATH/TO/FILE",
                                            "/PATH/to/File",
                                            "/path/中文",
                                            "/path/@a",
                                            "/path%",
                                            "/*as",
                                            "/path///",
                                            "/root/..",
                                            "/root/.",
                                            "/root/../",
                                            "/root/./",
                                            "/path/to/dir/" };

    for (const auto &path : validPaths) {
        LOG(INFO) << "Validate the path: " << Uri(path).Path();
        DS_EXPECT_OK(Uri(path).Validate());
    }

    std::vector<std::string> invalidPaths = { "a@",     "path",    "/path/>", "/path/t:o",
                                              "/path|", "/path\\", "/path\"", "/path<" };

    for (const auto &path : invalidPaths) {
        LOG(INFO) << "Invalidate the path: " << Uri(path).Path();
        DS_EXPECT_NOT_OK(Uri(path).Validate());
    }

    std::string largePath = "/" + RandomData().GetRandomString(256);
    Uri largeUri(largePath);
    EXPECT_FALSE(largeUri.Validate());

    std::string largeOkPath = "/" + RandomData().GetRandomString(255);
    Uri largeOkUri(largeOkPath);
    EXPECT_TRUE(largeOkUri.Validate());
}

TEST_F(UriTest, TestGetHomeDirByEnv)
{
    std::string resultDir = "";
    auto homeDir = std::getenv("HOME");
    ASSERT_NE(homeDir, nullptr);
    LOG(INFO) << "User home path is " << homeDir;
    DS_EXPECT_OK(Uri::GetHomeDir(resultDir));
    EXPECT_EQ(resultDir, std::string(homeDir));
}

TEST_F(UriTest, TestGetHomeDirWithoutHomeEnv)
{
    std::string resultDir = "";
    unsetenv("HOME");
    DS_EXPECT_OK(Uri::GetHomeDir(resultDir));
}

TEST_F(UriTest, TestRealpathSuccess)
{
    std::string path = "./";
    std::string resolvedPath;
    Status status = Uri::Realpath(path, resolvedPath);
    ASSERT_EQ(status, Status::OK());
    ASSERT_NE(resolvedPath, path);
}

TEST_F(UriTest, TestRealpathEmptyPath)
{
    std::string path = "";
    std::string resolvedPath;
    Status status = Uri::Realpath(path, resolvedPath);
    ASSERT_NE(status, Status::OK());
    ASSERT_EQ(status.GetCode(), StatusCode::K_RUNTIME_ERROR);
}

TEST_F(UriTest, TestGetHomeDirRealpathFailed)
{
    std::string resultDir = "";
    char changedEnv[] = "HOME=//a//b//c//d/";
    putenv(changedEnv);
    DS_EXPECT_NOT_OK(Uri::GetHomeDir(resultDir));
}

TEST_F(UriTest, TestNormalizePathWithUserHomeDir)
{
    std::string path1 = "~/path";
    Uri::NormalizePathWithUserHomeDir(path1, "~/path", "");
    LOG(INFO) << "path1: " << path1;
    std::string truePath1;
    Uri::GetHomeDir(truePath1);
    EXPECT_EQ(path1, truePath1 + "/path");

    std::string path2 = "/path";
    Uri::NormalizePathWithUserHomeDir(path2, "/path", "");
    LOG(INFO) << "path2: " << path2;
    EXPECT_EQ(path2, "/path");

    std::string path3 = "";
    EXPECT_FALSE(Uri::NormalizePathWithUserHomeDir(path3, "", ""));

    std::string path4 = "~/path";
    Uri::NormalizePathWithUserHomeDir(path4, "~/path", "/appendPath");
    LOG(INFO) << "path4: " << path4;
    std::string truePath4;
    Uri::GetHomeDir(truePath4);
    EXPECT_EQ(path4, truePath4 + "/path/appendPath");

    std::string path5 = "/path";
    Uri::NormalizePathWithUserHomeDir(path5, "/path", "/appendPath");
    LOG(INFO) << "path5: " << path5;
    EXPECT_EQ(path5, "/path");
}

TEST_F(UriTest, TestDeleteAppendPath)
{
    std::string path = "~/path/";
    Uri::DeleteAppendPath(path);
    EXPECT_EQ(path, "~/");
}

TEST_F(UriTest, TestChangeModeAndDel)
{
    const mode_t permission = 0600;
    std::string path = FLAGS_log_dir;
    std::string fileName = path + "/test.txt";

    int fd = open(fileName.c_str(), O_WRONLY | O_CREAT, 0700);
    ASSERT_TRUE(fd > 0);
    ssize_t rc = close(fd);
    EXPECT_EQ(rc, 0);

    inject::Set("util.beforeChmod", "sleep(2000)");
    std::thread t([&fileName] {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        int rc = unlink(fileName.c_str());
        ASSERT_EQ(rc, 0);
    });
    DS_ASSERT_NOT_OK(Uri::ModifyFilesInInputDir(path, permission));
    t.join();
}

TEST_F(UriTest, TestStrToInt32)
{
    int32_t res;
    ASSERT_TRUE(Uri::StrToInt32("-1", res));
    ASSERT_EQ(res, -1);
    ASSERT_FALSE(Uri::StrToInt32("HAJSDHASJ", res));
    ASSERT_EQ(res, 0);
    ASSERT_FALSE(Uri::StrToInt32("122DHASJ", res));
    ASSERT_EQ(res, 0);
    ASSERT_FALSE(Uri::StrToInt32("afdasf122DHASJ", res));
    ASSERT_EQ(res, 0);
    ASSERT_FALSE(Uri::StrToInt32("afdasf122", res));
    ASSERT_EQ(res, 0);
    ASSERT_FALSE(Uri::StrToInt32("    123", res));
    ASSERT_EQ(res, 0);
    ASSERT_FALSE(Uri::StrToInt32("    dfads123", res));
    ASSERT_EQ(res, 0);
    ASSERT_FALSE(Uri::StrToInt32("    12cg", res));
    ASSERT_EQ(res, 0);
    ASSERT_FALSE(Uri::StrToInt32("12   ", res));
    ASSERT_EQ(res, 0);
    int64_t num1 = int64_t(INT32_MAX) + 1;
    ASSERT_FALSE(Uri::StrToInt32(std::to_string(num1).c_str(), res));
    ASSERT_EQ(res, 0);
    int64_t num2 = int64_t(INT32_MIN) - 1;
    ASSERT_FALSE(Uri::StrToInt32(std::to_string(num2).c_str(), res));
    ASSERT_EQ(res, 0);
}

TEST_F(UriTest, TestStrToInt)
{
    int res;
    ASSERT_FALSE(Uri::StrToInt(nullptr, res));
    ASSERT_FALSE(Uri::StrToInt("HAJSDHASJ", res));
    ASSERT_EQ(res, 0);
    ASSERT_FALSE(Uri::StrToInt("122DHASJ", res));
    ASSERT_EQ(res, 0);
    ASSERT_FALSE(Uri::StrToInt("afdasf122DHASJ", res));
    ASSERT_EQ(res, 0);
    ASSERT_FALSE(Uri::StrToInt("afdasf122", res));
    ASSERT_EQ(res, 0);
    ASSERT_FALSE(Uri::StrToInt("    123", res));
    ASSERT_EQ(res, 0);
    ASSERT_FALSE(Uri::StrToInt("    dfads123", res));
    ASSERT_EQ(res, 0);
    ASSERT_FALSE(Uri::StrToInt("    12cg", res));
    ASSERT_EQ(res, 0);
    ASSERT_FALSE(Uri::StrToInt("12   ", res));
    ASSERT_EQ(res, 0);
    ASSERT_FALSE(Uri::StrToInt("18446744073709551617", res));
    ASSERT_EQ(res, 0);
    uint64_t num2 = 0;
    ASSERT_TRUE(Uri::StrToInt(std::to_string(num2).c_str(), res));
    ASSERT_EQ(res, 0);
    int64_t num3 = -1;
    ASSERT_TRUE(Uri::StrToInt(std::to_string(num3).c_str(), res));
    ASSERT_EQ(res, -1);
}

TEST_F(UriTest, TestSubPath)
{
    Uri uri("/home/user/docs");
    ASSERT_TRUE(uri.IsSubPathOf("/home"));
    ASSERT_TRUE(uri.IsSubPathOf("/home/user"));
    ASSERT_TRUE(uri.IsSubPathOf("/home/user/docs/"));
    ASSERT_FALSE(uri.IsSubPathOf("/home/user/docs/_ext"));
    ASSERT_FALSE(uri.IsSubPathOf("/home/user/docs/_ext/"));
    ASSERT_FALSE(uri.IsSubPathOf("/home/user/../user1"));
    ASSERT_TRUE(uri.IsSubPathOf("/home/user/../user1/../user/"));
    ASSERT_TRUE(uri.IsSubPathOf("/home/user/../.."));
    ASSERT_TRUE(uri.IsSubPathOf("/home/user/../.././"));
}
}  // namespace ut
}  // namespace datasystem