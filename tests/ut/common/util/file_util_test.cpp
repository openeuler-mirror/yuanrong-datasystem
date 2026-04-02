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
 * Description: File util basic function test.
 */
#include "datasystem/common/util/file_util.h"

#include <fcntl.h>
#include <fstream>
#include <glob.h>

#include "ut/common.h"
#include "datasystem/common/util/random_data.h"

DS_DECLARE_string(log_dir);

namespace datasystem {
namespace ut {
class FileUtilTest : public CommonTest {
public:
    FileUtilTest()
        : rand_(std::chrono::time_point_cast<std::chrono::microseconds>(std::chrono::system_clock::now())
                    .time_since_epoch()
                    .count())
    {
    }

    Status CreateTextFile(const std::string &filename, size_t len)
    {
        std::ofstream osf;
        std::string fullPath = FLAGS_log_dir + "/" + filename;
        osf.open(fullPath, std::ios::out);
        if (!osf.is_open()) {
            RETURN_STATUS(StatusCode::K_UNKNOWN_ERROR, "create txt file failed: " + fullPath + std::to_string(errno));
        }
        std::string content = rand_.GetRandomString(len);
        osf << content << std::endl;
        osf.close();
        return Status::OK();
    }

protected:
    RandomData rand_;
};

TEST_F(FileUtilTest, JoinPathTest)
{
    std::vector<std::string> absPath = { "", "absolute", "path" };
    std::vector<std::string> relativePath = { ".", "relative", "path" };
    EXPECT_EQ("/absolute/path", JoinPath(absPath));
    EXPECT_EQ("./relative/path", JoinPath(relativePath));
}

TEST_F(FileUtilTest, TestCreateDir)
{
    LOG(INFO) << "Test create dir.";
    std::string dirname =
        FLAGS_log_dir + "/FileUtilTest/" + rand_.GetRandomString(10) + "/" + rand_.GetRandomString(10);
    DS_ASSERT_NOT_OK(CreateDir(dirname, false));
    DS_ASSERT_OK(CreateDir(dirname, true));
}

TEST_F(FileUtilTest, TestIsSafeDir)
{
    LOG(INFO) << "Test safe dir.";
    ASSERT_TRUE(IsSafePath("/home"));
    ASSERT_TRUE(IsSafePath("/home/../home1"));
    ASSERT_TRUE(IsSafePath("/not/exist/path"));
    ASSERT_FALSE(IsSafePath("/"));
    ASSERT_FALSE(IsSafePath("/home/.."));
    ASSERT_FALSE(IsSafePath("/boot"));
    ASSERT_FALSE(IsSafePath("/boot/xxx"));
    ASSERT_TRUE(IsSafePath("/usr1/xxx"));
    ASSERT_TRUE(IsSafePath("/usr1"));
}

TEST_F(FileUtilTest, TestIsDirectory)
{
    LOG(INFO) << "Test is dir.";
    {
        // test normal directory.
        bool exist = false;
        DS_ASSERT_OK(IsDirectory("/tmp", exist));
        ASSERT_TRUE(exist);
    }
    {
        // test not exist files.
        std::string dirname =
            FLAGS_log_dir + "/FileUtilTest/" + rand_.GetRandomString(10) + "/" + rand_.GetRandomString(10);
        bool exist = false;
        DS_ASSERT_NOT_OK(IsDirectory(dirname, exist));
    }
    {
        // test not directory.
        std::string filename = rand_.GetRandomString(10);
        DS_EXPECT_OK(CreateTextFile(filename, 10));
        bool isDir = false;
        DS_EXPECT_OK(IsDirectory(FLAGS_log_dir + "/" + filename, isDir));
        EXPECT_FALSE(isDir);
        DeleteFile(filename);
    }
}

TEST_F(FileUtilTest, TestDeleteNotExistFile)
{
    LOG(INFO) << "Test delete not exist file.";
    std::string filename = "/xxx/yyy/test.txt";
    DS_ASSERT_NOT_OK(DeleteFile(filename));
}

TEST_F(FileUtilTest, FileLimitReachedException)
{
    LOG(INFO) << "Test file limit reached exception scenario.";
    LOG(ERROR) << "Start the error log.";
    SetFileLimit(34);
    int FD[50];
    std::string filename[50];
    for (int i = 0; i < 50; i++) {
        FD[i] = -1;
        filename[i] = rand_.GetRandomString(10);
    }
    // create a dir for all test created files, under log_dir which exist.
    std::string folder = FLAGS_log_dir + "/FileLimitTest";
    DS_ASSERT_OK(CreateDir(folder));
    // since 50 is larger than limit, we will eventually
    // pass the soft limit
    for (int i = 0; i < 50; i++) {
        std::string name = folder + "/" + filename[i];
        FD[i] = open(name.c_str(), O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO);
    }
    int res = -1;
    // tag indicating if we reach the limit or not
    bool tag = true;
    std::string testcase = folder + "/Exception";
    Status rc = OpenFile(testcase, O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO, &res);
    if (res != -1) {
        tag = false;
        close(res);
        DS_ASSERT_OK(DeleteFile(testcase));
    } else {
        ASSERT_EQ(StatusCode::K_FILE_LIMIT_REACHED, rc.GetCode());
        SetFileLimit(2048);
        DS_ASSERT_OK(OpenFile(testcase, O_RDWR | O_CREAT, S_IRWXU | S_IRWXG | S_IRWXO, &res));
        close(res);
        DS_ASSERT_OK(DeleteFile(testcase));
    }
    // clean-up
    for (int i = 0; i < 50; i++) {
        std::string name = folder + "/" + filename[i];
        if (FD[i] != -1) {
            close(FD[i]);
            DS_ASSERT_OK(DeleteFile(name));
        }
    }
    rmdir(folder.c_str());
    // not true we failed to set the file limit
    ASSERT_TRUE(tag);
}

TEST_F(FileUtilTest, FileLimitErrors1)
{
    Status rc;
    struct rlimit rlimSet;
    // Manually set the limits directly
    rlimSet.rlim_max = 20;  // irreversably, you can lower hard limit for the process (without root)
    rlimSet.rlim_cur = 10;
    ASSERT_EQ(setrlimit(RLIMIT_NOFILE, &rlimSet), 0);

    // violate the hard limit and expect error
    rc = SetFileLimit(21);
    ASSERT_EQ(rc.GetCode(), StatusCode::K_IO_ERROR);
}

TEST_F(FileUtilTest, FileLimitErrors2)
{
    struct rlimit rlimGet;

    ASSERT_EQ(getrlimit(RLIMIT_NOFILE, &rlimGet), 0);
    LOG(INFO) << "rlimGet.rlim_max: " << rlimGet.rlim_max << " rlimGet.rlim_cur: " << rlimGet.rlim_cur;

    // On some systems, its possible that the hard limit max is RLIM_INFINITY.  In such a case do this additional test.
    if (rlimGet.rlim_max == RLIM_INFINITY) {
        // Try to set the limit.  It will set the limit to 10, and it will define the hard limit from the system file.
        DS_ASSERT_OK(SetFileLimit(10));

        // Now, get the limits again and validate the assigned value and that max is no longer INFINITY
        ASSERT_EQ(getrlimit(RLIMIT_NOFILE, &rlimGet), 0);
        ASSERT_EQ(rlimGet.rlim_cur, static_cast<rlim_t>(10));
        ASSERT_NE(rlimGet.rlim_max, RLIM_INFINITY);
    }
}

TEST_F(FileUtilTest, TestChangeFileMod)
{
    const std::string filename = rand_.GetRandomString(10);
    DS_ASSERT_OK(CreateTextFile(filename, 10));
    const mode_t permission = 01760;
    const std::string fullPath = FLAGS_log_dir + "/" + filename;
    DS_ASSERT_NOT_OK(ChangeFileMod(filename, permission));  // File not exist
    DS_ASSERT_OK(ChangeFileMod(fullPath, permission));      // File exist
    struct stat result;
    ASSERT_EQ(stat(fullPath.c_str(), &result), 0);
    ASSERT_EQ(result.st_mode, static_cast<mode_t>(0101760));
    DS_ASSERT_OK(DeleteFile(fullPath));
}

TEST_F(FileUtilTest, TestGetFreeSpace)
{
    size_t fsize = GetFreeSpaceBytes("/home");
    LOG(INFO) << "Free Size is: " << fsize;
    fsize = GetFreeSpaceBytes("/dev/shm");
    LOG(INFO) << "Free Size is: " << fsize;
    ASSERT_NE(fsize, 0u);

    fsize = GetFreeSpaceBytes("./");
    LOG(INFO) << "Free Size is: " << fsize;
    ASSERT_NE(fsize, 0u);
}

TEST_F(FileUtilTest, TestRemoveAFile)
{
    std::string filename("testfile");
    DS_ASSERT_OK(CreateTextFile(filename, 10));
    ASSERT_TRUE(FileExist(FLAGS_log_dir + "/" + filename));

    DS_ASSERT_OK(Remove(FLAGS_log_dir + "/" + filename));
    ASSERT_FALSE(FileExist(FLAGS_log_dir + "/" + filename));
}

TEST_F(FileUtilTest, TestRemoveAnDir)
{
    std::string dirname = FLAGS_log_dir + "/FileUtilTest/level0/level1";
    DS_ASSERT_OK(CreateDir(dirname, true));

    // can not remove not empty dir
    DS_ASSERT_NOT_OK(Remove(FLAGS_log_dir + "/FileUtilTest/level0"));
    // can remove empty dir
    DS_ASSERT_OK(Remove(FLAGS_log_dir + "/FileUtilTest/level0/level1"));
    ASSERT_FALSE(FileExist(FLAGS_log_dir + "/FileUtilTest/level0/level1"));
}

TEST_F(FileUtilTest, TestRemoveAll)
{
    std::string dirname = FLAGS_log_dir + "/FileUtilTest/level0/level1";
    DS_ASSERT_OK(CreateDir(dirname, true));
    std::string level0File(FLAGS_log_dir + "/FileUtilTest/level0/file");
    std::ofstream create(level0File);
    std::string level0FileLink(FLAGS_log_dir + "/FileUtilTest/level0/filelink");
    ASSERT_EQ(symlink(level0File.c_str(), level0FileLink.c_str()), 0);

    DS_ASSERT_OK(RemoveAll(FLAGS_log_dir + "/FileUtilTest"));
    ASSERT_FALSE(FileExist(FLAGS_log_dir + "/FileUtilTest"));
}

TEST_F(FileUtilTest, TestResizeFile)
{
    std::string filename("testfile");
    std::string fullname(FLAGS_log_dir + "/" + filename);
    DS_ASSERT_OK(CreateTextFile(filename, 10));

    DS_ASSERT_OK(ResizeFile(fullname, 2048));
    EXPECT_EQ(FileSize(fullname), 2048u);
    DS_ASSERT_OK(ResizeFile(fullname, 100));
    EXPECT_EQ(FileSize(fullname), 100u);
}

TEST_F(FileUtilTest, TestIsEmptyDir)
{
    std::string filename("testfile");
    DS_ASSERT_OK(CreateTextFile(filename, 10));
    EXPECT_FALSE(IsEmptyDir(FLAGS_log_dir + "/" + filename));
    EXPECT_FALSE(IsEmptyDir(FLAGS_log_dir));

    DS_ASSERT_OK(DeleteFile(FLAGS_log_dir + "/" + filename));
    EXPECT_TRUE(IsEmptyDir(FLAGS_log_dir));
}

TEST_F(FileUtilTest, BigFileWriteRead)
{
    // README
    // For CI purposes:
    // size is reduced from 4 * 1024UL * 1024UL * 1024UL;
    // partSize is reduced from 128 * 1024 * 1024;
    int fd = -1;
    size_t size = 2 * 1024UL * 1024UL * 1024UL;
    size_t partSize = 64 * 1024 * 1024;
    std::string part = rand_.GetRandomString(partSize);
    char *src = (char *)malloc(size);
    char *dst = (char *)malloc(size);
    size_t remainSize = size;
    size_t offSet = 0;
    for (size_t i = 0; i < size / partSize; i++) {
        ASSERT_EQ(memcpy_s(src + offSet, std::min(remainSize, partSize), part.data(), partSize), EOK);
        remainSize -= partSize;
        offSet += partSize;
    }
    std::string filename = "test.bin";
    DS_EXPECT_OK(OpenFile(filename, O_RDWR | O_CREAT, 0755, &fd));
    DS_ASSERT_OK(WriteFile(fd, src, size, 0));
    DS_ASSERT_OK(ReadFile(fd, dst, size, 0));
    ASSERT_EQ(memcmp(src, dst, size), 0);
    close(fd);
    DS_ASSERT_OK(DeleteFile(filename));
}

TEST_F(FileUtilTest, TestMvFileToNewPath)
{
    std::string filename = "old_file";
    std::string newPath = "./";
    DS_ASSERT_OK(CreateTextFile(filename, 10));

    std::string realPath = FLAGS_log_dir + "/" + filename;
    std::string invalidFilePath = FLAGS_log_dir + "/" + "invalid";
    std::string invalidNewPath = "./invalid_dir";

    Status rc = MoveFileToNewPath(invalidFilePath, newPath);
    ASSERT_EQ(rc.GetCode(), StatusCode::K_INVALID);
    rc = MoveFileToNewPath(realPath, invalidNewPath);
    ASSERT_EQ(rc.GetCode(), StatusCode::K_INVALID);

    DS_ASSERT_OK(MoveFileToNewPath(realPath, newPath));
    std::string newFilePath = newPath + filename;
    ASSERT_TRUE(FileExist(newFilePath));
    DS_ASSERT_OK(DeleteFile(newFilePath));
}
}  // namespace ut
}  // namespace datasystem
