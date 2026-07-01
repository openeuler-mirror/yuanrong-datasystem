/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */
#include <gtest/gtest.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>

#include "internal/log/logging.h"

namespace datasystem {
namespace internal {
namespace {

std::string ReadFile(const std::filesystem::path &path)
{
    std::ifstream input(path);
    std::ostringstream content;
    content << input.rdbuf();
    return content.str();
}

std::filesystem::path FindSeverityFile(const std::filesystem::path &directory, const std::string &severity)
{
    const std::string marker = ".log." + severity + ".";
    for (const auto &entry : std::filesystem::directory_iterator(directory)) {
        if (entry.is_symlink() || !entry.is_regular_file()) {
            continue;
        }
        if (entry.path().filename().string().find(marker) != std::string::npos) {
            return entry.path();
        }
    }
    return {};
}

std::string FindLogLine(const std::string &content, const std::string &message)
{
    const size_t messagePos = content.find(message);
    if (messagePos == std::string::npos) {
        return {};
    }
    const size_t lineBegin = content.rfind('\n', messagePos);
    const size_t lineEnd = content.find('\n', messagePos);
    const size_t begin = lineBegin == std::string::npos ? 0 : lineBegin + 1;
    return content.substr(begin, lineEnd == std::string::npos ? std::string::npos : lineEnd - begin);
}

void EmitFileTestLogs()
{
    testing::internal::CaptureStderr();
    TE_LOG_INFO << "transfer-engine-info-file-test";
    TE_VLOG_1 << "transfer-engine-vmodule-file-test";
    TE_LOG_WARNING << "transfer-engine-warning-file-test";
    TE_LOG_ERROR << "transfer-engine-error-file-test";
    FlushLogs();
}

void ExpectSeverityFileRouting(const std::string &infoContent, const std::string &warningContent,
                               const std::string &errorContent)
{
    EXPECT_NE(infoContent.find("transfer-engine-info-file-test"), std::string::npos);
    EXPECT_NE(infoContent.find("transfer-engine-vmodule-file-test"), std::string::npos);
    EXPECT_NE(infoContent.find("transfer-engine-warning-file-test"), std::string::npos);
    EXPECT_NE(infoContent.find("transfer-engine-error-file-test"), std::string::npos);
    EXPECT_EQ(warningContent.find("transfer-engine-info-file-test"), std::string::npos);
    EXPECT_EQ(warningContent.find("transfer-engine-vmodule-file-test"), std::string::npos);
    EXPECT_NE(warningContent.find("transfer-engine-warning-file-test"), std::string::npos);
    EXPECT_NE(warningContent.find("transfer-engine-error-file-test"), std::string::npos);
    EXPECT_EQ(errorContent.find("transfer-engine-info-file-test"), std::string::npos);
    EXPECT_EQ(errorContent.find("transfer-engine-warning-file-test"), std::string::npos);
    EXPECT_NE(errorContent.find("transfer-engine-error-file-test"), std::string::npos);
}

void ExpectSeverityLinePrefixes(const std::string &infoContent)
{
    const std::string infoLine = FindLogLine(infoContent, "transfer-engine-info-file-test");
    const std::string warningLine = FindLogLine(infoContent, "transfer-engine-warning-file-test");
    const std::string errorLine = FindLogLine(infoContent, "transfer-engine-error-file-test");
    ASSERT_FALSE(infoLine.empty());
    ASSERT_FALSE(warningLine.empty());
    ASSERT_FALSE(errorLine.empty());
    EXPECT_EQ(infoLine.front(), 'I');
    EXPECT_EQ(warningLine.front(), 'W');
    EXPECT_EQ(errorLine.front(), 'E');
    EXPECT_NE(infoLine.find("logging_llt_test.cpp:"), std::string::npos);
    EXPECT_NE(infoContent.find("Log file created at:"), std::string::npos);
}

void ExpectStderrThresholdRouting(const std::string &stderrOutput)
{
    EXPECT_EQ(stderrOutput.find("transfer-engine-info-file-test"), std::string::npos);
    EXPECT_EQ(stderrOutput.find("transfer-engine-warning-file-test"), std::string::npos);
    EXPECT_NE(stderrOutput.find("transfer-engine-error-file-test"), std::string::npos);
}

void ExpectSeveritySymlinks(const std::filesystem::path &directory)
{
    EXPECT_TRUE(std::filesystem::is_symlink(directory / "transfer_engine.INFO"));
    EXPECT_TRUE(std::filesystem::is_symlink(directory / "transfer_engine.WARNING"));
    EXPECT_TRUE(std::filesystem::is_symlink(directory / "transfer_engine.ERROR"));
}

TEST(TransferEngineLoggingStderrTest, FiltersDisabledLogBeforeEvaluatingStreamArguments)
{
    int sideEffect = 0;
    TE_LOG_INFO << ++sideEffect;
    TE_VLOG_1 << ++sideEffect;
    EXPECT_EQ(sideEffect, 0);
}

TEST(TransferEngineLoggingStderrTest, EmitsEnabledWarning)
{
    testing::internal::CaptureStderr();
    TE_LOG_WARNING << "transfer-engine-warning-test";
    FlushLogs();
    const std::string output = testing::internal::GetCapturedStderr();

    EXPECT_NE(output.find("transfer-engine-warning-test"), std::string::npos);
    EXPECT_NE(output.find("logging_llt_test.cpp"), std::string::npos);
}

TEST(TransferEngineLoggingStderrTest, ExposesConfiguredLevels)
{
    EXPECT_FALSE(ShouldLog(LogSeverity::DEBUG));
    EXPECT_FALSE(ShouldLog(LogSeverity::INFO));
    EXPECT_TRUE(ShouldLog(LogSeverity::WARNING));
    EXPECT_TRUE(ShouldLog(LogSeverity::ERROR));
    EXPECT_TRUE(ShouldLog(LogSeverity::FATAL));
    EXPECT_FALSE(ShouldVLog(1, __FILE__));
}

TEST(TransferEngineLoggingStderrTest, FatalLogFlushesAndAborts)
{
    EXPECT_DEATH({ TE_LOG_FATAL << "transfer-engine-fatal-test"; }, "transfer-engine-fatal-test");
}

TEST(TransferEngineLoggingFileTest, PreservesSeverityFilesAndStderrThreshold)
{
    const char *logDir = std::getenv("TRANSFER_ENGINE_LOG_DIR");
    ASSERT_NE(logDir, nullptr);
    const std::filesystem::path directory(logDir);
    std::filesystem::remove_all(directory);
    std::filesystem::create_directories(directory);

    EmitFileTestLogs();
    const std::string stderrOutput = testing::internal::GetCapturedStderr();

    const auto infoFile = FindSeverityFile(directory, "INFO");
    const auto warningFile = FindSeverityFile(directory, "WARNING");
    const auto errorFile = FindSeverityFile(directory, "ERROR");
    ASSERT_FALSE(infoFile.empty());
    ASSERT_FALSE(warningFile.empty());
    ASSERT_FALSE(errorFile.empty());

    const std::string infoContent = ReadFile(infoFile);
    const std::string warningContent = ReadFile(warningFile);
    const std::string errorContent = ReadFile(errorFile);

    ExpectSeverityFileRouting(infoContent, warningContent, errorContent);
    ExpectSeverityLinePrefixes(infoContent);
    ExpectStderrThresholdRouting(stderrOutput);
    ExpectSeveritySymlinks(directory);

    EXPECT_DEATH({ TE_LOG_FATAL << "transfer-engine-fatal-file-test"; }, "transfer-engine-fatal-file-test");
    const auto fatalFile = FindSeverityFile(directory, "FATAL");
    ASSERT_FALSE(fatalFile.empty());
    const std::string fatalLine = FindLogLine(ReadFile(fatalFile), "transfer-engine-fatal-file-test");
    ASSERT_FALSE(fatalLine.empty());
    EXPECT_EQ(fatalLine.front(), 'F');
    EXPECT_TRUE(std::filesystem::is_symlink(directory / "transfer_engine.FATAL"));
}

}  // namespace
}  // namespace internal
}  // namespace datasystem
