/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 */
#include "tools/logging.h"

#include <atomic>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <string>

#include <pthread.h>
#include <unistd.h>

extern char **environ;

namespace p2p {
namespace {

enum class LogSeverity {
    INFO = 0,
    WARNING = 1,
    ERROR = 2,
};

std::atomic<P2pLogCallback> g_logCallback{ nullptr };

const char *SeverityName(LogSeverity severity)
{
    switch (severity) {
        case LogSeverity::INFO:
            return "INFO";
        case LogSeverity::WARNING:
            return "WARNING";
        case LogSeverity::ERROR:
            return "ERROR";
        default:
            return "INFO";
    }
}

void WriteAll(int fd, const char *data, size_t size)
{
    while (size > 0) {
        const ssize_t written = write(fd, data, size);
        if (written > 0) {
            data += written;
            size -= static_cast<size_t>(written);
            continue;
        }
        if (written < 0 && errno == EINTR) {
            continue;
        }
        break;
    }
}

void PrintFallback(LogSeverity severity, const std::string &message)
{
    static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
    const bool locked = pthread_mutex_lock(&mutex) == 0;

    char prefix[64] = {};
    const int prefixSize = std::snprintf(prefix, sizeof(prefix), "[P2P][%s] ", SeverityName(severity));
    if (prefixSize > 0) {
        WriteAll(STDERR_FILENO, prefix, static_cast<size_t>(prefixSize));
    }
    WriteAll(STDERR_FILENO, message.data(), message.size());
    WriteAll(STDERR_FILENO, "\n", 1);
    if (locked) {
        (void)pthread_mutex_unlock(&mutex);
    }
}

bool EmitToCallback(P2pLogSeverity severity, int vlogLevel, const std::string &message, const char *file, int line)
{
    P2pLogCallback callback = g_logCallback.load(std::memory_order_acquire);
    if (callback == nullptr) {
        return false;
    }
    callback(severity, vlogLevel, file, line, message.c_str());
    return true;
}

bool IsEnvDumpEnabled()
{
    const char *value = std::getenv("TRANSFER_ENGINE_ENABLE_ENV_DUMP");
    if (value == nullptr) {
        return false;
    }
    const std::string flag(value);
    return flag == "1" || flag == "true" || flag == "TRUE" || flag == "on" || flag == "ON" || flag == "yes" ||
           flag == "YES";
}

}  // namespace

void SetLogCallback(P2pLogCallback callback)
{
    g_logCallback.store(callback, std::memory_order_release);
}

void LogInfo(const std::string &message)
{
    if (EmitToCallback(P2P_LOG_INFO, 1, message, __FILE__, __LINE__)) {
        return;
    }
    PrintFallback(LogSeverity::INFO, message);
}

void LogWarning(const std::string &message)
{
    if (EmitToCallback(P2P_LOG_WARNING, -1, message, __FILE__, __LINE__)) {
        return;
    }
    PrintFallback(LogSeverity::WARNING, message);
}

void LogError(const std::string &message)
{
    if (EmitToCallback(P2P_LOG_ERROR, -1, message, __FILE__, __LINE__)) {
        return;
    }
    PrintFallback(LogSeverity::ERROR, message);
}

void DumpProcessEnvironment(const char *stage)
{
    if (!IsEnvDumpEnabled()) {
        return;
    }
    const char *safeStage = stage == nullptr ? "unknown" : stage;
    LogInfo(std::string("process environment dump begin, stage=") + safeStage);
    if (::environ == nullptr) {
        LogWarning(std::string("process environment dump skipped: environ is null, stage=") + safeStage);
        return;
    }
    for (int index = 0; ::environ[index] != nullptr; ++index) {
        LogInfo(std::string("env[") + std::to_string(index) + "] " + ::environ[index]);
    }
    LogInfo(std::string("process environment dump end, stage=") + safeStage);
}

}  // namespace p2p

extern "C" void P2PSetLogCallback(P2pLogCallback callback)
{
    p2p::SetLogCallback(callback);
}
