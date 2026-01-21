#pragma once
#include <string>
#include <vector>
#include <memory>
#include <functional>
#include <future>
#include <mutex>
#include <condition_variable>
#include <stack>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <algorithm>
#include <cctype>
 
using ProgressCb = std::function<void(int64_t /*bytes*/, int64_t /*total*/)>;

class DownloaderFactory {
public:
    virtual ~DownloaderFactory() = default;
    virtual bool download(const std::string& uri,
                          const std::string& local_path,
                          ProgressCb cb = nullptr) = 0;
    virtual std::future<bool> downloadAsync(const std::string& uri,
                                            const std::string& local_path,
                                            ProgressCb cb = nullptr) = 0;

    static std::unique_ptr<DownloaderFactory> createDownloader(const std::string& uri);
};

inline std::string toLower(std::string s) {
    for (char& c : s) c = (char)std::tolower(c);
    return s;
}
 
namespace fs = std::filesystem;

class LocalFileDownloader final : public DownloaderFactory {
public:
    bool download(const std::string& uri,
                  const std::string& local_path,
                  ProgressCb cb) override {
        std::string src = uri.find("file://") ? uri : uri.substr(7);
        if (!fs::exists(src)) return false;
        fs::path s(src), d(local_path);
        fs::path dst = d / s.filename();
        fs::create_directories(d);
        fs::copy_file(s, dst, fs::copy_options::overwrite_existing);
        if (cb) {
            auto size = fs::file_size(src);
            cb(size, size);
        }
        return true;
    }
    std::future<bool> downloadAsync(const std::string& uri,
                                    const std::string& local_path,
                                    ProgressCb cb) override {
        return std::async(std::launch::async, &LocalFileDownloader::download, this, uri, local_path, cb);
    }
};

inline std::unique_ptr<DownloaderFactory> DownloaderFactory::createDownloader(const std::string& uri) {
    auto pos = uri.find("://");
    if (pos == std::string::npos) return std::make_unique<LocalFileDownloader>();
    std::string scheme = toLower(uri.substr(0, pos));
    if (scheme == "file") return std::make_unique<LocalFileDownloader>();
    // if (scheme == "ftp" || scheme == "ftps") return std::make_unique<FtpDownloader>();
    // if (scheme == "http" || scheme == "https") return std::make_unique<HttpDownloader>();
    // if (scheme == "hdfs") return std::make_unique<HdfsDownloader>();
    throw std::invalid_argument("Unsupported scheme: " + scheme);
}