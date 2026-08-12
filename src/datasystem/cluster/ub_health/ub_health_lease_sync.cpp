/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

#include "datasystem/cluster/ub_health/ub_health_lease_sync.h"

#include <utility>

#include "datasystem/common/inject/inject_point.h"
#include "datasystem/common/log/log.h"
#include "datasystem/common/object_cache/ub_health_summary_codec.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/protos/share_memory.pb.h"

namespace datasystem::cluster {
UbHealthLeaseSync::UbHealthLeaseSync(ICoordinationBackend &backend, Config config)
    : UbHealthLeaseSync(
          [&backend](const std::string &table, const std::string &key, const std::string &value) {
              return backend.PutWithKeepAliveLease(table, key, value);
          },
          [&backend](const std::string &table, std::vector<std::pair<std::string, std::string>> &records) {
              return backend.GetAll(table, records);
          },
          std::move(config))
{
}

UbHealthLeaseSync::UbHealthLeaseSync(LeasePublisher publisher, SnapshotLoader loader, Config config)
    : publisher_(std::move(publisher)),
      loader_(std::move(loader)),
      tableName_(std::move(config.tableName)),
      self_(std::move(config.self)),
      incarnationProvider_(std::move(config.incarnationProvider)),
      provider_(std::move(config.provider)),
      consumer_(std::move(config.consumer)),
      interval_(config.interval)
{
}

UbHealthLeaseSync::~UbHealthLeaseSync()
{
    Stop();
}

Status UbHealthLeaseSync::Start()
{
    CHECK_FAIL_RETURN_STATUS(publisher_ && loader_ && !tableName_.empty() && !self_.Empty() && incarnationProvider_
                                 && provider_ && consumer_ && interval_.count() > 0,
                             K_INVALID, "invalid UB health lease synchronizer configuration");
    bool expected = false;
    CHECK_FAIL_RETURN_STATUS(started_.compare_exchange_strong(expected, true), K_INVALID,
                             "UB health lease synchronizer is already started");
    stopping_.store(false, std::memory_order_release);
    thread_ = Thread(&UbHealthLeaseSync::Run, this);
    thread_.set_name("ub-health-sync");
    return Status::OK();
}

void UbHealthLeaseSync::Stop()
{
    if (!started_.load(std::memory_order_acquire)) {
        return;
    }
    stopping_.store(true, std::memory_order_release);
    cv_.notify_all();
    if (thread_.joinable()) {
        thread_.join();
    }
    started_.store(false, std::memory_order_release);
}

Status UbHealthLeaseSync::SyncOnce()
{
    std::lock_guard<std::mutex> syncLock(syncMutex_);
    INJECT_POINT("UbHealthLeaseSync.beforePublish");
    std::string incarnation;
    RETURN_IF_NOT_OK(incarnationProvider_(incarnation));
    CHECK_FAIL_RETURN_STATUS(!incarnation.empty(), K_NOT_READY, "UB health topology incarnation is unavailable");
    auto selfSummary = provider_();
    selfSummary.worker = self_;
    selfSummary.incarnation = std::move(incarnation);
    UbHealthSummaryPb selfPb;
    EncodeUbHealthSummary(selfSummary, selfPb);
    std::string bytes;
    CHECK_FAIL_RETURN_STATUS(selfPb.SerializeToString(&bytes), K_RUNTIME_ERROR,
                             "serialize UB health lease value failed");
    RETURN_IF_NOT_OK(publisher_(tableName_, self_.ToString(), bytes));

    std::vector<std::pair<std::string, std::string>> records;
    RETURN_IF_NOT_OK(loader_(tableName_, records));
    std::unordered_map<std::string, UbHealthSummary> nextSummaries;
    nextSummaries.reserve(records.size());
    for (const auto &[key, value] : records) {
        UbHealthSummaryPb pb;
        if (!pb.ParseFromString(value)) {
            LOG(WARNING) << "Ignore malformed UB health lease value for worker " << key;
            auto previous = lastSummaries_.find(key);
            if (previous != lastSummaries_.end()) {
                nextSummaries.emplace(key, previous->second);
            }
            continue;
        }
        UbHealthSummary summary;
        auto rc = DecodeUbHealthSummary(pb, summary);
        if (rc.IsError() || summary.worker.ToString() != key) {
            LOG(WARNING) << "Ignore invalid UB health lease value for worker " << key << ": " << rc.ToString();
            auto previous = lastSummaries_.find(key);
            if (previous != lastSummaries_.end()) {
                nextSummaries.emplace(key, previous->second);
            }
            continue;
        }
        nextSummaries.emplace(key, std::move(summary));
    }

    std::vector<UbHealthSummary> summaries;
    summaries.reserve(nextSummaries.size());
    for (const auto &[key, summary] : nextSummaries) {
        (void)key;
        summaries.emplace_back(summary);
    }
    lastSummaries_ = std::move(nextSummaries);
    consumer_(summaries);
    return Status::OK();
}

void UbHealthLeaseSync::Run()
{
    while (!stopping_.load(std::memory_order_acquire)) {
        LOG_IF_ERROR(SyncOnce(), "Refresh UB health lease snapshot failed");
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait_for(lock, interval_, [this] { return stopping_.load(std::memory_order_acquire); });
    }
}
}  // namespace datasystem::cluster
