#include "notify_dispatcher.h"

#include "pipeline/cache_reader.h"
#include "pipeline/data_pattern.h"
#include "common/simple_log.h"
#include <utility>

using namespace datasystem;

NotifyDispatcher::NotifyDispatcher(const Config &cfg,
                                    std::shared_ptr<datasystem::KVClient> client,
                                    MetricsCollector &metrics)
    : cfg_(cfg), client_(std::move(client)), metrics_(metrics), notifyPool_(100) {
    for (auto &name : cfg_.notifyPipeline) {
        auto fn = GetOpFunc(name);
        if (!fn) {
            SLOG_WARN("Unknown notify_pipeline op: " << name << ", skipping");
            continue;
        }
        notifyOps_.emplace_back(name, fn);
        if (name == kOpSetStringView || name == kOpMemoryCopy) {
            notifyNeedsData_ = true;
        }
    }
}

void NotifyDispatcher::DispatchNotify(const std::string &action, int sender,
                                      const std::vector<std::string> &keys,
                                      uint64_t size) {
    // Cache mode: warmup_done notification
    if (action == "warmup_done" && cacheReader_) {
        cacheReader_->OnWarmupDone(sender, keys);
        return;
    }

    if (keys.empty()) return;

    // Cache mode: evict keys -> CacheReader
    if (cacheReader_) {
        cacheReader_->OnEvictKeys(keys);
        return;
    }

    // Non-cache mode: original notifyPipeline logic
    notifyPool_.Submit([this, keys = std::move(keys), sender, expectedSize = size]() {
        PipelineContext ctx;
        ctx.key = keys[0];
        ctx.batchKeys = keys;
        ctx.size = expectedSize;
        ctx.senderId = sender;
        if (notifyNeedsData_) {
            auto cacheKey = std::to_string(expectedSize) + "_" + std::to_string(sender);
            {
                std::lock_guard<kvtest::mutex> lock(pregenMutex_);
                auto it = pregenData_.find(cacheKey);
                if (it != pregenData_.end()) {
                    ctx.data = it->second;
                } else {
                    ctx.data = GeneratePatternData(expectedSize, sender);
                    pregenData_[cacheKey] = ctx.data;
                }
            }
        }
        ctx.client = client_;
        ctx.param.writeMode = WriteMode::NONE_L2_CACHE_EVICT;
        ctx.param.ttlSecond = cfg_.ttlSeconds;
        ctx.verifyFailCount = &metrics_.VerifyFailCounter();
        ctx.verifyCfg = BuildVerifyConfig(cfg_.verifyLevel, cfg_.verifySampleBytes,
                                          cfg_.verifySampleStepBytes, cfg_.verifyFailOp);

        ExecutePipeline(notifyOps_, ctx, metrics_,
                        metrics_.VerifyFailCounter());
    });
}
