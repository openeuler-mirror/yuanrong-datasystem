#include "notify_dispatcher.h"

#include "pipeline/cache_reader.h"
#include "pipeline/data_pattern.h"
#include "pipeline/kv_worker.h"
#include "common/simple_log.h"
#include <utility>

using namespace datasystem;

NotifyDispatcher::NotifyDispatcher(const Config &cfg,
                                    std::shared_ptr<datasystem::KVClient> client,
                                    MetricsCollector &metrics)
    : cfg_(cfg), client_(std::move(client)), metrics_(metrics),
      notifyPool_(cfg.NumReadThreads(), cfg.notifyQueueMax),
      // mgetPool_ reuses the same read-thread count; its queue bound caps
      // in-flight mGet batches so a notify flood does not OOM pending tasks.
      mgetPool_(cfg.NumReadThreads(), cfg.notifyQueueMax),
      mgetRngs_(cfg.NumReadThreads()) {
    for (size_t i = 0; i < mgetRngs_.size(); i++) {
        mgetRngs_[i].seed(static_cast<uint32_t>(i + 1));
    }
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
                                      uint64_t size, uint64_t version) {
    // Cache mode: warmup_done notification → build the shared key pool, either
    // on the cache reader or (homogeneous model) on the writer.
    if (action == "warmup_done") {
        if (cacheReader_) {
            cacheReader_->OnWarmupDone(sender, keys);
        } else if (cfg_.keyPoolSize > 0 && worker_) {
            worker_->OnWarmupDone(sender, keys);
        }
        return;
    }

    if (keys.empty()) return;

    // Cache mode reader: version-bearing notify is an update broadcast;
    // otherwise the keys are plain backfill sync.
    if (cacheReader_) {
        if (version > 0) {
            cacheReader_->OnUpdateVersion(keys, version);
        } else {
            cacheReader_->OnEvictKeys(keys);
        }
        return;
    }

    // Cache mode writer (homogeneous read+write clients): version updates feed
    // the writer's max-version map; plain backfill/new-key sync feeds its
    // shared key pool. This also keeps these from falling through to the
    // pipeline-mode notify_pipeline (getBuffer) below.
    if (cfg_.keyPoolSize > 0 && worker_) {
        if (version > 0) {
            worker_->OnUpdateVersion(keys, version);
        } else {
            worker_->OnEvictKeys(keys);
        }
        return;
    }

    // Distribution mode: buffer keys into a pending queue, then trigger a
    // probability-sampled mGet batch on the mgetPool_. C2 non-blocking: the
    // sampled target batch is min(target, queue depth); no waiting for the
    // queue to fill. Each notify can trigger one sampled mGet batch, so the
    // mGet rate tracks the notify rate (with queue backlog as slack).
    if (cfg_.mgetSizeDist.enabled) {
        {
            std::lock_guard<kvtest::mutex> lock(pendingMutex_);
            size_t cap = cfg_.mgetSizeDist.pendingQueueMax;
            for (const auto &k : keys) {
                if (cap != 0 && pendingQueue_.size() >= cap) {
                    // Drop oldest to bound memory (matches notifyQueueMax
                    // fire-and-forget semantics).
                    pendingQueue_.pop_front();
                }
                pendingQueue_.push_back({k, sender, size});
            }
        }
        // Trigger one sampled mGet per notify. The worker picks its own RNG
        // by thread id (hash modulo) so no shared RNG contention.
        mgetPool_.Submit([this]() {
            std::mt19937 *rng = nullptr;
            {
                static std::hash<std::thread::id> h;
                // In bazel mode the std::thread::id of a bthread is still
                // well-defined and unique per worker; in cmake mode it is the
                // pthread id. Either way the modulo gives a stable index.
                // Fallback to 0 if NumReadThreads is 0 (degenerate config).
                auto idx = cfg_.NumReadThreads() > 0
                           ? h(std::this_thread::get_id()) % static_cast<size_t>(cfg_.NumReadThreads())
                           : 0;
                if (idx < mgetRngs_.size()) rng = &mgetRngs_[idx];
            }
            int target = 1;
            if (rng) target = SampleMgetBatchSize(cfg_.mgetSizeDist, *rng);
            std::vector<PendingKey> batch;
            batch.reserve(target);
            uint64_t batchSize = 0;
            int batchSender = 0;
            {
                std::lock_guard<kvtest::mutex> lock(pendingMutex_);
                int actual = std::min(target, static_cast<int>(pendingQueue_.size()));
                for (int i = 0; i < actual; i++) {
                    batch.push_back(std::move(pendingQueue_.front()));
                    pendingQueue_.pop_front();
                }
                batchSize = batch.empty() ? 0 : batch.front().size;
                batchSender = batch.empty() ? 0 : batch.front().sender;
            }
            if (batch.empty()) return;
            std::vector<std::string> batchKeys;
            batchKeys.reserve(batch.size());
            for (auto &p : batch) batchKeys.push_back(std::move(p.key));
            bool degraded = target > static_cast<int>(batchKeys.size());
            RunMgetBatch(batchKeys, batchSize, batchSender);
            if (degraded) {
                metrics_.Record("mget_degraded", 0, 0, 0);
            }
            metrics_.Record("mget_actual_batch_size", 0, 0, batchKeys.size());
        });
        return;
    }

    // Non-distribution mode: original notifyPipeline logic
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
                        metrics_.VerifyFailCounter(), cfg_.instanceId);
    });
}

void NotifyDispatcher::RunMgetBatch(const std::vector<std::string> &keys, uint64_t size,
                                     int sender) {
    PipelineContext ctx;
    ctx.key = keys[0];
    ctx.batchKeys = keys;
    ctx.size = size;
    ctx.senderId = sender;
    if (notifyNeedsData_) {
        auto cacheKey = std::to_string(size) + "_" + std::to_string(sender);
        {
            std::lock_guard<kvtest::mutex> lock(pregenMutex_);
            auto it = pregenData_.find(cacheKey);
            if (it != pregenData_.end()) {
                ctx.data = it->second;
            } else {
                ctx.data = GeneratePatternData(size, sender);
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
                    metrics_.VerifyFailCounter(), cfg_.instanceId);
}

void NotifyDispatcher::StopNow() {
    notifyPool_.StopNow();
    mgetPool_.StopNow();
    if (cacheReader_) cacheReader_->RequestStop();
}
