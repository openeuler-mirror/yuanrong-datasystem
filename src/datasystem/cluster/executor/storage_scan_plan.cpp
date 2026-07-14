/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
 * Licensed under the Apache License, Version 2.0 (the "License");
 */

/**
 * Description: Opaque read-only storage candidate scan plan.
 */
#include "datasystem/cluster/executor/storage_scan_plan_access.h"

#include <utility>

#include "datasystem/common/util/status_helper.h"

namespace datasystem::cluster {

struct StorageScanPlan::Impl {
    std::vector<StorageScanSegment> segments;
};

StorageScanPlan::StorageScanPlan(std::shared_ptr<const Impl> impl) : impl_(std::move(impl))
{
}

std::unique_ptr<StorageScanPlan> StorageScanPlan::CreateHash(const std::vector<TokenRange> &ranges)
{
    auto impl = std::make_shared<Impl>();
    impl->segments.reserve(ranges.size());
    for (const auto &range : ranges) {
        impl->segments.push_back({ range.from, range.end });
    }
    return std::unique_ptr<StorageScanPlan>(new StorageScanPlan(std::move(impl)));
}

std::string StorageScanPlan::DebugString() const
{
    return "storage-scan segment_count=" + std::to_string(impl_ == nullptr ? 0 : impl_->segments.size());
}

Status StorageScanPlanAccess::GetSegments(const StorageScanPlan &plan, std::vector<StorageScanSegment> &segments)
{
    CHECK_FAIL_RETURN_STATUS(plan.impl_ != nullptr && !plan.impl_->segments.empty(), K_INVALID,
                             "invalid or empty topology storage scan plan");
    segments = plan.impl_->segments;
    return Status::OK();
}

}  // namespace datasystem::cluster
