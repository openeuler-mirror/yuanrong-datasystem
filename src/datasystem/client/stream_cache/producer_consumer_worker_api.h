/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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
 * Description: Implement stream cache base class for producer and consumer.
 */
#ifndef DATASYSTEM_CLIENT_STREAM_CACHE_PRODUCER_CONSUMER_WORKER_API_H
#define DATASYSTEM_CLIENT_STREAM_CACHE_PRODUCER_CONSUMER_WORKER_API_H

#include <memory>
#include "datasystem/client/stream_cache/client_worker_api.h"

namespace datasystem {
namespace client {
namespace stream_cache {
class ProducerConsumerWorkerApi {
public:
    ProducerConsumerWorkerApi(const std::string tenantId, std::shared_ptr<ClientWorkerApi> workerApi);

    /**
     * @brief Get the Lock Id object
     * @return uint32_t lock id
     */
    uint32_t GetLockId()
    {
        return workerApi_->lockId_;
    }

    /**
     * @brief Get the Data Page object
     * @param[in/out] req GetDataPageReqPb info.
     * @param[out] outPage out page.
     * @return Status of the call
     */
    Status GetDataPage(GetDataPageReqPb &req, ShmView &outPage);

    /**
     * @brief Allocate big element memory
     * @param[in] streamName stream name
     * @param[in] producerId producer id
     * @param[in] sizeNeeded need size
     * @param[in] timeoutMs timeout ms
     * @param[out] outView The out Shmview
     * @return Status of the call
     */
    Status AllocBigElementMemory(const std::string &streamName, const std::string &producerId, size_t sizeNeeded,
                                 int64_t timeoutMs, ShmView &outView);

    /**
     * @brief Release big element memory
     * @param[in] streamName stream name
     * @param[in] producerId producer id
     * @param[in] pageView page view to release
     * @return Status of the call
     */
    Status ReleaseBigElementMemory(const std::string &streamName, const std::string &producerId,
                                   const ShmView &pageView);

    /**
     * @brief Send rpc request to worker to create WritePage for producer.
     * @param[in] streamName The name of stream.
     * @param[in] producerId The producer uuid.
     * @param[in] pageId The page id.
     * @param[in] timeoutMs The timeout for the call
     * @param[out] outPage The memory page that producer will send element.
     * @param[out] isFlushOK If the flush operation was successful
     * @param[in] elementsMeta The meta info of element.
     * @return Status of the call.
     */
    Status CreateWritePage(const std::string &streamName, const std::string &producerId, int64_t timeoutMs,
                           const ShmView &curView, ShmView &outPage);

    /**
     * @brief Send rpc request to worker to close producer.
     * @param[in] streamName The name of stream that will be close.
     * @param[in] producerId The name of producer that will be close.
     * @return Status of the call.
     */
    Status CloseProducer(const std::string &streamName, const std::string &producerId);

    /**
     * @brief Send rpc request to worker to close consumer.
     * @param[in] streamName The name of stream that will be close.
     * @param[in] subscriptionName The name of subscription that will be close.
     * @param[in] consumerId The uuid of consumer that will be close.
     * @return Status of the call.
     */
    Status CloseConsumer(const std::string &streamName, const std::string &subscriptionName,
                         const std::string &consumerId);

    /**
     * @brief Get last append cursor in worker consumer.
     * @param[in] streamName Target stream.
     * @param[out] lastAppendCursor Last append cursor in worker consumer.
     * @return Status of the call.
     */
    Status GetLastAppendCursor(const std::string &streamName, uint64_t &lastAppendCursor);

    template <class ReqType>
    Status SetTokenAndTenantId(ReqType &req)
    {
        req.set_tenant_id(tenantId_);
        return workerApi_->SetToken(req);
    }

private:
    std::string tenantId_;
    std::shared_ptr<ClientWorkerApi> workerApi_;
    const std::unordered_set<StatusCode> retryCode_ = { StatusCode::K_RPC_CANCELLED, StatusCode::K_RPC_UNAVAILABLE,
                                                        StatusCode::K_RPC_DEADLINE_EXCEEDED };
};
}  // namespace stream_cache
}  // namespace client
}  // namespace datasystem
#endif