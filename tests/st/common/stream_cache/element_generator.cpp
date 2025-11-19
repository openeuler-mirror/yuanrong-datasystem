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
 * Description: Element Generator.
 */
#include "element_generator.h"

#include <future>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <utility>

#include <openssl/md5.h>

#include "common.h"
#include "datasystem/common/util/strings_util.h"
#include "datasystem/common/util/thread_pool.h"
#include "datasystem/common/util/timer.h"
#include "datasystem/common/log/log.h"

namespace datasystem {
std::string Md5SumStr(uint8_t *data, uint64_t sz)
{
    uint8_t digest[MD5_DIGEST_LENGTH];
    MD5(data, sz, digest);

    std::stringstream result;
    constexpr uint32_t hexWidthForUint8 = 2;
    for (int i : digest) {
        result << std::setw(hexWidthForUint8) << std::setfill('0') << std::hex << i;
    }
    return result.str();
}

std::string Md5Sum(const std::string &str)
{
    return Md5SumStr((uint8_t *)str.data(), str.size());
}

std::string ElementGenerator::GenElement(const std::string &producerId)
{
    return GenElements(producerId, 1).front();
}

ElementBuilder &ElementBuilder::SetProducerId(std::string producerId)
{
    producerId_ = std::move(producerId);
    return *this;
}

ElementBuilder &ElementBuilder::SetSeqNo(uint64_t seqNo)
{
    seqNo_ = seqNo;
    return *this;
}

ElementBuilder &ElementBuilder::SetData(uint8_t *data, uint64_t sz)
{
    data_ = data;
    dataSz_ = sz;
    return *this;
}

std::string ElementBuilder::Build() const
{
    // | lenOfId | id | seqNo | lenData | Data | lenOfMd5 | md5Sum |.
    uint64_t cap = sizeof(uint64_t) * 3 + producerId_.size() + dataSz_;
    std::string str;
    constexpr uint64_t MD5_STR_LEN = 32;
    str.reserve(cap + sizeof(uint64_t) + MD5_STR_LEN);

    // uint64 virtual address.
    uint64_t uint64 = producerId_.size();
    auto ptr = (uint8_t *)(&uint64);
    auto ptr_end = ptr + sizeof(uint64_t);

    // Id.
    std::copy(ptr, ptr_end, std::back_inserter(str));
    std::copy(producerId_.data(), producerId_.data() + producerId_.size(), std::back_inserter(str));

    // SeqNo.
    uint64 = seqNo_;
    std::copy(ptr, ptr_end, std::back_inserter(str));

    // Data.
    uint64 = dataSz_;
    std::copy(ptr, ptr_end, std::back_inserter(str));
    std::copy(data_, data_ + dataSz_, std::back_inserter(str));

    // Md5Sum.
    auto md5Sum = Md5SumStr((uint8_t *)str.data(), cap);
    uint64 = md5Sum.size();

    std::copy(ptr, ptr_end, std::back_inserter(str));
    std::copy(begin(md5Sum), end(md5Sum), std::back_inserter(str));
    return str;
}

ElementGenerator::ElementGenerator(uint64_t maxSz, uint64_t minSz)
    : maxSz_(maxSz), minSz_(minSz), randomData_(), randomBytes_(randomData_.RandomBytes(maxSz * 2))
{
}

std::vector<std::string> ElementGenerator::GenElements(const std::string &producerId, uint64_t numElements,
                                                       int numThreads)
{
    Timer timer;
    std::vector<std::string> res;
    res.resize(numElements);
    auto it = seqNoMap_.find(producerId);
    if (it == seqNoMap_.end()) {
        it = seqNoMap_.emplace(producerId, 0).first;
    }
    auto &seqNo = it->second;
    auto startSeqNo = seqNo;
    std::vector<std::promise<bool>> token(numElements + 1);
    token[0].set_value(true);

    std::mutex mtx;
    std::unique_ptr<ThreadPool> pool;
    std::unique_ptr<ThreadPool> pool2;
    LOG_IF_EXCEPTION_OCCURS(pool = std::make_unique<ThreadPool>(numThreads));
    LOG_IF_EXCEPTION_OCCURS(pool2 = std::make_unique<ThreadPool>(numThreads));
    for (auto i = 0u; i < numElements; i++) {
        std::shared_future<std::string> strFut = pool->Submit([i, this, &mtx, &seqNo, startSeqNo, producerId]() {
            uint64_t sz;
            if (i % 10 == 0) {
                sz = maxSz_;
            } else {
                sz = randomData_.GetRandomUint64(minSz_, maxSz_);
            }
            auto pos = randomData_.GetRandomUint64() % maxSz_;

            auto builder = std::make_shared<ElementBuilder>();
            builder->SetProducerId(producerId).SetData(randomBytes_.data() + pos, sz);
            builder->SetSeqNo(i + startSeqNo);
            {
                std::lock_guard<std::mutex> lck(mtx);
                seqNo++;
            }
            auto str = builder->Build();
            return str;
        });
        pool2->Submit([i, strFut, &token, &res]() {
            token[i].get_future().get();
            res[i] = strFut.get();
            token[i + 1].set_value(true);
        });
    }
    pool.reset();
    pool2.reset();
    LOG(INFO) << FormatString("Stream Gen elapsed: [%.6lf]s", timer.ElapsedSecond());
    return res;
}

ElementView::ElementView(std::string view) : view_(std::move(view))
{
}

Status ElementView::ParseData()
{
    // | lenOfId | id | seqNo | lenData | Data | lenOfMd5 | md5Sum |.
    uint64_t off = 0;
    auto lenOfId = *reinterpret_cast<const uint64_t *>(view_.data() + off);
    off += sizeof(uint64_t);
    CHECK_FAIL_RETURN_STATUS(off + lenOfId < view_.size(), StatusCode::K_RUNTIME_ERROR,
                             FormatString("off: [%zu], lenOfId: [%zu], viewSz: [%zu]", off, lenOfId, view_.size()));
    producerId_ = std::string(view_.data() + off, lenOfId);
    off += lenOfId;
    CHECK_FAIL_RETURN_STATUS(
        off + sizeof(uint64_t) < view_.size(), StatusCode::K_RUNTIME_ERROR,
        FormatString("off: [%zu], lenOfSeqNo: [%zu], viewSz: [%zu]", off, sizeof(uint64_t), view_.size()));
    seqNo_ = *reinterpret_cast<const uint64_t *>(view_.data() + off);
    off += sizeof(uint64_t);

    CHECK_FAIL_RETURN_STATUS(
        off + sizeof(uint64_t) < view_.size(), StatusCode::K_RUNTIME_ERROR,
        FormatString("off: [%zu], lenMeta: [%zu], viewSz: [%zu]", off, sizeof(uint64_t), view_.size()));
    auto lenOfData = *reinterpret_cast<const uint64_t *>(view_.data() + off);
    off += sizeof(uint64_t);

    CHECK_FAIL_RETURN_STATUS(off + lenOfData < view_.size(), StatusCode::K_RUNTIME_ERROR,
                             FormatString("off: [%zu], data: [%zu], viewSz: [%zu]", off, lenOfData, view_.size()));
    data_ = std::string(view_.data() + off, lenOfData);
    off += lenOfData;

    CHECK_FAIL_RETURN_STATUS(
        off + sizeof(uint64_t) < view_.size(), StatusCode::K_RUNTIME_ERROR,
        FormatString("off: [%zu], data: [%zu], viewSz: [%zu]", off, sizeof(uint64_t), view_.size()));
    auto lenOfMd5 = *reinterpret_cast<const uint64_t *>(view_.data() + off);
    off += sizeof(uint64_t);
    md5sum_ = std::string(view_.data() + off, lenOfMd5);

    CHECK_FAIL_RETURN_STATUS(off + lenOfMd5 == view_.size(), StatusCode::K_RUNTIME_ERROR,
                             FormatString("Producer id:[%s], seq no:[%zu], off: [%zu], data: [%zu], viewSz: [%zu]",
                                          producerId_, seqNo_, off, lenOfMd5, view_.size()));
    lenPayload_ = off - sizeof(uint64_t);
    isParsed = true;
    DLOG(INFO) << FormatString("id: [%s], seqNo: [%zu], lenData: [%zu], md5sum: [%s]", producerId_, seqNo_, lenOfData,
                               md5sum_);
    return Status::OK();
}

Status ElementView::VerifyIntegrity()
{
    if (!isParsed) {
        RETURN_IF_NOT_OK(ParseData());
    }
    auto md5Sum = Md5SumStr((uint8_t *)view_.data(), lenPayload_);
    CHECK_FAIL_RETURN_STATUS(md5Sum == md5sum_, StatusCode::K_RUNTIME_ERROR,
                             FormatString("Integrity violation; ground truth: [%s]; got: [%s]", md5sum_, md5Sum));
    return Status::OK();
}

Status ElementView::VerifyFifo(std::unordered_map<std::string, uint64_t> &seqNoMap, uint64_t offset)
{
    if (!isParsed) {
        RETURN_IF_NOT_OK(ParseData());
    }

    std::string producerId{ producerId_.data(), producerId_.size() };
    auto it = seqNoMap.find(producerId);
    if (it == seqNoMap.end()) {
        it = seqNoMap.emplace(producerId, offset).first;
    }

    auto seqNo = it->second;
    CHECK_FAIL_RETURN_STATUS(
        seqNo == seqNo_, StatusCode::K_RUNTIME_ERROR,
        FormatString("FIFO SeqNo violation [%s], got: [%zu], expect: [%zu]", producerId, seqNo_, seqNo));
    it->second++;
    return Status::OK();
}

Status ElementView::VerifyFifoInitOff(std::unordered_map<std::string, uint64_t> &seqNoMap)
{
    if (!isParsed) {
        RETURN_IF_NOT_OK(ParseData());
    }
    std::string producerId{ producerId_.data(), producerId_.size() };
    auto it = seqNoMap.find(producerId);
    if (it == seqNoMap.end()) {
        it = seqNoMap.emplace(producerId, seqNo_).first;
    }

    auto seqNo = it->second;
    CHECK_FAIL_RETURN_STATUS(
        seqNo == seqNo_, StatusCode::K_RUNTIME_ERROR,
        FormatString("FIFO SeqNo violation [%s], gt: [%zu], got: [%zu]", producerId, seqNo_, seqNo));
    it->second++;
    return Status::OK();
}
}  // namespace datasystem
