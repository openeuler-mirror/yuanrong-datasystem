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
#ifndef DATASYSTEM_TEST_ST_WORKER_STREAM_CACHE_ELEMENT_GENERATOR_H
#define DATASYSTEM_TEST_ST_WORKER_STREAM_CACHE_ELEMENT_GENERATOR_H

#include <unordered_map>

#include "datasystem/common/util/random_data.h"
#include "datasystem/common/util/status_helper.h"

namespace datasystem {
std::string Md5Sum(const std::string &str);
std::string Md5SumStr(uint8_t *data, uint64_t sz);

/*
 * Element:
 * | lenOfId | id | seqNo | lenData | Data | lenOfMd5 | md5Sum |
 * | lenOfId |  seqNo | lenData | lenOfMd5 | id | Data |  md5Sum |
 * i.e., | Payload | md5 |
 */
class ElementBuilder {
public:
    ElementBuilder() = default;

    ElementBuilder &SetProducerId(std::string producerId);

    ElementBuilder &SetSeqNo(uint64_t seqNo);

    ElementBuilder &SetData(uint8_t *data, uint64_t sz);

    std::string Build() const;

private:
    std::string producerId_;
    uint64_t seqNo_ = 0;
    uint8_t *data_ = nullptr;
    uint64_t dataSz_ = 0;
};

class ElementView {
public:
    explicit ElementView(std::string view);

    Status VerifyFifo(std::unordered_map<std::string, uint64_t> &seqNoMap, uint64_t offset = 0);

    Status VerifyFifoInitOff(std::unordered_map<std::string, uint64_t> &seqNoMap);

    Status VerifyIntegrity();

    Status GetSeqNo(uint64_t &seqNo)
    {
        if (!isParsed) {
            RETURN_IF_NOT_OK(ParseData());
        }
        seqNo = seqNo_;
        return Status::OK();
    }

    Status GetProducerId(std::string &outProducerId)
    {
        if (!isParsed) {
            RETURN_IF_NOT_OK(ParseData());
        }
        outProducerId = producerId_;
        return Status::OK();
    }

private:
    Status ParseData();

    std::string producerId_;
    uint64_t seqNo_ = 0;
    std::string data_;
    std::string md5sum_;
    bool isParsed = false;
    uint64_t lenPayload_ = 0;

    std::string view_;
};

class ElementGenerator {
public:
    explicit ElementGenerator(uint64_t maxSz, uint64_t minSz = 1);

    std::vector<std::string> GenElements(const std::string &producerId, uint64_t numElements, int numThreads = 1);

    std::string GenElement(const std::string &producerId);

private:
    uint64_t maxSz_;
    uint64_t minSz_;
    RandomData randomData_;
    std::vector<uint8_t> randomBytes_;
    std::unordered_map<std::string, uint64_t> seqNoMap_;
};
}  // namespace datasystem

#endif  // DATASYSTEM_ELEMENT_GENERATOR_H
