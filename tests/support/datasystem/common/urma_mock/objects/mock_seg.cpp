/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
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

#include "datasystem/common/urma_mock/objects/mock_seg.h"

#include <sys/mman.h>
#include <unistd.h>

namespace datasystem {
namespace urma_mock {

MockSeg::MockSeg(uint64_t id, MockContext *ctx, uint64_t size, const std::string &key, const std::string &shmName,
                 int shmFd, void *ptr, bool isOwner, MappingOwner mappingOwner)
    : id_(id),
      ctx_(ctx),
      size_(size),
      key_(key),
      shmName_(shmName),
      shmFd_(shmFd),
      ptr_(ptr),
      isOwner_(isOwner),
      mappingOwner_(mappingOwner)
{
}

MockSeg::~MockSeg()
{
    if (ptr_ != nullptr && OwnsMapping()) {
        ::munmap(ptr_, size_);
    }
    ptr_ = nullptr;
    if (shmFd_ >= 0) {
        ::close(shmFd_);
        shmFd_ = -1;
    }
    if (isOwner_ && !shmName_.empty()) {
        ::shm_unlink(shmName_.c_str());
    }
}

uint64_t MockSeg::GetId() const
{
    return id_;
}

MockContext *MockSeg::GetContext() const
{
    return ctx_;
}

uint64_t MockSeg::GetSize() const
{
    return size_;
}

const std::string &MockSeg::GetKey() const
{
    return key_;
}

const std::string &MockSeg::GetShmName() const
{
    return shmName_;
}

int MockSeg::GetShmFd() const
{
    return shmFd_;
}

void *MockSeg::GetPtr() const
{
    return ptr_;
}

bool MockSeg::OwnsMapping() const
{
    return mappingOwner_ == MappingOwner::MOCK_MMAP;
}

int MockSeg::GetMemfdFd() const
{
    return memfdFd_;
}

void MockSeg::SetMemfdFd(int fd)
{
    memfdFd_ = fd;
}

uint64_t MockSeg::GetMemfdOffset() const
{
    return memfdOffset_;
}

void MockSeg::SetMemfdOffset(uint64_t offset)
{
    memfdOffset_ = offset;
}

bool MockSeg::IsRemote() const
{
    return isRemote_;
}

void MockSeg::SetIsRemote(bool v)
{
    isRemote_ = v;
}

uint64_t MockSeg::GetRemoteVa() const
{
    return remoteVa_;
}

void MockSeg::SetRemoteVa(uint64_t v)
{
    remoteVa_ = v;
}

const std::string &MockSeg::GetRemoteClientId() const
{
    return remoteClientId_;
}

void MockSeg::SetRemoteClientId(const std::string &v)
{
    remoteClientId_ = v;
}

const std::string &MockSeg::GetRemoteHost() const
{
    return remoteHost_;
}

void MockSeg::SetRemoteHost(const std::string &v)
{
    remoteHost_ = v;
}

int MockSeg::GetRemotePort() const
{
    return remotePort_;
}

void MockSeg::SetRemotePort(int v)
{
    remotePort_ = v;
}

}  // namespace urma_mock
}  // namespace datasystem
