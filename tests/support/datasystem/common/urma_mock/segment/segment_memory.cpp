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

#include "datasystem/common/urma_mock/segment/segment_memory.h"

#ifdef USE_URMA_MOCK

#ifdef __linux__
#include <linux/memfd.h>
#endif
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <fcntl.h>
#include <unistd.h>

#include <charconv>
#include <cerrno>
#include <cstring>
#include <fstream>
#include <functional>
#include <memory>
#include <utility>

#include "datasystem/common/log/log.h"
#include "datasystem/common/urma_mock/mock_registry.h"
#include "datasystem/common/urma_mock/segment/memfd_resolver.h"
#include "datasystem/common/urma_mock/segment/segment_endpoint_registry.h"
#include "datasystem/common/urma_mock/transport/uds_endpoint_service.h"
#include "datasystem/common/util/strings_util.h"

namespace datasystem {
namespace urma_mock {
namespace {
constexpr mode_t K_SHM_FILE_MODE = 0600;
constexpr int K_HEX_BASE = 16;
constexpr uint64_t K_MEMFD_LOOKUP_SIZE = 4096;

std::string BuildPosixShmName(const std::string &key, uint64_t size, uint64_t wireVa)
{
    std::hash<std::string> hasher;
    auto material = key + ":" + std::to_string(size) + ":" + std::to_string(wireVa);
    return "/urma_mock_seg_" + std::to_string(hasher(material));
}

bool IsMappedRange(uint64_t va, uint64_t len)
{
    if (va == 0 || len == 0 || va > UINT64_MAX - len) {
        return false;
    }
    std::ifstream maps("/proc/self/maps");
    if (!maps.is_open()) {
        return false;
    }
    const uint64_t end = va + len;
    std::string line;
    while (std::getline(maps, line)) {
        auto separator = line.find('-');
        if (separator == std::string::npos) {
            continue;
        }
        uint64_t startAddr = 0;
        uint64_t endAddr = 0;
        auto startResult = std::from_chars(line.data(), line.data() + separator, startAddr, K_HEX_BASE);
        auto endBegin = line.data() + separator + 1;
        auto endResult = std::from_chars(endBegin, line.data() + line.size(), endAddr, K_HEX_BASE);
        if (startResult.ec != std::errc() || startResult.ptr != line.data() + separator || endResult.ec != std::errc()
            || endResult.ptr == endBegin) {
            continue;
        }
        if (va >= startAddr && end <= endAddr) {
            return true;
        }
    }
    return false;
}

bool HasRegisteredImportEndpoint(const ImportEndpoint &ep)
{
    return (!ep.host.empty() && ep.port > 0) || !ep.instanceId.empty();
}

bool HasHostPortImportEndpoint(const ImportEndpoint &ep)
{
    return !ep.host.empty() && ep.port > 0;
}

int CreateMemfdBacking(uint64_t size)
{
#ifdef SYS_memfd_create
    int fd = static_cast<int>(::syscall(SYS_memfd_create, "urma_mock_seg", MFD_CLOEXEC));
    if (fd < 0) {
        LOG(WARNING) << "[MockUrma] memfd_create fallback failed: " << StrErr(errno);
        return -1;
    }
    if (::ftruncate(fd, static_cast<off_t>(size)) != 0) {
        LOG(WARNING) << "[MockUrma] memfd ftruncate fallback failed: " << StrErr(errno);
        ::close(fd);
        return -1;
    }
    return fd;
#else
    (void)size;
    return -1;
#endif
}

struct ImportedMemfd {
    int fd = -1;
    uint64_t va = 0;
    uint64_t len = 0;
    uint64_t offset = 0;
};

ImportedMemfd OpenImportEndpointMemfd(const ImportEndpoint &ep, uint64_t token)
{
    ImportedMemfd imported;
    if (!ep.host.empty() && ep.port > 0) {
        VLOG(1) << "[MockUrma] import_seg host+port UDS path chosen "
                   ": host="
                << ep.host << " port=" << ep.port;
        auto &endpointService = UdsEndpointService::Instance();
        imported.fd = endpointService.ImportSegViaUdsForHost(ep.host, ep.port, token, &imported.va, &imported.len,
                                                             ep.va, &imported.offset);
        if (imported.fd >= 0 || ep.instanceId.empty()) {
            return imported;
        }
        VLOG(1) << "[MockUrma] import_seg host+port path failed, falling back to instanceId=" << ep.instanceId;
    }
    if (!ep.instanceId.empty()) {
        imported.fd = UdsEndpointService::Instance().ImportSegViaUds(ep.instanceId, token, &imported.va, &imported.len,
                                                                     ep.va, &imported.offset);
    }
    return imported;
}

uint64_t ParseImportToken(const std::string &token)
{
    uint64_t tokenNum = 0;
    auto [ptr, ec] = std::from_chars(token.data(), token.data() + token.size(), tokenNum);
    return (ec == std::errc() && ptr == token.data() + token.size()) ? tokenNum : 0;
}

std::shared_ptr<MockSeg> FindLocalPeerSegment(const std::string &token, uint64_t remoteVa)
{
    auto &tables = Tables();
    std::lock_guard<std::mutex> tlk(tables.mu);
    for (auto &[raw, mockSeg] : tables.tseg) {
        (void)raw;
        if (mockSeg != nullptr && !mockSeg->GetKey().empty() && mockSeg->GetKey() == token
            && (remoteVa == 0 || mockSeg->GetRemoteVa() == remoteVa)) {
            return mockSeg;
        }
    }
    return nullptr;
}

}  // namespace

SegmentBacking::~SegmentBacking()
{
    Reset();
}

SegmentBacking::SegmentBacking(SegmentBacking &&other) noexcept
{
    MoveFrom(other);
}

SegmentBacking &SegmentBacking::operator=(SegmentBacking &&other) noexcept
{
    if (this != &other) {
        Reset();
        MoveFrom(other);
    }
    return *this;
}

void SegmentBacking::Reset()
{
    if (ownsResource_) {
        if (ptr_ != nullptr && ptr_ != MAP_FAILED && size_ != 0 && mappingOwner_ == MockSeg::MappingOwner::MOCK_MMAP) {
            (void)::munmap(ptr_, size_);
        }
        if (shmFd_ >= 0) {
            (void)::close(shmFd_);
        }
        if (isOwner_ && !shmName_.empty()) {
            (void)::shm_unlink(shmName_.c_str());
        }
    }
    shmName_.clear();
    shmFd_ = -1;
    ptr_ = nullptr;
    size_ = 0;
    isOwner_ = false;
    mappingOwner_ = MockSeg::MappingOwner::MOCK_MMAP;
    wireVa_ = 0;
    memfdOffset_ = 0;
    memfdFd_ = -1;
    ownsResource_ = false;
}

void SegmentBacking::ReleaseOwnership()
{
    ownsResource_ = false;
}

void *SegmentBacking::GetPtr() const
{
    return ptr_;
}

uint64_t SegmentBacking::GetSize() const
{
    return size_;
}

const std::string &SegmentBacking::GetShmName() const
{
    return shmName_;
}

int SegmentBacking::GetShmFd() const
{
    return shmFd_;
}

bool SegmentBacking::IsOwner() const
{
    return isOwner_;
}

MockSeg::MappingOwner SegmentBacking::GetMappingOwner() const
{
    return mappingOwner_;
}

uint64_t SegmentBacking::GetWireVa() const
{
    return wireVa_;
}

uint64_t SegmentBacking::GetMemfdOffset() const
{
    return memfdOffset_;
}

int SegmentBacking::GetMemfdFd() const
{
    return memfdFd_;
}

void SegmentBacking::SetMapping(const std::string &shmName, int shmFd, void *ptr, uint64_t size, bool isOwner,
                                MockSeg::MappingOwner mappingOwner, uint64_t wireVa)
{
    Reset();
    shmName_ = shmName;
    shmFd_ = shmFd;
    ptr_ = ptr;
    size_ = size;
    isOwner_ = isOwner;
    mappingOwner_ = mappingOwner;
    wireVa_ = wireVa;
    ownsResource_ = true;
}

void SegmentBacking::SetMemfd(int fd, uint64_t offset)
{
    memfdFd_ = fd;
    memfdOffset_ = offset;
}

void SegmentBacking::MoveFrom(SegmentBacking &other)
{
    shmName_ = std::move(other.shmName_);
    shmFd_ = other.shmFd_;
    ptr_ = other.ptr_;
    size_ = other.size_;
    isOwner_ = other.isOwner_;
    mappingOwner_ = other.mappingOwner_;
    wireVa_ = other.wireVa_;
    memfdOffset_ = other.memfdOffset_;
    memfdFd_ = other.memfdFd_;
    ownsResource_ = other.ownsResource_;
    other.shmFd_ = -1;
    other.ptr_ = nullptr;
    other.size_ = 0;
    other.isOwner_ = false;
    other.mappingOwner_ = MockSeg::MappingOwner::MOCK_MMAP;
    other.wireVa_ = 0;
    other.memfdOffset_ = 0;
    other.memfdFd_ = -1;
    other.ownsResource_ = false;
}

SegmentImportResult SegmentImportResult::Failed()
{
    return {};
}

SegmentImportResult SegmentImportResult::Backing(SegmentBacking &&backing, const ImportEndpoint &ep, bool hasEndpoint)
{
    SegmentImportResult result;
    result.kind_ = SegmentImportKind::BACKING;
    result.backing_ = std::move(backing);
    result.endpoint_ = ep;
    result.hasEndpoint_ = hasEndpoint;
    return result;
}

SegmentImportResult SegmentImportResult::Placeholder(const ImportEndpoint &ep)
{
    SegmentImportResult result;
    result.kind_ = SegmentImportKind::PLACEHOLDER;
    result.endpoint_ = ep;
    result.hasEndpoint_ = true;
    return result;
}

SegmentImportKind SegmentImportResult::GetKind() const
{
    return kind_;
}

SegmentBacking &SegmentImportResult::GetBacking()
{
    return backing_;
}

const ImportEndpoint &SegmentImportResult::GetEndpoint() const
{
    return endpoint_;
}

bool SegmentImportResult::HasEndpoint() const
{
    return hasEndpoint_;
}

const SegmentMemoryManager &SegmentMemoryManager::Instance()
{
    static const SegmentMemoryManager manager;
    return manager;
}

bool SegmentMemoryManager::CreateLocal(uint64_t size, const std::string &key, uint64_t businessVa,
                                       SegmentBacking &backing) const
{
    if (size == 0) {
        return false;
    }
    backing.Reset();
    if (businessVa != 0 && AdoptBusinessMemfd(size, businessVa, backing)) {
        return true;
    }
    if (businessVa != 0 && BorrowMappedRange(size, businessVa, backing)) {
        return true;
    }
    if (CreateMockMemfd(size, businessVa, backing)) {
        return true;
    }
    return CreatePosixShm(size, key, businessVa, backing);
}

bool SegmentMemoryManager::ImportViaEndpoint(const ::datasystem::urma_mock::ImportEndpoint &ep, uint64_t token,
                                             uint64_t size, SegmentBacking &backing) const
{
    if (size == 0) {
        return false;
    }
    backing.Reset();
    auto imported = OpenImportEndpointMemfd(ep, token);
    const uint64_t importedSize = imported.len != 0 ? imported.len : size;
    if (imported.fd < 0 || imported.va == 0 || imported.len < size) {
        LOG(WARNING) << "[MockUrma] import_seg UDS bad va/len va=0x" << std::hex << imported.va << " len=" << std::dec
                     << imported.len << " size=" << size;
        if (imported.fd >= 0) {
            ::close(imported.fd);
        }
        return false;
    }
    void *ptr = ::mmap(nullptr, importedSize, PROT_READ | PROT_WRITE, MAP_SHARED, imported.fd,
                       static_cast<off_t>(imported.offset));
    if (ptr == MAP_FAILED) {
        LOG(ERROR) << "[MockUrma] import_seg relocated mmap failed for wire va=0x" << std::hex << imported.va
                   << std::dec << " size=" << importedSize << " offset=" << imported.offset << " errno=" << errno;
        ::close(imported.fd);
        return false;
    }
    ::close(imported.fd);
    backing.SetMapping("/memfd:remote-via-uds", -1, ptr, importedSize, false, MockSeg::MappingOwner::MOCK_MMAP,
                       imported.va);
    backing.SetMemfd(-1, imported.offset);
    VLOG(1) << "[MockUrma] import_seg relocated mmap ok wire va=0x" << std::hex << imported.va << " ptr=" << ptr
            << std::dec << " size=" << importedSize << " offset=" << imported.offset;
    return true;
}

bool SegmentMemoryManager::ImportLocal(const MockSeg &localPeerSeg, uint64_t size, SegmentBacking &backing) const
{
    if (size == 0) {
        return false;
    }
    backing.Reset();
    int srcFd = localPeerSeg.GetMemfdFd() >= 0 ? localPeerSeg.GetMemfdFd() : localPeerSeg.GetShmFd();
    int fd = srcFd >= 0 ? ::dup(srcFd) : -1;
    if (fd < 0) {
        return false;
    }
    void *ptr = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd,
                       static_cast<off_t>(localPeerSeg.GetMemfdOffset()));
    if (ptr == MAP_FAILED) {
        LOG(ERROR) << "[MockUrma] import_seg local alias mmap failed for " << localPeerSeg.GetShmName() << ": "
                   << StrErr(errno);
        ::close(fd);
        return false;
    }
    backing.SetMapping(localPeerSeg.GetShmName(), fd, ptr, size, false, MockSeg::MappingOwner::MOCK_MMAP,
                       localPeerSeg.GetRemoteVa());
    backing.SetMemfd(-1, localPeerSeg.GetMemfdOffset());
    if (localPeerSeg.GetMemfdFd() >= 0) {
        backing.SetMemfd(fd, localPeerSeg.GetMemfdOffset());
    }
    VLOG(1) << "[MockUrma] import_seg local alias shm=" << localPeerSeg.GetShmName() << " size=" << size << " fd=" << fd
            << " ptr=" << ptr;
    return true;
}

bool SegmentMemoryManager::ImportPosix(const std::string &token, uint64_t size, uint64_t remoteVa,
                                       SegmentBacking &backing) const
{
    if (size == 0) {
        return false;
    }
    backing.Reset();
    auto shmName = BuildPosixShmName(token, size, remoteVa);
    int fd = ::shm_open(shmName.c_str(), O_RDWR, K_SHM_FILE_MODE);
    if (fd < 0) {
        LOG(ERROR) << "[MockUrma] import_seg shm_open failed for " << shmName << ": " << StrErr(errno);
        return false;
    }
    void *ptr = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (ptr == MAP_FAILED) {
        LOG(ERROR) << "[MockUrma] import_seg mmap failed for " << shmName << ": " << StrErr(errno);
        ::close(fd);
        return false;
    }
    backing.SetMapping(shmName, fd, ptr, size, false, MockSeg::MappingOwner::MOCK_MMAP, remoteVa);
    VLOG(1) << "[MockUrma] import_seg shm=" << shmName << " size=" << size << " fd=" << fd << " ptr=" << ptr
            << " (alias)";
    return true;
}

bool SegmentMemoryManager::AdoptBusinessMemfd(uint64_t size, uint64_t businessVa, SegmentBacking &backing) const
{
    MemfdMapping memfdMapping;
    int memfdFd = LookupMemfdFd(businessVa, K_MEMFD_LOOKUP_SIZE, "datasystem", &memfdMapping);
    if (memfdFd < 0) {
        return false;
    }
    backing.SetMapping("/memfd:datasystem", memfdFd, reinterpret_cast<void *>(businessVa), size, false,
                       MockSeg::MappingOwner::BORROWED, businessVa);
    backing.SetMemfd(memfdFd, memfdMapping.offset);
    LOG(INFO) << "[MockUrma] register_seg adopted business memfd fd=" << memfdFd << " ptr=" << backing.GetPtr()
              << " size=" << size << " businessVa=0x" << std::hex << businessVa << std::dec
              << " offset=" << memfdMapping.offset;
    return true;
}

bool SegmentMemoryManager::BorrowMappedRange(uint64_t size, uint64_t businessVa, SegmentBacking &backing) const
{
    if (!IsMappedRange(businessVa, size)) {
        return false;
    }
    int memfdFd = CreateMemfdBacking(size);
    if (memfdFd < 0) {
        return false;
    }
    void *ptr =
        ::mmap(reinterpret_cast<void *>(businessVa), size, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FIXED, memfdFd, 0);
    if (ptr == MAP_FAILED) {
        LOG(WARNING) << "[MockUrma] mapped-range memfd mmap failed: " << StrErr(errno);
        ::close(memfdFd);
        return false;
    }
    backing.SetMapping("/memfd:urma_mock_seg", memfdFd, ptr, size, false, MockSeg::MappingOwner::BORROWED, businessVa);
    backing.SetMemfd(memfdFd, 0);
    LOG(INFO) << "[MockUrma] register_seg mapped-range memfd fd=" << memfdFd << " ptr=" << ptr << " size=" << size
              << " businessVa=0x" << std::hex << businessVa << std::dec;
    return true;
}

bool SegmentMemoryManager::CreateMockMemfd(uint64_t size, uint64_t businessVa, SegmentBacking &backing) const
{
    int memfdFd = CreateMemfdBacking(size);
    if (memfdFd < 0) {
        return false;
    }
    void *ptr = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, memfdFd, 0);
    if (ptr == MAP_FAILED) {
        LOG(WARNING) << "[MockUrma] memfd fallback mmap failed: " << StrErr(errno);
        ::close(memfdFd);
        return false;
    }
    backing.SetMapping("/memfd:urma_mock_seg", memfdFd, ptr, size, false, MockSeg::MappingOwner::MOCK_MMAP,
                       reinterpret_cast<uint64_t>(ptr));
    backing.SetMemfd(memfdFd, 0);
    LOG(INFO) << "[MockUrma] register_seg memfd fallback fd=" << memfdFd << " ptr=" << ptr << " size=" << size
              << " businessVa=0x" << std::hex << businessVa << std::dec;
    return true;
}

bool SegmentMemoryManager::CreatePosixShm(uint64_t size, const std::string &key, uint64_t businessVa,
                                          SegmentBacking &backing) const
{
    auto shmName = BuildPosixShmName(key, size, businessVa);
    ::shm_unlink(shmName.c_str());
    int shmFd = ::shm_open(shmName.c_str(), O_CREAT | O_RDWR, K_SHM_FILE_MODE);
    if (shmFd < 0) {
        LOG(ERROR) << "[MockUrma] shm_open failed for " << shmName << ": " << StrErr(errno);
        return false;
    }
    if (::ftruncate(shmFd, static_cast<off_t>(size)) != 0) {
        LOG(ERROR) << "[MockUrma] ftruncate failed: " << StrErr(errno);
        ::close(shmFd);
        ::shm_unlink(shmName.c_str());
        return false;
    }
    void *ptr = ::mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_SHARED, shmFd, 0);
    if (ptr == MAP_FAILED) {
        LOG(ERROR) << "[MockUrma] mmap failed: " << StrErr(errno);
        ::close(shmFd);
        ::shm_unlink(shmName.c_str());
        return false;
    }
    backing.SetMapping(shmName, shmFd, ptr, size, true, MockSeg::MappingOwner::MOCK_MMAP,
                       reinterpret_cast<uint64_t>(ptr));
    LOG(INFO) << "[MockUrma] register_seg shm=" << shmName << " size=" << size << " fd=" << shmFd << " ptr=" << ptr
              << " (shm fallback)";
    return true;
}

const SegmentImporter &SegmentImporter::Instance()
{
    static const SegmentImporter importer;
    return importer;
}

SegmentImportResult SegmentImporter::Resolve(const SegmentImportRequest &request) const
{
    if (request.size == 0) {
        return SegmentImportResult::Failed();
    }
    const auto &segmentMemory = SegmentMemoryManager::Instance();
    uint64_t tokenNum = ParseImportToken(request.token);
    if (tokenNum != 0) {
        auto ep = UdsEndpointService::Instance().LookupImportEndpoint(tokenNum, request.remoteVa);
        VLOG(1) << "[MockUrma] import_seg path-select token=" << tokenNum << " host=" << ep.host << " port=" << ep.port
                << " instanceId=" << ep.instanceId << " useHostPort=" << (!ep.host.empty() && ep.port > 0)
                << " useInstanceId=" << !ep.instanceId.empty();
        if (HasRegisteredImportEndpoint(ep)) {
            SegmentBacking backing;
            if (segmentMemory.ImportViaEndpoint(ep, tokenNum, request.size, backing)) {
                VLOG(1) << "[MockUrma] import_seg via UDS instance=" << ep.instanceId << " clientId=" << ep.clientId
                        << " token=" << tokenNum << " va=0x" << std::hex << backing.GetWireVa() << " len=" << std::dec
                        << backing.GetSize() << " ptr=" << backing.GetPtr();
                return SegmentImportResult::Backing(std::move(backing), ep, true);
            }

            LOG(ERROR) << "[MockUrma] import_seg UDS failed for registered endpoint token=" << tokenNum
                       << " host=" << ep.host << " port=" << ep.port << " instanceId=" << ep.instanceId
                       << "; skip local alias fallback";
            if (!HasHostPortImportEndpoint(ep)) {
                auto refreshedEp = SegmentEndpointRegistry::Instance().Lookup(tokenNum, request.remoteVa);
                if (HasRegisteredImportEndpoint(refreshedEp) && refreshedEp.instanceId != ep.instanceId) {
                    SegmentBacking backing;
                    if (segmentMemory.ImportViaEndpoint(refreshedEp, tokenNum, request.size, backing)) {
                        VLOG(1) << "[MockUrma] import_seg recovered via refreshed endpoint instance="
                                << refreshedEp.instanceId << " token=" << tokenNum << " va=0x" << std::hex
                                << backing.GetWireVa() << " len=" << std::dec << backing.GetSize()
                                << " ptr=" << backing.GetPtr();
                        return SegmentImportResult::Backing(std::move(backing), refreshedEp, true);
                    }
                }
                VLOG(1) << "[MockUrma] import_seg creates local-only remote placeholder token=" << tokenNum
                        << " instanceId=" << ep.instanceId << " va=0x" << std::hex << request.remoteVa << std::dec;
                return SegmentImportResult::Placeholder(ep);
            }
            return SegmentImportResult::Failed();
        }
    }

    auto localPeerSeg = FindLocalPeerSegment(request.token, request.remoteVa);
    if (localPeerSeg != nullptr) {
        SegmentBacking backing;
        if (segmentMemory.ImportLocal(*localPeerSeg, request.size, backing)) {
            return SegmentImportResult::Backing(std::move(backing), ImportEndpoint{}, false);
        }
    }
    if (tokenNum != 0 && request.remoteVa != 0) {
        ImportEndpoint ep;
        ep.va = request.remoteVa;
        ep.len = request.size;
        VLOG(1) << "[MockUrma] import_seg creates remote placeholder token=" << tokenNum << " va=0x" << std::hex
                << request.remoteVa << std::dec;
        return SegmentImportResult::Placeholder(ep);
    }

    SegmentBacking backing;
    if (!segmentMemory.ImportPosix(request.token, request.size, request.remoteVa, backing)) {
        return SegmentImportResult::Failed();
    }
    return SegmentImportResult::Backing(std::move(backing), ImportEndpoint{}, false);
}

}  // namespace urma_mock
}  // namespace datasystem

#endif  // USE_URMA_MOCK
