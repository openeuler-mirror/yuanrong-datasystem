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

#include <fcntl.h>
#include <queue>
#include <mutex>
#include <unistd.h>
#include <condition_variable>
#include <securec.h>
#include <sys/file.h>
#include <arpa/inet.h>

#include "common.h"
#include "../common/binmock/binmock.h"
#include "datasystem/common/util/memory.h"
#include "datasystem/common/util/status_helper.h"
#include "datasystem/common/device/ascend/cann_types.h"
#include "datasystem/common/device/ascend/acl_device_manager.h"
#include "datasystem/common/device/device_pointer_wrapper.h"
#include "datasystem/common/util/file_util.h"
#include "datasystem/common/util/thread_pool.h"

namespace datasystem {
constexpr uint32_t MK_P2P_RANK_NUM = 2;
constexpr uint32_t MK_P2P_SEND_RANK = 0;
constexpr uint32_t MK_P2P_RECV_RANK = 1;

const auto SYNC_EVENT = 1u;
const auto TASK_EVENT = 0u;
class MockEvent {
public:
    MockEvent()
    {
        promise = new std::promise<Status>();
        future = promise->get_future().share();
    }

    ~MockEvent()
    {
        delete promise;
    }

    void SetValue(const Status &status)
    {
        std::unique_lock<std::mutex> lock(mtx);
        if (isSet) {
            return;
        }
        isSet = true;
        promise->set_value(status);
    }
    uint64_t type = TASK_EVENT;
    std::promise<Status> *promise;
    std::shared_future<Status> future;
    volatile bool isSet = false;
    std::mutex mtx;
    uint64_t seq;
};

template <typename T>
class ThreadSafeQueue {
public:
    void Push(T value)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push(value);
    }

    T Pop()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        T value = queue_.front();
        queue_.pop();
        return value;
    }

    bool Empty()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.empty();
    }

    void Clear()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_ = {};
    }

    int GetSize()
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

private:
    std::queue<T> queue_;
    mutable std::mutex mutex_;
};

class PipeCommunicator {
public:
    PipeCommunicator(std::string rootInfo)
    {
        // create to fd
        const ::testing::TestInfo *curTest = ::testing::UnitTest::GetInstance()->current_test_info();
        std::string caseName = std::string(curTest->test_case_name());
        std::string name = std::string(curTest->name());
        rootInfo_ = std::string(LLT_BIN_PATH) + "/ds/" + caseName + "." + name + "/" + rootInfo;
        LOG(ERROR) << "Init rootInfo_ is : " << rootInfo_;
    }

    ~PipeCommunicator()
    {
        if (dataFd_ != -1) {
            close(dataFd_);
        }
        if (ackFd_ != -1) {
            close(ackFd_);
        }
        if (lockFd_ != -1) {
            close(lockFd_);
        }
        std::string dataPipe = rootInfo_ + "/data_pipe";
        std::string ackPipe = rootInfo_ + "/ack_pipe";
        unlink(dataPipe.c_str());
        unlink(ackPipe.c_str());
        LOG_IF_ERROR(RemoveAll(rootInfo_), "Remove file failed");
    }

    Status Init(uint32_t nRanks, uint32_t rank)
    {
        LOG(INFO) << "PipeCommunicator init start with rank : " << rank;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(rank < nRanks, K_INVALID, FormatString("Invalid rank d% !", rank));
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(nRanks == MK_P2P_RANK_NUM, K_INVALID,
                                             FormatString("Invalid nRanks d% !", nRanks));
        constexpr uint32_t mode = 0755;
        if (!FileExist(rootInfo_, mode)) {
            auto createRc = CreateDir(rootInfo_, true, mode);
            if (createRc.IsError() && errno != EEXIST) {
                RETURN_STATUS(K_RUNTIME_ERROR, createRc.ToString());
            }
            std::string dataPipe = rootInfo_ + "/data_pipe";
            std::string ackPipe = rootInfo_ + "/ack_pipe";
            std::string fileLock = rootInfo_ + "/fifo_lock";
            lockFd_ = open(fileLock.c_str(), O_RDWR | O_CREAT, mode);
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(lockFd_ != -1, K_RUNTIME_ERROR, "Open lockFd failed");
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(flock(lockFd_, LOCK_EX) != -1, K_RUNTIME_ERROR, "Failed  lock");
            RETURN_IF_NOT_OK(CreateFifo(dataPipe));
            RETURN_IF_NOT_OK(CreateFifo(ackPipe));
            flock(lockFd_, LOCK_UN);
            isDataSender_ = rank == MK_P2P_SEND_RANK;
            auto dataPolicy = isDataSender_ ? O_WRONLY : O_RDONLY;
            auto ackPolicy = isDataSender_ ? O_RDONLY : O_WRONLY;

            // set data pipe
            dataFd_ = open(dataPipe.c_str(), dataPolicy);
            CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(dataFd_ != -1, K_RUNTIME_ERROR, "open dataFifo failed");
            // set ack pipe
            ackFd_ = open(ackPipe.c_str(), ackPolicy);
            if (ackFd_ == -1) {
                LOG(ERROR) << "ERROR is " << errno;
                close(dataFd_);
                dataFd_ = -1;
                RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, "open ackFifo for read failed");
            }
        } else {
            LOG(WARNING) << FormatString("rootInfo[%s] is exist! rootInfo may create twice!");
        }
        LOG(INFO) << "PipeCommunicator init finish with rank : " << rank;
        return Status::OK();
    }

    Status Send(void *data, uint32_t size)
    {
        auto senderPipe = isDataSender_ ? dataFd_ : ackFd_;
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(senderPipe != -1, K_RUNTIME_ERROR, "Invalid fd for send");
        // Send data size first
        uint64_t dataSize = htonl(size);
        auto byteWritten = write(senderPipe, &dataSize, sizeof(dataSize));
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(byteWritten != -1, K_RUNTIME_ERROR,
                                             FormatString("Failed to send size : %d", size));
        // Send actual data
        byteWritten = write(senderPipe, data, size);
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(byteWritten != -1, K_RUNTIME_ERROR, "Failed to send data");
        return Status::OK();
    }

    Status Receive(void *data, uint32_t size)
    {
        auto receiverPipe = isDataSender_ ? ackFd_ : dataFd_;
        uint64_t dataSize;
        auto bytesRead = read(receiverPipe, &dataSize, sizeof(dataSize));
        if (bytesRead != sizeof(dataSize)) {
            return Status(K_RUNTIME_ERROR, "Get data size failed!");
        }
        dataSize = ntohl(dataSize);
        uint64_t totalBytesRead = 0;
        const uint32_t bufferSize = 1024 * 1024;  // 1MB cache
        char *buffer = new char[bufferSize];
        while (totalBytesRead < dataSize) {
            bytesRead = read(receiverPipe, buffer, bufferSize);
            if (bytesRead == -1) {
                delete[] buffer;
                RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, "Failed to receive");
            }
            auto ret = memcpy_s((char *)data + totalBytesRead, size, buffer, bytesRead);
            if (ret != EOK) {
                delete[] buffer;
                RETURN_STATUS_LOG_ERROR(K_RUNTIME_ERROR, FormatString("Mem copy key component failed, ret = %d", ret));
            }
            totalBytesRead += bytesRead;
        }
        delete[] buffer;
        return Status::OK();
    }

    bool CheckAck()
    {
        bool result = true;
        int bufferLen = 2048;
        char buffer[2048];
        auto rc = Receive(buffer, bufferLen);
        if (rc.IsError()) {
            result = false;
        }
        return result;
    }

private:
    Status CreateFifo(std::string fifoPath)
    {
        auto ret = mkfifo(fifoPath.c_str(), 0755);
        if (ret == -1) {
            LOG(WARNING) << "mk fifo failed";
            if (errno != EEXIST) {
                return Status(K_RUNTIME_ERROR, FormatString("mk fifo error no is : %d", errno));
            }
        }
        return Status::OK();
    }

    bool isDataSender_ = true;
    std::string rootInfo_;
    int dataFd_ = -1;
    int ackFd_ = -1;
    int lockFd_ = -1;
};

class AclDeviceManagerMock : public acl::AclDeviceManager {
    struct cmp {
        bool operator()(void *a, void *b) const
        {
            auto typedEventA = reinterpret_cast<MockEvent *>(a);
            auto typedEventB = reinterpret_cast<MockEvent *>(b);
            return typedEventA->seq < typedEventB->seq;
        }
    };
    using EMap = std::map<void *, std::shared_future<Status>, cmp>;

public:
    static AclDeviceManagerMock *Instance()
    {
        static std::atomic<int> pid;
        static std::once_flag init_;
        static std::unique_ptr<AclDeviceManagerMock> mockInstance_;
        if (pid != getpid()) {
            std::call_once(init_, []() {
                mockInstance_ = std::make_unique<AclDeviceManagerMock>();
                pid = getpid();
            });
        }
        return mockInstance_.get();
    }

    virtual ~AclDeviceManagerMock() noexcept override
    {
        isShutDown = true;
        aclpool_.reset();
        aclsteam_.Empty();
    }

    void Loop()
    {
        aclpool_->Submit([this]() {
            while (!isShutDown) {
                bool tag = false;
                for (auto it : streamEventMap_) {
                    auto &eMap = it.second;
                    for (auto eIt : eMap) {
                        auto typedEvent = static_cast<MockEvent *>(eIt.first);
                        if (typedEvent->isSet) {
                            continue;
                        }
                        if (typedEvent->type == SYNC_EVENT) {
                            LOG(INFO) << "loop auto set event";
                            typedEvent->SetValue(Status::OK());
                            tag = true;
                            continue;
                        }
                        break;
                    }
                }
                if (!tag) {
                    auto ms10 = 10;
                    std::this_thread::sleep_for(std::chrono::milliseconds(ms10));
                }
            }
            LOG(INFO) << "loop shutdown";
        });
    }

    AclDeviceManagerMock() noexcept : AclDeviceManager::AclDeviceManager()
    {
        int minThreadNum = 1;
        int maxThreadNum = 1024;
        aclpool_ = std::make_unique<ThreadPool>(minThreadNum, maxThreadNum);
        Loop();
    }

    Status MemCopyMock(void *dest, size_t destMax, const void *src, size_t count)
    {
        auto ret =
            HugeMemoryCopy(reinterpret_cast<uint8_t *>(dest), destMax, reinterpret_cast<const uint8_t *>(src), count);
        CHECK_FAIL_RETURN_STATUS(ret, K_RUNTIME_ERROR, FormatString("memcpy_s failed.ret:%d", ret));
        return Status::OK();
    }

    Status MallocDeviceMemory(size_t dataSize, void *&deviceData) override
    {
        auto maxSize = 12ul * 1024 * 1024 * 1024;
        if (dataSize > maxSize) {
            return Status(K_RUNTIME_ERROR, " 910b page constraint ");
        }
        deviceData = malloc(dataSize);
        return Status::OK();
    }

    Status DSHcclGetCommAsyncError(HcclComm comm) override
    {
        (void)comm;
        return Status::OK();
    }

    Status MemCopyD2H(void *hostDst, size_t dstMaxSize, const void *devSrc, size_t srcSize) override
    {
        return MemCopyMock(hostDst, dstMaxSize, devSrc, srcSize);
    }

    Status MemCopyH2D(void *devDst, size_t dstMaxSize, const void *hostSrc, size_t srcSize) override
    {
        return MemCopyMock(devDst, dstMaxSize, hostSrc, srcSize);
    }

    Status MemCopyD2D(void *dst, size_t dstMaxSize, const void *src, size_t srcSize) override
    {
        return MemCopyMock(dst, dstMaxSize, src, srcSize);
    }

    Status FreeDeviceMemory(void *deviceData) override
    {
        free(deviceData);
        return Status::OK();
    }

    Status aclInit(const char *configPath) override
    {
        (void)configPath;
        return Status::OK();
    }

    Status SetDeviceIdx(int32_t deviceId) override
    {
        int minDeviceNum = 0;
        int maxDeviceNum = 7;
        if (deviceId >= minDeviceNum && deviceId <= maxDeviceNum) {
            aclDeviceId = deviceId;
            return Status::OK();
        }
        return Status(K_RUNTIME_ERROR, "deviceId error");
    }

    Status aclrtSetDevice(int deviceId) override
    {
        LOG(ERROR) << "mock set device";
        int minDeviceNum = 0;
        int maxDeviceNum = 7;
        if (deviceId >= minDeviceNum && deviceId <= maxDeviceNum) {
            aclDeviceId = deviceId;
            return Status::OK();
        }
        return Status(K_RUNTIME_ERROR, "deviceId error");
    }

    Status VerifyingSha256(const std::string &aclPluginPath) override
    {
        if (aclPluginPath == "xx.so") {
            return Status(K_RUNTIME_ERROR, "cannot find");
        }
        return Status::OK();
    }

    Status aclFinalize() override
    {
        return Status::OK();
    }

    Status aclrtGetDeviceCount(uint32_t *count) override
    {
        int mockNum = 8;
        *count = mockNum;
        return Status::OK();
    }

    Status aclrtQueryDeviceStatus(uint32_t deviceId) override
    {
        uint32_t count;
        aclrtGetDeviceCount(&count);
        if (deviceId >= count) {
            return Status(K_INVALID, "Illegal or abnormal device id.");
        }
        return Status::OK();
    }

    Status aclrtResetDevice(int32_t deviceId) override
    {
        (void)deviceId;
        return Status::OK();
    }

    Status aclrtMemcpyAsync(void *dst, size_t destMax, const void *src, size_t count, aclrtMemcpyKind kind,
                            aclrtStream stream) override
    {
        auto eventPtr = new MockEvent();
        LOG(INFO) << FormatString("aclrtMemcpyAsync start stream:%p ", stream);
        DSAclrtRecordEvent(eventPtr, stream);
        (void)kind;
        aclpool_->Execute([dst, destMax, src, count, stream, this, eventPtr]() {
            auto srcPtr = reinterpret_cast<uint8_t *>(const_cast<void *>(src));
            auto dstPtr = reinterpret_cast<uint8_t *>(dst);
            auto ret = HugeMemoryCopy(dstPtr, destMax, srcPtr, count);
            if (ret.IsError()) {
                LOG(ERROR) << "memcpy_s error";
            }
            eventPtr->SetValue(ret);
            LOG(INFO) << "memcpy_s ok";
        });
        return Status::OK();
    }

    Status aclrtMalloc(void **devPtr, size_t size, aclrtMemMallocPolicy policy) override
    {
        (void)policy;
        *devPtr = new char[size];
        return Status::OK();
    }

    Status aclrtMallocHost(void **devPtr, size_t size) override
    {
        *devPtr = new char[size];
        return Status::OK();
    }

    Status aclrtFreeHost(void *devPtr) override
    {
        free(devPtr);
        return Status::OK();
    }

    Status GetDeviceIdx(int32_t &deviceIdx) override
    {
        deviceIdx = aclDeviceId;
        return Status::OK();
    }

    Status RtDestroyStream(aclrtStream stream) override
    {
        RtDestroyStreamForce(stream);
        return Status::OK();
    }

    Status RtCreateStream(aclrtStream *stream) override
    {
        LOG(INFO) << "Mock RtCreateStream";
        auto stPtr = std::make_shared<ThreadSafeQueue<void *>>();
        *stream = reinterpret_cast<aclrtStream *>(stPtr.get());
        sSMap_.insert({ *stream, stPtr });
        LOG(INFO) << FormatString("stream ptr %p", *stream);
        if (!streamEventMap_.insert({ *stream, {} })) {
            LOG(INFO) << "stream exit";
        }
        return Status::OK();
    }

    Status DSAclrtCreateEvent(aclrtEvent *event) override
    {
        auto *typedEvent = new MockEvent();
        typedEvent->type = SYNC_EVENT;
        *event = typedEvent;
        return Status::OK();
    }

    Status DSAclrtQueryEventStatus(aclrtEvent event) override
    {
        (void)event;
        int streamSize = aclsteam_.GetSize();
        auto itervalMs = 5;
        std::this_thread::sleep_for(std::chrono::milliseconds(itervalMs));
        if (streamSize != 0) {
            return Status(K_ACL_ERROR, "stream contains a jod");
        } else {
            return Status::OK();
        }
    }

    Status DSAclrtRecordEvent(aclrtEvent event, aclrtStream stream) override
    {
        LOG(INFO) << "record Process ID: " << getpid();
        auto *typedEvent = static_cast<MockEvent *>(event);
        typedEvent->seq = globalSeq_;
        globalSeq_++;
        EMap eventPair = GetFutureByEvent(event);
        tbb::concurrent_hash_map<void *, EMap>::accessor accessor;
        LOG(INFO) << FormatString("DSAclrtRecordEvent stream:%p , size:%s ", stream,
                                  streamEventMap_.begin()->second.size());
        if (streamEventMap_.find(accessor, stream)) {
            accessor->second.insert({ eventPair.begin()->first, eventPair.begin()->second });
        } else {
            RETURN_STATUS_LOG_ERROR(K_INVALID, "need Record new event");
        }
        streamEventMap_.find(accessor, stream);
        LOG(INFO) << FormatString("DSAclrtRecordEvent end stream:%p , size:%s ", stream,
                                  streamEventMap_.begin()->second.size());
        return Status::OK();
    }

    Status DSAclrtSynchronizeEvent(aclrtEvent event) override
    {
        auto eventMp = GetFutureByEvent(event);
        RETURN_IF_NOT_OK_PRINT_ERROR_MSG((eventMp.begin()->second).get(), "fut error");
        return Status::OK();
    }

    Status DSAclrtSynchronizeEventWithTimeout(aclrtEvent event, int32_t timeoutMs) override
    {
        auto eventMp = GetFutureByEvent(event);
        if ((eventMp.begin()->second).wait_for(std::chrono::milliseconds(timeoutMs)) == std::future_status::timeout) {
            RETURN_STATUS_LOG_ERROR(K_FUTURE_TIMEOUT, "The future has timed out.");
        } else {
            return Status::OK();
        }
    }

    Status RtSynchronizeStream(aclrtStream stream) override
    {
        tbb::concurrent_hash_map<void *, EMap>::const_accessor readAccessor;
        if (streamEventMap_.find(readAccessor, stream)) {
            auto &map = readAccessor->second;
            LOG(INFO) << FormatString("stream: %p,event size:%s", stream, map.size());
            for (auto i = map.begin(); i != map.end(); i++) {
                auto *typedEvent = reinterpret_cast<MockEvent *>(i->first);
                if (typedEvent->type == SYNC_EVENT) {
                    LOG(INFO) << "auto set sync event";
                    typedEvent->SetValue(Status::OK());
                }
                auto res = (i->second).get();
                if (res.IsError()) {
                    LOG(INFO) << res.ToString();
                }
            }
        } else {
            LOG(ERROR) << "can 't find stream";
        }
        LOG(INFO) << "Finish to get with RtSynchronizeStream";
        return Status::OK();
    }

    Status DSAclrtDestroyEvent(aclrtEvent event) override
    {
        (void)event;
        return Status::OK();
    }
    Status DSHcclSend(void *sendBuf, uint64_t count, HcclDataType dataType, uint32_t destRank, HcclComm comm,
                      aclrtStream stream) override
    {
        (void)destRank;
        (void)stream;
        LOG(INFO) << "Start to send with DSHcclSend.";
        auto streamClientPtr = static_cast<PipeCommunicator *>(comm);
        auto size = GetBytesFromDataType(static_cast<DataType>(static_cast<uint8_t>(dataType)));
        RETURN_IF_NOT_OK(streamClientPtr->Send(sendBuf, size * count));
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(streamClientPtr->CheckAck(), K_RUNTIME_ERROR, "Failed to send");
        LOG(INFO) << "End to send with DSHcclSend.";
        return Status::OK();
    }

    Status DSHcclRecv(void *recvBuf, uint64_t count, HcclDataType dataType, uint32_t srcRank, HcclComm comm,
                      aclrtStream stream) override
    {
        LOG(INFO) << "Start to receive with DSHcclRecv";
        (void)srcRank;
        (void)stream;
        auto streamClientPtr = static_cast<PipeCommunicator *>(comm);
        auto size = GetBytesFromDataType(static_cast<DataType>(static_cast<uint8_t>(dataType)));
        RETURN_IF_NOT_OK(streamClientPtr->Receive(recvBuf, size * count));
        char finish[] = "Finish";
        RETURN_IF_NOT_OK(streamClientPtr->Send(finish, sizeof(finish) / sizeof(finish[0])));
        LOG(INFO) << "End to receive with DSHcclRecv";
        return Status::OK();
    }

    Status DSHcclGetRootInfo(HcclRootInfo *rootInfo) override
    {
        constexpr int32_t maxLen = 4108;
        char invalidData = 'a';  // '`' next is 'a'
        auto pid = getpid();
        int ret = memset_s(rootInfo->internal, maxLen, invalidData, maxLen);
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(ret == EOK, K_RUNTIME_ERROR,
                                             FormatString("Mem set key component failed, ret = %d", ret));
        auto strPid = std::to_string(pid);
        ret = memcpy_s(rootInfo->internal, maxLen, strPid.c_str(), strPid.size());
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(ret == EOK, K_RUNTIME_ERROR,
                                             FormatString("Mem copy key component failed, ret = %d", ret));
        return Status::OK();
    }

    Status DSP2PGetRootInfo(HcclRootInfo *rootInfo) override
    {
        constexpr int32_t maxLen = 4108;
        char invalidData = 'a';  // '`' next is 'a'
        auto pid = getpid();
        int ret = memset_s(rootInfo->internal, maxLen, invalidData, maxLen);
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(ret == EOK, K_RUNTIME_ERROR,
                                             FormatString("Mem set key component failed, ret = %d", ret));
        auto strPid = std::to_string(pid);
        ret = memcpy_s(rootInfo->internal, maxLen, strPid.c_str(), strPid.size());
        CHECK_FAIL_RETURN_STATUS_PRINT_ERROR(ret == EOK, K_RUNTIME_ERROR,
                                             FormatString("Mem copy key component failed, ret = %d", ret));
        return Status::OK();
    }

    Status DSHcclCommInitRootInfo(uint32_t nRanks, const HcclRootInfo *rootInfo, uint32_t rank, HcclComm *comm) override
    {
        std::string strRootInfo = RootInfoToString(rootInfo);
        std::unique_ptr<PipeCommunicator> pipePtr = std::make_unique<PipeCommunicator>(strRootInfo);
        RETURN_IF_NOT_OK(pipePtr->Init(nRanks, rank));
        *comm = pipePtr.release();
        return Status::OK();
    }

    Status CheckPluginOk() override
    {
        return Status::OK();
    }

    Status RtSynchronizeStreamWithTimeout(aclrtStream stream, int32_t timeoutMs) override
    {
        tbb::concurrent_hash_map<void *, EMap>::const_accessor readAccessor;
        LOG(INFO) << "RtSynchronizeStreamWithTimeout with size " << streamEventMap_.size();
        if (streamEventMap_.find(readAccessor, stream)) {
            auto &map = readAccessor->second;
            auto start = std::chrono::steady_clock::now();
            for (auto i = map.begin(); i != map.end(); i++) {
                auto *typedEvent = reinterpret_cast<MockEvent *>(i->first);
                if (typedEvent->type == SYNC_EVENT) {
                    typedEvent->SetValue(Status::OK());
                }
                auto now = std::chrono::steady_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(now - start);
                timeoutMs = timeoutMs - duration.count();
                if (timeoutMs <= 0) {
                    RETURN_STATUS_LOG_ERROR(K_FUTURE_TIMEOUT, "fut time out");
                }
                if ((i->second).wait_for(std::chrono::milliseconds(timeoutMs)) == std::future_status::timeout) {
                    RETURN_STATUS_LOG_ERROR(K_FUTURE_TIMEOUT, "fut time out");
                }
                start = std::chrono::steady_clock::now();
                LOG(INFO) << "Finish to get RtSynchronizeStreamWithTimeout";
            }
        } else {
            LOG(ERROR) << "can 't find stream";
        }
        return Status::OK();
    }

    Status RtDestroyStreamForce(aclrtStream stream) override
    {
        // make sure no one is using this str
        tbb::concurrent_hash_map<void *, EMap>::accessor accessor;
        if (streamEventMap_.find(accessor, stream)) {
            for (auto pair : accessor->second) {
                // Multiple parallelism mutex protected will to be considered at later
                tbb::concurrent_hash_map<void *, std::shared_future<Status>>::accessor eventFutAccessor;
                if (eventFutMap_.find(eventFutAccessor, pair.first)) {
                    eventFutMap_.erase(eventFutAccessor);
                }
            }
            LOG(INFO) << FormatString("RtDestroyStreamForce ptr:%p", stream);
            streamEventMap_.erase(accessor);
            sSMap_.erase(stream);
        }
        return Status::OK();
    }

    void Shutdown() override
    {
        return;
    }

private:
    bool FindAndSetOKEvent(aclrtStream stream)
    {
        tbb::concurrent_hash_map<void *, EMap>::const_accessor readAccessor;
        if (!streamEventMap_.find(readAccessor, stream)) {
            return false;
        }
        auto &map = readAccessor->second;
        if (map.empty()) {
            return false;
        }
        for (auto i = map.begin(); i != map.end(); i++) {
            // check and manager map size
            try {
                auto event = i->first;
                auto *typedEvent = static_cast<MockEvent *>(event);
                typedEvent->SetValue(Status::OK());
            } catch (const std::exception &e) {
                continue;
            }
        }
        return true;
    }

    EMap GetFutureByEvent(aclrtEvent event)
    {
        tbb::concurrent_hash_map<void *, std::shared_future<Status>>::accessor eventFutAccessor;
        EMap mp;
        if (eventFutMap_.find(eventFutAccessor, event)) {
            mp.emplace(event, eventFutAccessor->second);
            return mp;
        }
        auto *typedEvent = static_cast<MockEvent *>(event);
        eventFutMap_.insert({ event, typedEvent->future });
        mp.emplace(event, typedEvent->future);
        return mp;
    }

    std::string RootInfoToString(const HcclRootInfo *rootInfo)
    {
        std::stringstream infoStr;
        for (char i : rootInfo->internal) {
            if (i >= '!' && i <= '`') {
                infoStr << i;
            }
        }
        return infoStr.str();
    }

    int aclDeviceId = -1;
    ThreadSafeQueue<const char *> aclsteam_;
    std::unique_ptr<ThreadPool> aclpool_;
    tbb::concurrent_hash_map<void *, EMap> streamEventMap_;                     // stream, <event[promise], future>
    mutable std::mutex mutex_;                                                  // protect typedEvent->get_future()
    tbb::concurrent_hash_map<void *, std::shared_future<Status>> eventFutMap_;  // event, future
    tbb::concurrent_hash_map<void *, bool> sendRecvRecordMap_;  // *stream
    tbb::concurrent_hash_map<aclrtStream, std::shared_ptr<ThreadSafeQueue<void *>>> sSMap_;
    volatile bool isShutDown = false;
    std::atomic<uint64_t> globalSeq_ = 0;
};

using namespace testing;

}  // namespace datasystem