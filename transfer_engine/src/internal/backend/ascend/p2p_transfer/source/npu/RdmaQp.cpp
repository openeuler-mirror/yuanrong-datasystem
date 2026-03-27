/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#include "npu/RdmaQp.h"
#include "npu/RaWrapper.h"
#include <chrono>
#include <thread>
#include <unistd.h>

RdmaQp::~RdmaQp()
{
    for (auto it = registeredMrs.begin(); it != registeredMrs.end(); ++it) {
        RaMrDeRegWrapper(qpHandle, &it->second);
    }

    if (status >= RdmaQpStatus::QP_INITIALIZED) {
        RaQpDestroyWrapper(qpHandle);
    }
}

Status RdmaQp::create(void *rdmaHandle)
{
    if (status != RdmaQpStatus::QP_UNINITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma qp is already initialized");
    }

    struct qp_ext_attrs qpAttrs {};
    qpAttrs.qp_mode = RA_OP_QP_MODE_EXT;
    const int kQpVersion = 1;
    qpAttrs.version = kQpVersion;  // QP_CREATE_WITH_ATTR_VERSION
    const int kSendCqDepth = 32768;
    qpAttrs.cq_attr.send_cq_depth = kSendCqDepth;
    const int kRecvCqDepth = 128;
    qpAttrs.cq_attr.recv_cq_depth = kRecvCqDepth;
    const uint32_t kMaxInlineData = 32;
    qpAttrs.qp_attr.cap.max_inline_data = kMaxInlineData;
    const uint32_t kMaxSendSge = 1;
    qpAttrs.qp_attr.cap.max_send_sge = kMaxSendSge;
    const uint32_t kMaxRecvWr = 128;
    qpAttrs.qp_attr.cap.max_recv_wr = kMaxRecvWr;
    const uint32_t kMaxRecvSge = 1;
    qpAttrs.qp_attr.cap.max_recv_sge = kMaxRecvSge;
    qpAttrs.qp_attr.qp_type = IBV_QPT_RC;
    const uint32_t kMaxSendWr = 32768;
    qpAttrs.qp_attr.cap.max_send_wr = kMaxSendWr;

    CHECK_STATUS(RaQpCreateWithAttrsWrapper(rdmaHandle, &qpAttrs, &qpHandle));

    unsigned long long notifyBaseSize;
    CHECK_STATUS(RaGetNotifyBaseAddrWrapper(rdmaHandle, &notifyBaseVa, &notifyBaseSize));

    struct qos_attr qosAttr {};
    const unsigned char kTrafficClass = 132;
    qosAttr.tc = kTrafficClass;
    const unsigned char kServiceLevel = 4;
    qosAttr.sl = kServiceLevel;
    CHECK_STATUS(RaSetQpAttrQosWrapper(qpHandle, &qosAttr));

    uint32_t timeOut = 20;
    CHECK_STATUS(RaSetQpAttrTimeoutWrapper(qpHandle, &timeOut));

    uint32_t retryCount = 7;
    CHECK_STATUS(RaSetQpAttrRetryCntWrapper(qpHandle, &retryCount));

    status = RdmaQpStatus::QP_INITIALIZED;

    return Status::Success();
}

Status RdmaQp::registerMemoryRegion(void *addr, uint32_t size)
{
    if (status == RdmaQpStatus::QP_UNINITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma qp is not yet initialized");
    }

    struct mr_info inMrInfo {};
    inMrInfo.addr = addr;
    inMrInfo.size = size;
    inMrInfo.access = RA_ACCESS_LOCAL_WRITE | RA_ACCESS_REMOTE_WRITE | RA_ACCESS_REMOTE_READ;
    CHECK_STATUS(RaMrRegWrapper(qpHandle, &inMrInfo));

    registeredMrs[addr] = inMrInfo;

    return Status::Success();
}

Status RdmaQp::getMemoryRegionInfo(void *startAddr, struct mr_info *info)
{
    auto it = registeredMrs.find(startAddr);
    if (it == registeredMrs.end()) {
        return Status::Error(ErrorCode::NOT_FOUND, "memory region not registered");
    }

    *info = it->second;

    return Status::Success();
}

Status RdmaQp::connect(void *socketFdHandle)
{
    if (status != RdmaQpStatus::QP_INITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma qp is not initialized");
    }

    CHECK_STATUS(RaQpConnectAsyncWrapper(qpHandle, socketFdHandle));

    status = RdmaQpStatus::QP_CONNECTING;

    return Status::Success();
}

Status RdmaQp::getStatus(ra_qp_status *qpStatus)
{
    if (status == RdmaQpStatus::QP_UNINITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma qp is not initialized");
    }

    CHECK_STATUS(RaGetQpStatusWrapper(qpHandle, qpStatus));

    if (*qpStatus == RA_QP_STATUS_CONNECTED) {
        status = RdmaQpStatus::QP_CONNECTED;
    }

    return Status::Success();
}

std::string ra_qp_status_to_string(ra_qp_status status)
{
    switch (status) {
        case RA_QP_STATUS_DISCONNECT:
            return "RA_QP_STATUS_DISCONNECT";
        case RA_QP_STATUS_CONNECTED:
            return "RA_QP_STATUS_CONNECTED";
        case RA_QP_STATUS_TIMEOUT:
            return "RA_QP_STATUS_TIMEOUT";
        case RA_QP_STATUS_CONNECTING:
            return "RA_QP_STATUS_CONNECTING";
        case RA_QP_STATUS_REM_FD_CLOSE:
            return "RA_QP_STATUS_REM_FD_CLOSE";
        case RA_QP_STATUS_PAUSE:
            return "RA_QP_STATUS_PAUSE";
        default:
            return "UNKNOWN_RA_QP_STATUS";
    }
}

Status RdmaQp::waitReady(uint32_t timeOutMs)
{
    if (status == RdmaQpStatus::QP_UNINITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "qp has not been initialized yet");
    }

    auto startTime = std::chrono::steady_clock::now();
    auto timeOutDuration = std::chrono::milliseconds(timeOutMs);

    ra_qp_status qpStatus = RA_QP_STATUS_CONNECTING;
    while (qpStatus == RA_QP_STATUS_CONNECTING) {
        auto currentTime = std::chrono::steady_clock::now();
        if (timeOutMs > 0 && currentTime - startTime >= timeOutDuration) {
            return Status::Error(ErrorCode::TIMEOUT, "Timeout waiting for socket to connect.");
        }

        CHECK_STATUS(this->getStatus(&qpStatus));
    }

    if (qpStatus != RA_QP_STATUS_CONNECTED) {
        return Status::Error(ErrorCode::INTERNAL_ERROR, "qp failed to connect " + ra_qp_status_to_string(qpStatus));
    }

    return Status::Success();
}

Status RdmaQp::getNotifyBaseAddress(unsigned long long *notifyBaseAddr)
{
    if (status == RdmaQpStatus::QP_UNINITIALIZED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma qp is not initialized");
    }

    *notifyBaseAddr = notifyBaseVa;

    return Status::Success();
}

Status RdmaQp::dispatchRdmaOpFfts(p2p::DispatcherFFTS *dispatcher, uint64_t srcAddr, uint64_t dstAddr, uint32_t length,
                                  uint32_t op, int32_t flag, uint32_t *rdmaTaskId)
{
    if (status != RdmaQpStatus::QP_CONNECTED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma qp is not connected");
    }

    struct sg_list list = { 0 };
    list.addr = srcAddr;
    list.len = length;

    struct send_wr notWr {};
    notWr.buf_list = &list;
    notWr.buf_num = 1;
    notWr.dst_addr = dstAddr;
    notWr.op = op;
    notWr.send_flag = flag;

    struct send_wr_rsp notWrRsp {};

    CHECK_STATUS(RaSendWrWrapper(qpHandle, &notWr, &notWrRsp));

    dispatcher->RdmaSend(notWrRsp.db.db_index, notWrRsp.db.db_info, notWr, rdmaTaskId);
    return Status::Success();
}

Status RdmaQp::execRdmaOp(uint64_t srcAddr, uint64_t dstAddr, uint32_t length, uint32_t op, int32_t flag,
                          rtStream_t stm)
{
    if (status != RdmaQpStatus::QP_CONNECTED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma qp is not connected");
    }

    struct sg_list list = { 0 };
    list.addr = srcAddr;
    list.len = length;

    struct send_wr notWr {};
    notWr.buf_list = &list;
    notWr.buf_num = 1;
    notWr.dst_addr = dstAddr;
    notWr.op = op;
    notWr.send_flag = flag;

    struct send_wr_rsp notWrRsp {};

    CHECK_STATUS(RaSendWrWrapper(qpHandle, &notWr, &notWrRsp));
    ACL_CHECK_STATUS(rtRDMADBSend(notWrRsp.db.db_index, notWrRsp.db.db_info, stm));

    return Status::Success();
}

Status RdmaQp::dispatchTypicalRdmaOpFfts(p2p::DispatcherFFTS *dispatcher, uint64_t srcAddr, uint64_t dstAddr,
                                         uint32_t length, uint32_t op, int32_t flag, uint32_t lkey, uint32_t rkey,
                                         uint32_t *rdmaTaskId)
{
    if (status != RdmaQpStatus::QP_CONNECTED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma qp is not connected");
    }

    struct sg_list list = { 0 };
    list.addr = srcAddr;
    list.len = length;
    list.lkey = lkey;

    struct send_wr notWr {};
    notWr.buf_list = &list;
    notWr.buf_num = 1;
    notWr.dst_addr = dstAddr;
    notWr.op = op;
    notWr.rkey = rkey;
    notWr.send_flag = flag;

    struct send_wr_rsp notWrRsp {};

    CHECK_STATUS(RaTypicalSendWrWrapper(qpHandle, &notWr, &notWrRsp));

    dispatcher->RdmaSend(notWrRsp.db.db_index, notWrRsp.db.db_info, notWr, rdmaTaskId);
    return Status::Success();
}

Status RdmaQp::execTypicalRdmaOp(uint64_t srcAddr, uint64_t dstAddr, uint32_t length, uint32_t op, int32_t flag,
                                 uint32_t lkey, uint32_t rkey, rtStream_t stm)
{
    if (status != RdmaQpStatus::QP_CONNECTED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma qp is not connected");
    }

    struct sg_list list = { 0 };
    list.addr = srcAddr;
    list.len = length;
    list.lkey = lkey;

    struct send_wr notWr {};
    notWr.buf_list = &list;
    notWr.buf_num = 1;
    notWr.dst_addr = dstAddr;
    notWr.op = op;
    notWr.rkey = rkey;
    notWr.send_flag = flag;

    struct send_wr_rsp notWrRsp {};

    CHECK_STATUS(RaTypicalSendWrWrapper(qpHandle, &notWr, &notWrRsp));
    ACL_CHECK_STATUS(rtRDMADBSend(notWrRsp.db.db_index, notWrRsp.db.db_info, stm));

    return Status::Success();
}

Status RdmaQp::execRdmaOps(std::vector<uint64_t> srcAddrs, std::vector<uint64_t> dstAddrs,
                           std::vector<uint32_t> lengths, std::vector<uint32_t> ops, std::vector<int32_t> flags,
                           rtStream_t stm)
{
    if (status != RdmaQpStatus::QP_CONNECTED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma qp is not connected");
    }

    size_t numWrites = srcAddrs.size();
    if (dstAddrs.size() != numWrites || lengths.size() != numWrites) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "src, dst and length vectors must be of the same size");
    }

    if (numWrites == 0) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "src, dst and length vectors must contain at least 1 element");
    }

    struct send_wrlist_data_ext wrs[numWrites] = {};

    for (int i = 0; i < numWrites; i++) {
        wrs[i].dst_addr = dstAddrs[i];
        wrs[i].op = ops[i];
        wrs[i].send_flags = flags[i];
        wrs[i].mem_list.addr = srcAddrs[i];  // unsafe, don't use in prod ;)
        wrs[i].mem_list.len = lengths[i];
    }

    struct send_wr_rsp wrRsps[numWrites] = {};
    uint32_t completeNum = 0;
    CHECK_STATUS(RaSendWrlistExtWrapper(qpHandle, wrs, wrRsps, numWrites, &completeNum));
    if (completeNum != numWrites) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "Not all writes were submitted successfully");
    }

    for (int i = 0; i < completeNum; i++) {
        ACL_CHECK_STATUS(rtRDMADBSend(wrRsps[i].db.db_index, wrRsps[i].db.db_info, stm));
    }

    return Status::Success();
}

Status RdmaQp::dispatchRdmaOpsFfts(p2p::DispatcherFFTS *dispatcher, std::vector<uint64_t> srcAddrs,
                                   std::vector<uint64_t> dstAddrs, std::vector<uint32_t> lengths,
                                   std::vector<uint32_t> ops, std::vector<int32_t> flags, uint32_t *lastTaskId)
{
    if (status != RdmaQpStatus::QP_CONNECTED) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "rdma qp is not connected");
    }

    size_t numWrites = srcAddrs.size();
    if (dstAddrs.size() != numWrites || lengths.size() != numWrites) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "src, dst and length vectors must be of the same size");
    }

    if (numWrites == 0) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "src, dst and length vectors must contain at least 1 element");
    }

    struct send_wrlist_data_ext wrs[numWrites] = {};

    for (int i = 0; i < numWrites; i++) {
        wrs[i].dst_addr = dstAddrs[i];
        wrs[i].op = ops[i];
        wrs[i].send_flags = flags[i];
        wrs[i].mem_list.addr = srcAddrs[i];  // unsafe, don't use in prod ;)
        wrs[i].mem_list.len = lengths[i];
    }

    struct send_wr_rsp wrRsps[numWrites] = {};
    uint32_t completeNum = 0;
    CHECK_STATUS(RaSendWrlistExtWrapper(qpHandle, wrs, wrRsps, numWrites, &completeNum));
    if (completeNum != numWrites) {
        return Status::Error(ErrorCode::NOT_SUPPORTED, "Not all writes were submitted successfully");
    }

    uint32_t prevTaskId = 0;
    for (int i = 0; i < completeNum; i++) {
        struct sg_list list = { 0 };
        list.addr = wrs[i].mem_list.addr;
        list.len = wrs[i].mem_list.len;
        list.lkey = wrs[i].mem_list.lkey;

        struct send_wr wr {};
        wr.buf_list = &list;
        wr.buf_num = 1;
        wr.dst_addr = wrs[i].dst_addr;
        wr.op = wrs[i].op;
        wr.send_flag = wrs[i].send_flags;

        uint32_t taskId;
        dispatcher->RdmaSend(wrRsps[i].db.db_index, wrRsps[i].db.db_info, wr, &taskId);
        if (i > 0) {
            dispatcher->AddTaskDependency(prevTaskId, taskId);
        }
        prevTaskId = taskId;
    }

    *lastTaskId = prevTaskId;
    return Status::Success();
}
