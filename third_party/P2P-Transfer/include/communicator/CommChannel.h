/*
* Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_COMM_CHANNEL_H
#define P2P_COMM_CHANNEL_H

class ReceiveChannel {
public:
    virtual Status Initialize(TCPObjectClient *client, TCPObjectServer *server) = 0;
    virtual Status Receive(void **dstPtrs, uint64_t* sizes, uint32_t count, aclrtStream stream) = 0;
    virtual ~ReceiveChannel() {}
};

class SendChannel {
public:
    virtual Status Initialize(TCPObjectClient *client, TCPObjectServer *server) = 0;
    virtual Status Send(void **srcPtrs, uint64_t* sizes, uint32_t count, aclrtStream stream) = 0;
    virtual ~SendChannel() {}
};

#endif  // P2P_COMM_CHANNEL_H