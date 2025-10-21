/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2025-2025. All rights reserved.
 */
#include "npu/P2PStream.h"

P2PStream::~P2PStream()
{
    if (state != StreamState::STREAM_UNINITIALIZED && ownStream) {
        rtStreamDestroy(stream);
    }
}

Status P2PStream::create()
{
    ACL_CHECK_STATUS(
        rtStreamCreateWithFlags(&stream,
                                P2P_STREAM_PRIORITY,
                                RT_STREAM_FAST_LAUNCH | RT_STREAM_FAST_SYNC));
    ACL_CHECK_STATUS(rtGetStreamId(stream, &streamId));
    state = StreamState::STREAM_INITIALIZED;
    ownStream = true;
    return Status::Success();
}

Status P2PStream::open(rtStream_t stream)
{
    this->stream = stream;
    ACL_CHECK_STATUS(rtGetStreamId(stream, &streamId));
    state = StreamState::STREAM_INITIALIZED;
    return Status::Success();
}