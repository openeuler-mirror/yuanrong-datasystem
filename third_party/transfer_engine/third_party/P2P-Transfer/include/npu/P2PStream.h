/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */
#ifndef P2P_STREAM_H
#define P2P_STREAM_H
#include <stddef.h>
#include <array>
#include <cstring>
#include "tools/Status.h"
#include "tools/npu-error.h"
#include "acl/acl.h"
#include "experiment/msprof/toolchain/prof_api.h"
#include "runtime/stream.h"

enum StreamState { STREAM_INITIALIZED, STREAM_UNINITIALIZED };

constexpr int32_t P2P_STREAM_PRIORITY = 5;

class P2PStream {
public:
    P2PStream() : state(StreamState::STREAM_UNINITIALIZED) {}
    ~P2PStream();

    P2PStream(const P2PStream &) = delete;
    P2PStream &operator=(const P2PStream &) = delete;

    Status create();
    Status open(rtStream_t stream);

    void *ptr() const
    {
        return stream;
    }

    uint32_t id() const
    {
        return streamId;
    }

private:
    StreamState state;

    int32_t streamId;
    bool ownStream = false;
    rtStream_t stream = nullptr;
};

#endif  // P2P_STREAM_H