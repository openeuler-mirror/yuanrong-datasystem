
/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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