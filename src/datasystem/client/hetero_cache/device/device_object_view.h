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

/**
 * Description: Internal, request-scoped, non-owning views over a single device/host source object.
 *
 * Defines H2DObjectView (MGetH2D source grouping) and D2HObjectView (MSetD2H existing-object filtering).
 * Both carry only pointers and a requestIndex; neither owns a DeviceBlobList or Buffer, and neither is
 * copied into a Blob vector.
 *
 * This is an internal header. It must NOT be exposed from the public include tree and must NOT live in the
 * common device layer, because it carries routing/diagnostic fields (requestIndex) that are hetero-specific.
 *
 * A view is non-owning: it references a caller-owned DeviceBlobList and a request-owned Buffer. Callers must
 * guarantee the referenced storage outlives the view's use:
 *   - synchronous MGetH2D/MSetD2H: caller's devBlobList outlives the call;
 *   - async MGetH2D/MSetD2H: the AsyncM*H2DState owns the devBlobList copy and the Buffer containers until
 *     copy completion;
 *   - the owning Buffer containers outlive the local and remote copy.
 */

#ifndef DATASYSTEM_CLIENT_OBJECT_CACHE_DEVICE_OBJECT_VIEW_H
#define DATASYSTEM_CLIENT_OBJECT_CACHE_DEVICE_OBJECT_VIEW_H

#include <cstddef>

#include "datasystem/utils/device_blob.h"
#include "datasystem/object/buffer.h"

namespace datasystem {
namespace object_cache {

struct H2DObjectView {
    /// Non-owning pointer into the caller's DeviceBlobList. Read-only; never copied into a Blob vector.
    const DeviceBlobList *deviceBlobs;
    /// Non-owning pointer into the request-owned Buffer container. May be nullptr for missing/failed keys.
    Buffer *hostBuffer;
    /// Positional index of this object in the original request, preserved for diagnostics and failure mapping.
    size_t requestIndex;
};

/// D2H counterpart: identical layout to H2DObjectView. Kept as a distinct type so D2H call sites are
/// self-documenting and so the resource-manager D2H pointer-ref overload can be selected unambiguously.
/// Lifetime rules mirror H2DObjectView: sync views point at the caller's DeviceBlobList; async views point at
/// AsyncMSetD2HState::devBlobList. The owning vector<shared_ptr<Buffer>> must outlive compose, D2H copy,
/// MultiPublish and SHM ref-count handling.
struct D2HObjectView {
    const DeviceBlobList *deviceBlobs;
    Buffer *hostBuffer;
    size_t requestIndex;
};

}  // namespace object_cache
}  // namespace datasystem

#endif  // DATASYSTEM_CLIENT_OBJECT_CACHE_DEVICE_OBJECT_VIEW_H
