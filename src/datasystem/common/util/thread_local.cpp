/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2023. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * Description: Per-request context definitions.
 *
 * The 8 global ScopedBthreadLocal<T> definitions that formerly lived here
 * have been migrated INTO the RequestContext struct (see request_context.h).
 * Per-request timeout/auth state is now accessed via GetRequestContext()->field.
 *
 * This translation unit is kept as a build-graph anchor: the BUILD.bazel
 * alias `//src/datasystem/common/util:thread_local` points at common_util_impl,
 * and many targets depend on it. Removing the file would orphan the alias;
 * keeping it empty preserves the build graph without reintroducing globals.
 */

#include "datasystem/common/util/thread_local.h"

namespace datasystem {
// Intentionally empty — no per-request globals remain.
}  // namespace datasystem
