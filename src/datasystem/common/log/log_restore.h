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
 * Description: Restore datasystem LOG macros after glog/brpc headers
 *              (e.g. butil/logging.h) may have redefined them.
 *
 * Include this header at the VERY END of your .cpp file's include list,
 * after all headers that may transitively include glog or brpc logging.
 */

#ifndef DATASYSTEM_COMMON_LOG_LOG_RESTORE_H
#define DATASYSTEM_COMMON_LOG_LOG_RESTORE_H

#pragma pop_macro("LOG")
#pragma pop_macro("LOG_IF")
#pragma pop_macro("PLOG")
#pragma pop_macro("PLOG_IF")
#pragma pop_macro("LOG_EVERY_N")
#pragma pop_macro("LOG_FIRST_N")
#pragma pop_macro("LOG_IF_EVERY_N")
#pragma pop_macro("VLOG")
#pragma pop_macro("VLOG_IF")
#pragma pop_macro("VLOG_EVERY_N")
#pragma pop_macro("VLOG_IS_ON")
#pragma pop_macro("CHECK")
#pragma pop_macro("CHECK_EQ")
#pragma pop_macro("CHECK_NE")
#pragma pop_macro("CHECK_LT")
#pragma pop_macro("CHECK_LE")
#pragma pop_macro("CHECK_GT")
#pragma pop_macro("CHECK_GE")
#pragma pop_macro("DLOG")
#pragma pop_macro("DCHECK")
#pragma pop_macro("DCHECK_IS_ON")

#endif  // DATASYSTEM_COMMON_LOG_LOG_RESTORE_H
