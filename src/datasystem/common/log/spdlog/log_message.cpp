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

/**
 * Description: Log message.
 */
#include "datasystem/common/log/spdlog/log_message.h"
#include "datasystem/common/log/spdlog/log_message_impl.h"

namespace datasystem {
LogMessage::LogMessage(LogSeverity logSeverity, const char *file, int line, bool forceLog)
{
    impl_ = std::make_shared<LogMessageImpl>(logSeverity, file, line, forceLog);
}

std::ostream &LogMessage::Stream()
{
    return impl_->Stream();
}
}  // namespace datasystem
