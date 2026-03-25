#ifndef TRANSFER_ENGINE_STATUS_HELPER_H
#define TRANSFER_ENGINE_STATUS_HELPER_H

#include "datasystem/transfer_engine/status.h"

#define TE_MAKE_STATUS(code_, msg_) \
    datasystem::Status((code_), (msg_), __LINE__, __FILE__)

#define TE_RETURN_STATUS(code_, msg_) \
    do {                              \
        return TE_MAKE_STATUS((code_), (msg_)); \
    } while (false)

#define TE_RETURN_IF_ERROR(statement_)          \
    do {                                        \
        auto _te_status = (statement_);         \
        if (_te_status.IsError()) {             \
            return _te_status;                  \
        }                                       \
    } while (false)

#define TE_CHECK_OR_RETURN(condition_, code_, msg_)        \
    do {                                                    \
        if (!(condition_)) {                                \
            TE_RETURN_STATUS((code_), (msg_));              \
        }                                                   \
    } while (false)

#define TE_CHECK_PTR_OR_RETURN(ptr_)                                                   \
    do {                                                                                \
        if ((ptr_) == nullptr) {                                                        \
            TE_RETURN_STATUS(datasystem::StatusCode::kRuntimeError, "null pointer: " #ptr_); \
        }                                                                               \
    } while (false)

#endif  // TRANSFER_ENGINE_STATUS_HELPER_H
