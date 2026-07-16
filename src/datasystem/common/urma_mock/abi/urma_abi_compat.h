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
 * @brief Compatibility type shim for URMA mock builds.
 * When the project is built without real URMA SDK headers, this shim provides the minimum type, enum, and constant
 * definitions required by Datasystem URMA manager code. It is not a behavioral simulation; runtime behavior is provided
 * by MockUrmaBackend through the ds_urma_* wrapper surface.
 */

#ifndef DATASYSTEM_COMMON_URMA_MOCK_URMA_ABI_COMPAT_H
#define DATASYSTEM_COMMON_URMA_MOCK_URMA_ABI_COMPAT_H

// This header mirrors the URMA C ABI. Keep C-style names, typedefs, macros, unions, magic values, and pointer-based
// signatures aligned with the external ABI instead of applying project C++ style rewrites.
// NOLINTBEGIN

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// ---- Status / opcodes / flags (values match the real SDK) ----
typedef enum urma_status_t {
    URMA_SUCCESS = 0,
    URMA_ERROR = -1,
    URMA_E_FAIL = -1,
    URMA_E_NOT_SUPPORT = -2,
    URMA_E_INVALID = -3,
    URMA_E_BUSY = -4,
    URMA_E_TIMEOUT = -5,
    URMA_E_AGAIN = -6,
    URMA_E_NO_MEM = -7,
    URMA_E_PARTIAL = -8,
    URMA_E_IO = -9,
} urma_status_t;

typedef enum urma_opcode_t {
    URMA_OPC_WRITE = 0,
    URMA_OPC_READ = 1,
    URMA_OPC_SEND = 2,
    URMA_OPC_RECV = 3,
    URMA_OPC_ATOMIC = 4,
    URMA_OPC_BOND_WRITE = 5,
} urma_opcode_t;

enum {
    URMA_ACCESS_LOCAL_WRITE = 1 << 0,
    URMA_ACCESS_REMOTE_WRITE = 1 << 1,
    URMA_ACCESS_REMOTE_READ = 1 << 2,
    URMA_ACCESS_REMOTE_ATOMIC = 1 << 3,
    URMA_ACCESS_BIND = 1 << 4,
    URMA_ACCESS_MW_BIND = 1 << 5,
    URMA_ACCESS_ZERO_BASED = 1 << 6,
    URMA_ACCESS_ON_DEMAND = 1 << 7,
    URMA_ACCESS_HUGETLB = 1 << 8,
    URMA_ACCESS_RELAXED_ORDERING = 1 << 9,
    URMA_ACCESS_REMOTE_CACHED = 1 << 10,
    URMA_ACCESS_WRITE = URMA_ACCESS_LOCAL_WRITE | URMA_ACCESS_REMOTE_WRITE,
    URMA_ACCESS_READ = URMA_ACCESS_REMOTE_READ,
    URMA_ACCESS_ATOMIC = URMA_ACCESS_REMOTE_ATOMIC,
};

// Async event types (values match real SDK enum urma_async_event_type_t)
enum {
    URMA_EVENT_JETTY_ERR = 1,
    URMA_EVENT_JFC_ERR = 2,
    URMA_EVENT_DEVICE_ERR = 3,
    URMA_EVENT_PORT_ERR = 4,
    URMA_EVENT_NONE = 0,
};

typedef enum urma_cr_status_t {
    URMA_CR_SUCCESS = 0,
    URMA_CR_REM_ACCESS_ABORT_ERR = 1,
    URMA_CR_REM_FLUSH_ERR = 2,
    URMA_CR_REM_INVALID_REQ_ERR = 3,
    URMA_CR_LOC_ACCESS_ERR = 4,
    URMA_CR_LOC_LEN_ERR = 5,
    URMA_CR_LOC_PROT_ERR = 6,
    URMA_CR_LOC_QP_ERR = 7,
    URMA_CR_WR_FLUSH_ERR_DONE = 8,
    URMA_CR_GENERAL_ERR = 9,
} urma_cr_status_t;

// Token: opaque blob exchanged between URMA peers
#ifndef URMA_EID_SIZE
#define URMA_EID_SIZE 16
#endif
#ifndef URMA_TOKEN_SIZE
#define URMA_TOKEN_SIZE 64
#endif
#ifndef URMA_EID_INFO_RESERVED_SIZE
#define URMA_EID_INFO_RESERVED_SIZE 60
#endif
#ifndef URMA_DEVICE_NAME_SIZE
#define URMA_DEVICE_NAME_SIZE 64
#endif
#ifndef URMA_RJETTY_DATA_SIZE
#define URMA_RJETTY_DATA_SIZE 64
#endif

// ---- Opaque / forward-declared types (not used by value) ----
// Note: real SDK declares them as `typedef struct urma_x { ... } urma_x_t;`
// (i.e. struct and _t-typedef share the same name). Our shim mirrors that
// so DS code can write both `urma_jetty_t*` and `struct urma_jetty*`.
// urma_device_t and urma_context_t are defined below as full structs
// (used by value); all others here are forward-decls.
// urma_jetty_t and urma_jfc_t need a few visible fields (jetty_id, jfc_id),
// so we give them minimal full definitions instead of forward-declares.
typedef struct urma_jfce urma_jfce_t;
typedef struct urma_target_jetty urma_target_jetty_t;
typedef struct urma_target_seg urma_target_seg_t;
typedef struct urma_jetty_attr urma_jetty_attr_t;
typedef struct urma_jetty_stats urma_jetty_stats_t;

// EID (Endpoint ID) — DS accesses .raw[] via UrmaManager::EidToStr and embeds
// it into object IDs exchanged during JETTY setup.
struct urma_eid {
    uint8_t raw[URMA_EID_SIZE];
};
typedef struct urma_eid urma_eid_t;

#ifndef URMA_EID_STR_LEN
#define URMA_EID_STR_LEN (URMA_EID_SIZE * 2)
#endif
#define EID_FMT "%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x%02x"
#define EID_ARGS(e)                                                                                             \
    (e).raw[0], (e).raw[1], (e).raw[2], (e).raw[3], (e).raw[4], (e).raw[5], (e).raw[6], (e).raw[7], (e).raw[8], \
        (e).raw[9], (e).raw[10], (e).raw[11], (e).raw[12], (e).raw[13], (e).raw[14], (e).raw[15]

// 64-bit ID (used inside jetty_id, jfc_id, jfr_id, etc.)
struct urma_obj_id {
    urma_eid_t eid;
    uint32_t uasid;
    uint64_t id;
};
typedef struct urma_obj_id urma_obj_id_t;

// JFC event queue handle (returned by create_jfce; not dereferenced by DS)
struct urma_jfce {
    urma_obj_id_t jfce_id;
    void *priv;
};

// Target jetty (returned by import_jetty; DS passes opaque ptr to PostSendWr)
struct urma_target_jetty {
    urma_obj_id_t jetty_id;
    void *priv;
};

// (Old 64-bit ID def moved up to satisfy forward decl ordering.)

// Jetty handle: DS reads .jetty_id.id via UrmaJetty::GetJettyId
struct urma_jetty {
    urma_obj_id_t jetty_id;
    uint32_t state;
    void *priv;  // for mock backend
};
typedef struct urma_jetty urma_jetty_t;

// JFC handle: DS reads .jfc_id.id in async event handler
struct urma_jfc {
    urma_obj_id_t jfc_id;
    uint32_t depth;
    void *priv;
};
typedef struct urma_jfc urma_jfc_t;

// JFR handle: DS reads .jfr_id.id in UrmaResource
struct urma_jfr {
    urma_obj_id_t jfr_id;
    uint32_t depth;
    void *priv;
};
typedef struct urma_jfr urma_jfr_t;

// JFS handle: DS reads .jfs_id.id and passes the handle back to URMA APIs.
struct urma_jfs {
    union {
        urma_obj_id_t jfs_id;
        urma_obj_id_t jetty_id;
    };
    uint32_t state;
    void *priv;
};
typedef struct urma_jfs urma_jfs_t;

// ---- Types used by value (full definition required) ----

// EID info (returned by ds_urma_get_eid_list)
struct urma_eid_info {
    urma_eid_t eid;
    int eid_index;
    uint8_t reserved[URMA_EID_INFO_RESERVED_SIZE];
};
typedef struct urma_eid_info urma_eid_info_t;

// Device — DS reads .type via UrmaResource::CreateContext
struct urma_device {
    int type;
    int eid_cnt;
    char name[URMA_DEVICE_NAME_SIZE];
};
typedef struct urma_device urma_device_t;

// Context — DS reads .async_fd in async event handler, plus .eid/.uasid in init
struct urma_context {
    int async_fd;
    int dev_type;
    urma_eid_t eid;
    uint32_t uasid;
};
typedef struct urma_context urma_context_t;

// Init attribute
struct urma_init_attr_t {
    int mode;
    int flags;
};
typedef struct urma_init_attr_t urma_init_attr_t;

// Log callback
typedef void (*urma_log_cb_t)(int level, char *message);

// Opt name
typedef enum urma_opt_name_t {
    URMA_OPT_CTX_ID = 0,
    URMA_OPT_MAX_JETTY = 1,
} urma_opt_name_t;

// user_ctl input/output — only forward decl is enough
struct urma_user_ctl_in_t;
struct urma_user_ctl_out_t;

// Token (exchanged between peers during import; DS uses { 0xACFE } 2-byte literal)
struct urma_token {
    union {
        uint64_t token;
        uint64_t value;
    };
};
typedef struct urma_token urma_token_t;

// Token ID handle (DS dereferences urma_token_id_t->token_id)
struct urma_token_id {
    uint32_t token_id;
    uint32_t reserved;
};
typedef struct urma_token_id urma_token_id_t;

struct urma_reg_seg_flag {
    uint32_t value;
    struct {
        uint32_t access;
        uint32_t token_policy;
        uint32_t token_id_valid;
        uint32_t cacheable;
        uint32_t reserved;
    } bs;
};
typedef struct urma_reg_seg_flag urma_reg_seg_flag_t;

struct urma_import_seg_flag {
    uint32_t value;
    struct {
        uint32_t access;
        uint32_t cacheable;
        uint32_t mapping;
        uint32_t reserved;
    } bs;
};
typedef struct urma_import_seg_flag urma_import_seg_flag_t;

// Segment config (passed to register_seg / import_seg)
struct urma_seg_cfg {
    uint64_t va;  // SDK type: void* but DS passes uint64_t segAddress
    uint64_t iova;
    size_t len;
    int access;
    urma_reg_seg_flag_t flag;
    int pgsize_shift;
    void *cookie;
    void *user_ctx;
    urma_token_t token_value;
    urma_token_id_t *token_id;
};
typedef struct urma_seg_cfg urma_seg_cfg_t;

// Jetty config (full version; see extended version below)
struct urma_jetty_cfg_unused_placeholder;

// Jetty attr
struct urma_jetty_attr {
    uint32_t mask;
    uint32_t state;
};
typedef struct urma_jetty_attr urma_jetty_attr_t;

// JFC config (extended for mock; DS writes flag.value/jfce/user_ctx/ceqn)
// flag is a bitfield struct (urma spec), DS writes .value and .bs.<bits>
struct urma_jfc_flag {
    union {
        struct {
            uint32_t reserved0 : 1;
            uint32_t reserved1 : 31;
        } bs;
        uint32_t value;
    };
};
typedef struct urma_jfc_flag urma_jfc_flag_t;

struct urma_jfc_cfg {
    int depth;
    int max_sge;
    urma_jfc_flag_t flag;
    void *jfce;
    void *user_ctx;
    int ceqn;
};
typedef struct urma_jfc_cfg urma_jfc_cfg_t;

// JFR config (extended for mock; DS writes flag.value/flag.bs.tag_matching/trans_mode/...)
struct urma_jfr_flag {
    union {
        struct {
            uint32_t tag_matching : 1;
            uint32_t reserved : 31;
        } bs;
        uint32_t value;
    };
};
typedef struct urma_jfr_flag urma_jfr_flag_t;

struct urma_jfr_cfg {
    int depth;
    int max_sge;
    int max_wr;
    urma_jfr_flag_t flag;
    int trans_mode;
    int min_rnr_timer;
    int rnr_retry;
    void *jfc;
    urma_token_t token_value;
    uint32_t id;
    void *user_ctx;
};
typedef struct urma_jfr_cfg urma_jfr_cfg_t;

// JFS config (DS writes flag.value/flag.bs.multi_path + depth/priority/max_sge/err_timeout/jfc)
struct urma_jfs_flag {
    union {
        struct {
            uint32_t multi_path : 1;
            uint32_t reserved : 31;
        } bs;
        uint32_t value;
    };
};
typedef struct urma_jfs_flag urma_jfs_flag_t;

struct urma_jfs_cfg {
    int depth;
    int priority;
    int trans_mode;
    int rnr_retry;
    int min_rnr_timer;
    int max_send_wr;
    int max_send_sge;
    int max_inline_data;
    int max_sge;
    int err_timeout;
    void *jfc;
    urma_jfs_flag_t flag;
    void *user_ctx;
};
typedef struct urma_jfs_cfg urma_jfs_cfg_t;

// Jetty config (extended; DS writes flag.value/flag.bs.share_jfr + jfs_cfg/shared/user_ctx)
struct urma_jetty_flag {
    union {
        struct {
            uint32_t share_jfr : 1;
            uint32_t has_drv_ext : 1;
            uint32_t reserved : 30;
        } bs;
        uint32_t value;
    };
};
typedef struct urma_jetty_flag urma_jetty_flag_t;

// shared is a union of jfr/jfc pointers (DS writes .jfr and .jfc)
struct urma_jetty_shared {
    void *jfr;
    void *jfc;
};
typedef struct urma_jetty_shared urma_jetty_shared_t;

struct urma_jetty_cfg {
    int access;
    int pgsize_shift;
    int max_send_wr;
    int max_recv_wr;
    int max_send_sge;
    int max_recv_sge;
    int max_inline_data;
    urma_jetty_flag_t flag;
    urma_jfs_cfg_t jfs_cfg;
    urma_jetty_shared_t shared;
    void *user_ctx;
};
typedef struct urma_jetty_cfg urma_jetty_cfg_t;

// Device attribute (passed to / from urma_query_device)
// DS reads .dev_cap.max_read_size via UrmaResource::GetMaxReadSize and
// .dev_cap.priority_info[].SL via UrmaResource::GetJettyPriorityInfoForCTP.
struct urma_tp_type_en {
    union {
        struct {
            uint32_t ctp : 1;
            uint32_t rtp : 1;
            uint32_t reserved : 30;
        } bs;
        uint32_t value;
    };
#ifdef __cplusplus
    urma_tp_type_en &operator=(uint32_t v)
    {
        value = v;
        return *this;
    }

    operator uint32_t() const
    {
        return value;
    }
#endif
};
typedef struct urma_tp_type_en urma_tp_type_en_t;

struct urma_priority_info {
    urma_tp_type_en_t tp_type;
    uint32_t SL;
    uint32_t reserved;
};
typedef struct urma_priority_info urma_priority_info_t;

#ifndef URMA_MAX_PRIORITY
#define URMA_MAX_PRIORITY 7
#endif

// Transport modes (URMA_TM_*)
enum {
    URMA_TM_RC = 0,  // Reliable Connection
    URMA_TM_RM = 1,  // Reliable Message
    URMA_TM_UD = 2,  // Unreliable Datagram
};

// JFR flag bits
enum {
    URMA_NO_TAG_MATCHING = 0,
    URMA_SHARE_JFR = 1,
};

// URMA rjetty types (DS uses URMA_CTP / URMA_RTP / URMA_JETTY constants)
enum {
    URMA_RTP = 1,
    URMA_CTP = 2,
    URMA_JETTY = 3,
};

// URMA token type
enum {
    URMA_TOKEN_PLAIN_TEXT = 0,
    URMA_TOKEN_ID_INVALID = 0xFFFFFFFFu,
};

// URMA seg flag bits
enum {
    URMA_SEG_NOMAP = 1,
    URMA_NON_CACHEABLE = 2,
};

// URMA vlog levels (used in log dispatch)
enum {
    URMA_VLOG_LEVEL_ERR = 1,
    URMA_VLOG_LEVEL_WARNING = 2,
    URMA_VLOG_LEVEL_NOTICE = 3,
    URMA_VLOG_LEVEL_INFO = 4,
    URMA_VLOG_LEVEL_DEBUG = 5,
};

// Default RNR timer / retry values (URMA spec)
#define URMA_TYPICAL_MIN_RNR_TIMER 0x10
#define URMA_TYPICAL_RNR_RETRY 6
#define URMA_TYPICAL_ERR_TIMEOUT 0x12

// Jetty state (URMA_JETTY_STATE_*)
enum {
    URMA_JETTY_STATE_ACTIVE = 0,
    URMA_JETTY_STATE_ERROR = 1,
    URMA_JETTY_STATE_RST = 2,
    URMA_JETTY_STATE_INIT = 3,
};
// JETTY_STATE bitmask (which attr fields are valid) — used in attr.mask
#ifndef JETTY_STATE
#define JETTY_STATE 0x1
#endif
#ifndef JFS_STATE
#define JFS_STATE JETTY_STATE
#endif

struct urma_jfs_attr {
    uint32_t mask;
    uint32_t state;
};
typedef struct urma_jfs_attr urma_jfs_attr_t;

struct urma_dev_cap {
    uint64_t max_write_size;
    uint64_t max_read_size;
    uint64_t max_send_wr;
    uint64_t max_recv_wr;
    int max_jetty;
    int max_jfc;
    int max_jfc_depth;
    int max_jfr;
    int page_size;
    urma_priority_info_t priority_info[URMA_MAX_PRIORITY + 1];
};
typedef struct urma_dev_cap urma_dev_cap_t;

struct urma_device_attr {
    int dev_type;
    int eid_cnt;
    urma_dev_cap_t dev_cap;
    int max_jetty;
    int max_jfc;
    int max_jfr;
    int max_send_wr;
    int max_recv_wr;
    int max_send_sge;
    int max_recv_sge;
    int max_inline_data;
    int max_seg_cnt;
    int page_size;
};
typedef struct urma_device_attr urma_device_attr_t;

typedef uint64_t urma_addr_t;

// SGE (scatter-gather element)
struct urma_sge {
    urma_addr_t addr;
    uint32_t len;
    uint32_t lkey;
    uint32_t rkey;
    urma_target_seg_t *tseg;
    void *user_tseg;
};
typedef struct urma_sge urma_sge_t;

// SG (scatter-gather list)
struct urma_sg {
    urma_sge_t *sge;
    uint32_t num_sge;
};
typedef struct urma_sg urma_sg_t;

// Segment attribute (bitfield; DS reads .value)
struct urma_seg_attr {
    union {
        struct {
            uint32_t local_write : 1;
            uint32_t remote_write : 1;
            uint32_t remote_read : 1;
            uint32_t remote_atomic : 1;
            uint32_t reserved : 28;
        };
        uint32_t value;
    };
};
typedef struct urma_seg_attr urma_seg_attr_t;

// User-space virtual address (UBVA) descriptor
struct urma_ubva {
    urma_eid_t eid;
    uint32_t uasid;
    uint64_t va;
};
typedef struct urma_ubva urma_ubva_t;

// Segment handle (returned by import_seg; DS reads .ubva / .attr / .token_id)
struct urma_seg {
    urma_ubva_t ubva;
    size_t len;
    urma_seg_attr_t attr;
    uint32_t token_id;
    void *priv;  // for mock backend
};
typedef struct urma_seg urma_seg_t;

// Target segment (imported by peer; DS reads .token_id and .seg.token_id)
struct urma_target_seg {
    urma_seg_t seg;
    urma_token_id_t *token_id;
    void *priv;
};
typedef struct urma_target_seg urma_target_seg_t;

// RJetty (remote jetty handle; DS reads .jetty_id / .trans_mode / .type / .tp_type / .flag)
struct urma_rjetty {
    urma_obj_id_t jetty_id;
    int trans_mode;
    int type;
    urma_tp_type_en_t tp_type;
    urma_jetty_flag_t flag;
    uint8_t data[URMA_RJETTY_DATA_SIZE];  // opaque SDK payload
};
typedef struct urma_rjetty urma_rjetty_t;

// RJFR (remote receive jetty handle; legacy URMA API used by Datasystem).
struct urma_rjfr {
    urma_obj_id_t jfr_id;
    int trans_mode;
    urma_tp_type_en_t tp_type;
    uint8_t data[URMA_RJETTY_DATA_SIZE];
};
typedef struct urma_rjfr urma_rjfr_t;

// RJetty (BONDP variant; cross-chip)
struct urma_bondp_rjetty {
    urma_rjetty_t base;
    urma_jetty_t *jetty;
    int src_chip_id;
    int dst_chip_id;
};
typedef struct urma_bondp_rjetty urma_bondp_rjetty_t;
typedef struct urma_bondp_rjetty bondp_rjetty_t;

// Import segment flag
enum {
    URMA_IMPORT_SEG_FLAG_NONE = 0,
    URMA_IMPORT_SEG_FLAG_LOCAL_WRITE = 1,
    URMA_IMPORT_SEG_FLAG_REMOTE_WRITE = 2,
    URMA_IMPORT_SEG_FLAG_REMOTE_READ = 4,
    URMA_IMPORT_SEG_FLAG_REMOTE_ATOMIC = 8,
};

// Register segment flag
enum {
    URMA_REG_SEG_FLAG_NONE = 0,
    URMA_REG_SEG_FLAG_LOCAL_WRITE = 1,
    URMA_REG_SEG_FLAG_REMOTE_WRITE = 2,
    URMA_REG_SEG_FLAG_REMOTE_READ = 4,
    URMA_REG_SEG_FLAG_REMOTE_ATOMIC = 8,
    URMA_REG_SEG_FLAG_DOUBLE_MAP = 16,
};

// Async event
struct urma_async_event_t {
    urma_context_t *urma_ctx;
    int event_type;
    int element_type;
    union {
        urma_jetty_t *jetty;
        urma_jfc_t *jfc;
        void *raw;
    } element;
    int data;
};
typedef struct urma_async_event_t urma_async_event_t;

// Completion record
struct urma_cr_t {
    urma_cr_status_t status;
    union {
        uint32_t completion_len;
        uint32_t byte_cnt;
    };
    uint32_t local_id;  // DS reads this in UrmaManager
    uint64_t user_ctx;  // DS reads as void* / uint64_t
    int opcode;
    int immediate_data;
    int reserved;
};
typedef struct urma_cr_t urma_cr_t;

// JFS work request flag (DS uses .bs.complete_enable / .bs.inline_flag)
struct urma_jfs_wr_flag_t {
    union {
        struct {
            uint32_t complete_enable : 1;
            uint32_t inline_flag : 1;
            uint32_t fence : 1;
            uint32_t signaled : 1;
            uint32_t reserved : 28;
        } bs;
        uint32_t value;
    };
};
typedef struct urma_jfs_wr_flag_t urma_jfs_wr_flag_t;

// RW (read/write) work request
struct urma_rw_wr_t {
    urma_sg_t src;
    urma_sg_t dst;
    uint64_t target_hint;
    uint64_t notify_data;
};
typedef struct urma_rw_wr_t urma_rw_wr_t;

// JFS (jetty send) work request
struct urma_jfs_wr;  // forward decl for self-reference in next
typedef struct urma_jfs_wr urma_jfs_wr_t;

struct urma_jfs_wr {
    urma_opcode_t opcode;
    urma_jfs_wr_flag_t flag;
    urma_target_jetty_t *tjetty;
    uint64_t user_ctx;  // SDK: void* but DS sometimes stores uint64_t
    urma_rw_wr_t rw;
    urma_jfs_wr_t *next;
};

// BONDP work request (cross-chip; not used by mock but needed for compile)
struct urma_bondp_jfs_wr {
    urma_jfs_wr_t base;
    int src_chip_id;
    int dst_chip_id;
};
typedef struct urma_bondp_jfs_wr urma_bondp_jfs_wr_t;
typedef struct urma_bondp_jfs_wr bondp_jfs_wr_t;

#ifdef __cplusplus
}
#endif

// NOLINTEND

#endif  // DATASYSTEM_COMMON_URMA_MOCK_URMA_ABI_COMPAT_H
