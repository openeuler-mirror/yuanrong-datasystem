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
#ifndef RA_H
#define RA_H

#include <stddef.h>
#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#include <sys/types.h>
#include <arpa/inet.h>
#include <netinet/in.h>

#ifdef __cplusplus
extern "C" {
#endif

/**
 * @ingroup libsocket
 * socket error code conversion
 */
#define SOCK_EAGAIN 128201        /* EAGAIN:no data received by socket */
#define SOCK_ENOENT 228200        /* ENOENT:SOCK_ENOENT means mr async not success right now,revoke the funcion again */
#define SOCK_EADDRINUSE 128205    /* EADDRINUSE：check if IP has been listened when SOCK_EADDRINUSE is returned */
#define SOCK_EADDRNOTAVAIL 128206 /* EADDRNOTAVAIL：check if IP exist when SOCK_EADDRNOTAVAIL is returned */
#define SOCK_ESOCKCLOSED 128207   /* ESOCKCLOSED：socket has been closed */
#define HCCP_EINVALIDIPS 328008   /* ranktable中ip和物理网卡的ip不一致 */
#define HCCP_ELINKDOWN 328004     /* 网口down */
#define ROCE_ENOMEM 328100        /* ENOMEM: roce module has ENOMEM error */
#define SOCK_ENODEV 228202        /* socket 设备不存在 */

#define SOCK_CONN_TAG_SIZE 192

enum class NICDeployment { NIC_DEPLOYMENT_HOST = 0, NIC_DEPLOYMENT_DEVICE, NIC_DEPLOYMENT_RESERVED };

enum network_mode {
    NETWORK_PEER_ONLINE = 0,  // Peer mode
    NETWORK_OFFLINE,          // HDC mode
    NETWORK_ONLINE,           // Reserved
};

enum notify_type { NOTIFY = 0, EVENTID = 1 };

enum socket_role { SERVER = 0, CLIENT = 1 };

struct ra_init_config {
    unsigned int phy_id;        // Physical device ID. Value is 0 for third-party NIC
    unsigned int nic_position;  // Supported scenario, see enum network_mode for details
    int hdc_type;  // For details about the HDC service types supported by the HDC mode, see HDC_SERVICE_TYPE_RDMA and
                   // HDC_SERVICE_TYPE_RDMA_V2 in enum drvHdcServiceType.
};

struct ra_get_ifattr {
    unsigned int phy_id;       /**< physical device id */
    unsigned int nic_position; /**< reference to network_mode */
    bool is_all; /**< valid when nic_position is NETWORK_OFFLINE. false: get specific rnic ip, true: get all rnic ip */
};

enum id_type { PHY_ID_VNIC_IP, SDID_VNIC_IP };

union hccp_ip_addr {
    struct in_addr addr;
    struct in6_addr addr6;
};

struct ip_info {
    int family;
    union hccp_ip_addr ip;
    unsigned int resv[2U];
};

struct rdev {
    unsigned int phy_id; /**< physical device id */
    int family;          /**< AF_INET(ipv4) or AF_INET6(ipv6) */
    union hccp_ip_addr local_ip;
};

struct socket_listen_info_t {
    void *socket_handle; /**< handle of created socket resource by ra_socket_init/ra_socket_init_v1 */
    unsigned int port;   /**< Socket listening port number */
    unsigned int phase;  /**< refer to enum listen_phase */
    unsigned int err;    /**< errno */
};

struct rdev_init_info {
    int mode;
    unsigned int notify_type;
    bool enabled_910a_lite;  // false indicates that 1980 RDMA lite function is disabled (4KB, 2MB). True indicates 1980
                             // RDMA lite function is enabled
    bool disabled_lite_thread;  // When RDMA lite is enabled, false indicates that HCCP polls SCQ. True indicates that
                                // background thread is disabled and invoker HCCL polls SCQ
    bool enabled_2mb_lite;      // False indicates that 2MB RDMA lite function is disabled. True indicates that 2MB RDMA
                                // lite function is enabeld (need enabled_910a_lite=true to enable on 1980)
};

struct socket_info_t {
    void *socket_handle;          /**< handle of created socket resource by ra_socket_init/ra_socket_init_v1 */
    void *fd_handle;              /**< Link setup information: Socket FD resource handle  */
    union hccp_ip_addr remote_ip; /**< IP address of remote socket */
    int status;                   /**< socket status:0 not connected 1:connected 2:connect timeout 3:connecting */
    char tag[SOCK_CONN_TAG_SIZE]; /**< tag must ended by '\0' */
};

struct socket_connect_info_t {
    void *socket_handle;          /**< handle of created socket resource by ra_socket_init/ra_socket_init_v1 */
    union hccp_ip_addr remote_ip; /**< IP address of remote socket, [0-7] is reserved for vnic */
    unsigned int port;            /**< Socket listening port number */
    char tag[SOCK_CONN_TAG_SIZE]; /**< tag must ended by '\0' */
};

struct socket_close_info_t {
    void *socket_handle; /**< handle of created socket resource by ra_socket_init/ra_socket_init_v1 */
    void *fd_handle;     /**< fd handle  Socket FD resource handle obtained by ra_get_sockets */
    int disuse_linger;   /**< 0:use(default l_linger is RS_CLOSE_TIMEOUT), others:disuse */
};

struct socket_wlist_info_t {
    union hccp_ip_addr remote_ip;                          /**< IP address of remote */
    unsigned int conn_limit; /**< limit of whilte list */  // 删除接口中该字段无意义
    char tag[SOCK_CONN_TAG_SIZE];                          /**< tag used for whitelist must ended by '\0' */
};

enum {
    RA_NOR_QP_MODE = 0,       // Normal mode
    RA_GDR_TMPL_QP_MODE = 1,  // 1980 aaync sink GDR mode
    RA_OP_QP_MODE = 2,        // 1980 Single oeprator mode
    RA_GDR_ASYN_QP_MODE = 3,  // 1971/1981 async sink GDR mode
    RA_OP_QP_MODE_EXT = 4,    // 1971/1981 single operator mode
    RA_ERR_QP_MODE = 5,
};

struct cq_ext_attr {
    int send_cq_depth;
    int recv_cq_depth;
    int send_cq_comp_vector;
    int recv_cq_comp_vector;
};
struct ibv_qp_cap {
    uint32_t max_send_wr;
    uint32_t max_recv_wr;
    uint32_t max_send_sge;
    uint32_t max_recv_sge;
    uint32_t max_inline_data;
};
enum ibv_qp_type {
    IBV_QPT_RC = 2,
    IBV_QPT_UC,
    IBV_QPT_UD,
    IBV_QPT_RAW_PACKET = 8,
    IBV_QPT_XRC_SEND = 9,
    IBV_QPT_XRC_RECV,
    IBV_QPT_DRIVER = 0xff,
};
struct ibv_qp_init_attr {
    void *qp_context;        // Does not support transparent transmission
    struct ibv_cq *send_cq;  // Does not support transparent transmission
    struct ibv_cq *recv_cq;  // Does not support transparent transmission
    struct ibv_srq *srq;     // Does not support transparent transmission
    struct ibv_qp_cap cap;
    enum ibv_qp_type qp_type;  // Currently, only IBV_QPT_RC is supported.
    int sq_sig_all;
};
struct qp_ext_attrs {
    int qp_mode;  // Same as ra_qp_create
    // cq attr
    struct cq_ext_attr cq_attr;
    // qp attr
    struct ibv_qp_init_attr qp_attr;
    // version control and reserved
    int version;
    int mem_align;  // Non-input parameter, not supported for transparent transmission, only for reuse of HCCP internal
                    // data structures: 0,1:4KB, 2:2MB page table
    uint32_t udp_sport;
    uint32_t reserved[30U];
};

struct qos_attr {
    unsigned char tc;  // traffic class
    unsigned char sl;  // priority(service level)
    unsigned char reserved[6];
};
struct mr_info {
    // Input parameters
    void *addr;              /**< starting address of mr */
    unsigned long long size; /**< size of mr */
    int access;              /**< access of mr, reference to ra_access_flags */
    // Output parameters
    unsigned int lkey; /**< local addr access key */
    unsigned int rkey; /**< remote addr access key */
};

enum ra_access_flags {
    RA_ACCESS_LOCAL_WRITE = 1,         /**< mr local write access */
    RA_ACCESS_REMOTE_WRITE = (1 << 1), /**< mr remote write access */
    RA_ACCESS_REMOTE_READ = (1 << 2),  /**< mr remote read access */
    RA_ACCESS_REDUCE = (1 << 8),
};

enum ra_qp_status {
    RA_QP_STATUS_DISCONNECT = 0,    // Not connected
    RA_QP_STATUS_CONNECTED = 1,     // Connection OK, link establishment status, corresponding QP status is RTS
    RA_QP_STATUS_TIMEOUT = 2,       // Connection timed out.
    RA_QP_STATUS_CONNECTING = 3,    // Connecting
    RA_QP_STATUS_REM_FD_CLOSE = 4,  // The peer handle is closed.
    RA_QP_STATUS_PAUSE = 5,         // Stop state corresponds to QP state RESET
};

enum ra_send_flags {
    RA_SEND_FENCE = 1 << 0,     /**< RDMA operation with fence */
    RA_SEND_SIGNALED = 1 << 1,  /**< RDMA operation with signaled */
    RA_SEND_SOLICITED = 1 << 2, /**< RDMA operation with solicited */
    RA_SEND_INLINE = 1 << 3,    /**< RDMA operation with inline */
};

struct sg_list {
    uint64_t addr;     /**< address of buf */
    unsigned int len;  /**< len of buf */
    unsigned int lkey; /**< local addr access key */
};

struct wr_aux_info {
    uint8_t dataType;
    uint8_t reduceType;
    uint32_t notifyOffset;
};

struct send_wrlist_data_ext {
    unsigned long long dst_addr; /**< destination address */
    unsigned int op;             /**< operations of RDMA supported:RDMA_WRITE:0, RDMA_READ:4 */
    int send_flags;              /**< reference to ra_send_flags */
    struct sg_list mem_list;     /**< list of sg  only 1 is supported */
    struct wr_aux_info aux;      /**< aux info */
};

// Output Data Structure
struct wqe_info {
    unsigned int sq_index;  /**< index of sq */
    unsigned int wqe_index; /**< index of wqe */
};
struct db_info {
    unsigned int db_index; /**< index of db */
    unsigned long db_info; /**< db content */
};
struct send_wr_rsp {
    union {
        struct wqe_info wqe_tmp; /**< wqe template info  Asynchronous sink mode */
        struct db_info db;       /**< doorbell info  Single-operator mode */
    };
};

struct send_wr {
    struct sg_list *buf_list; /**< list of sg */
    uint16_t buf_num;         /**< num of buf_list 最大规格为 MAX_SGE_NUM  */
    uint64_t dst_addr;        /**< destination address */
    unsigned int rkey;        /**< remote address access key */
    unsigned int op;          /**< operations of RDMA supported:RDMA_WRITE:0 */
    int send_flag;            /**< reference to ra_send_flags */
};

struct ifaddr_info {
    union hccp_ip_addr ip; /* Address of interface */
    struct in_addr mask;   /* Netmask of interface */
};

struct interface_info {
    int family;
    int scope_id;
    struct ifaddr_info ifaddr; /* Address and netmask of interface */
    char ifname[256];          /* Name of interface */
};

int ra_init(struct ra_init_config *config);
int ra_deinit(struct ra_init_config *config);

int ra_socket_get_vnic_ip_infos(uint32_t phy_id, enum id_type type, uint32_t ids[], uint32_t num,
                                struct ip_info infos[]);

int ra_socket_init(int mode, struct rdev rdev_info, void **socket_handle);
int ra_socket_deinit(void *socket_handle);

int ra_socket_listen_start(struct socket_listen_info_t conn[], uint32_t num);
int ra_socket_listen_stop(struct socket_listen_info_t conn[], uint32_t num);

int ra_rdev_init_v2(struct rdev_init_info init_info, struct rdev rdev_info, void **rdma_handle);
int ra_rdev_deinit(void *rdma_handle, unsigned int notify_type);

int ra_rdev_get_support_lite(void *rdma_handle, int *support_lite);

int ra_get_sockets(uint32_t role, struct socket_info_t conn[], uint32_t num, uint32_t *connected_num);

int ra_socket_batch_connect(struct socket_connect_info_t conn[], uint32_t num);
int ra_socket_batch_close(struct socket_close_info_t conn[], unsigned int num);

int ra_socket_white_list_add(void *socket_handle, struct socket_wlist_info_t white_list[], unsigned int num);
int ra_socket_white_list_del(void *socket_handle, struct socket_wlist_info_t white_list[], unsigned int num);

int ra_qp_create_with_attrs(void *rdev_handle, struct qp_ext_attrs *ext_attrs, void **qp_handle);
int ra_qp_destroy(void *qp_handle);

int ra_set_qp_attr_qos(void *qp_handle, struct qos_attr *attr);
int ra_set_qp_attr_timeout(void *qp_handle, uint32_t *timeout);
int ra_set_qp_attr_retry_cnt(void *qp_handle, uint32_t *retry_cnt);

int ra_mr_reg(void *qp_handle, struct mr_info *info);
int ra_mr_dereg(void *qp_handle, struct mr_info *info);

int ra_qp_connect_async(void *qp_handle, const void *fd_handle);

int ra_get_qp_status(void *qp_handle, ra_qp_status *status);

int ra_send_wrlist_ext(void *qp_handle, struct send_wrlist_data_ext wr[], struct send_wr_rsp op_rsp[],
                       uint32_t send_num, uint32_t *complete_num);

int ra_get_notify_base_addr(void *rdev_handle, unsigned long long *va, unsigned long long *size);

int ra_send_wr(void *qp_handle, struct send_wr *wr, struct send_wr_rsp *op_rsp);

int ra_get_ifaddrs(struct ra_get_ifattr *config, struct interface_info interface_infos[], unsigned int *num);

int ra_get_ifnum(struct ra_get_ifattr *config, unsigned int *num);

#ifdef __cplusplus
}
#endif
#endif  // RA_H