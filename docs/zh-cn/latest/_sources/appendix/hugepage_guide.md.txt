# 大页内存配置指南
适用系统：openEuler 22.03 / Ubuntu 22.04 及以上，内核 ≥ 5.10

重要性与风险提示
为何需要配置大页内存：大页内存能显著减少TLB未命中，提升内存访问效率，对数据密集型应用至关重要。
风险：错误配置可能导致系统内存不足、内核OOM或系统不稳定。请遵循以下步骤做前置检查，以2MB单个大页*1024个大页数量为例。

## 数据系统配置大页内存步骤
### 第一步：内存资源评估
1. 检查当前可用内存
    ```bash
    grep MemAvailable /proc/meminfo
    ```
要求： MemAvailable 的值 必须大于 3 GB。这是为了给大页分配和系统正常运行预留足够的安全边界。

如果不足：关闭其他占用内存的应用程序，或考虑在系统空闲时进行操作。

2. 检查内存碎片程度
    ```bash
    cat /proc/buddyinfo
    ```
找到您架构对应的行（如 Node 0， DMA32 或 Normal）。
从左到右，数字代表连续页块的数量。需要找到 order 为 9 的连续页块（因为 2^9 * 4KB = 2MB）。
简易判断：查看第10列（从0开始计数）的数字。如果这个数字 大于 100，则认为碎片化程度较低，分配成功率高。如果很小（如 < 10），则分配可能失败或非常缓慢。

若节点内存分布不均匀可定向扩池，或先整理内存再扩池。

### 第二步：执行动态分配
方法A：适用于单路（UMA）或简单环境
分配大页(需要root权限)

```bash
# 分配1280个大页(为系统预留256个)
sudo su -c "echo 1280 > /proc/sys/vm/nr_hugepages"
```

方法B：适用于多路服务器（NUMA架构）
1. 查看NUMA节点信息

    ```bash
    lscpu | grep NUMA
    ```

2. 在指定节点上分配 
在node0分配768个，node1分配512个
    ```bash
    sudo su -c "echo 768 > /sys/devices/system/node/node0/hugepages/hugepages-2048kB/nr_hugepages"
    sudo su -c "echo 512 > /sys/devices/system/node/node1/hugepages/hugepages-2048kB/nr_hugepages"
    ```

### 第三步：验证分配结果
分配命令是异步的，内核需要时间整理内存。请等待并验证。
1. 等待并检查：

    ```bash
    # 等待30秒内分配完成
    timeout 30 bash -c 'while [[ $(grep HugePages_Total /proc/meminfo | awk "{print \$2}") -lt 1280 ]]; do sleep 1; done'
    ```

2. 查看详细信息：
    ```bash
    grep -i huge /proc/meminfo
    ```

关注 HugePages_Total、HugePages_Free 和 HugePages_Rsvd。
成功标志：HugePages_Total 达到或接近目标值（1280）。

### 第四步：部署数据系统worker开启大页内存
启动数据系统时将enable_huge_tlb配置选项设置为true。
示例：
    ```bash
    dscli start -w --worker_address "${host}:${worker_port}" --etcd_address "${host}:${etcd_port}" --shared_memory_size_mb {shared_memory} --enable_huge_tlb true
    ```

### 第五步：确认应用在使用大页
数据系统部署后再次运行:
    ```bash
    grep -i huge /proc/meminfo
    ```
成功标志： HugePages_Free 的数量明显减少，HugePages_Rsvd 可能增加。这表明您的大页已经被应用成功申请和使用。