# p2p-transfer

P2P transfer is a lightweight library for efficient peer to peer transfer between pairs of devices, aiming to provide
low communicator setup latency, low memory consumption and high bandwidth for both large and small (e.g. 256kiB)
block sizes.

## Building and installing

See the [BUILDING](BUILDING.md) document.

## Interfaces

For easy integration, P2P transfer interfaces are similar to HCCL and can be found in `include/p2p.h`.

### Environment variables

Similar to HCCL, P2P uses 2 environment variables in order to exchange necessary information through the host NIC
for communicator setup:
- `IF_IP_DEFAULT`: The host interface IP(s) which can be used by P2P for communicator information exchange
    - Default: `0.0.0.0`
- `P2P_PORT_RANGE`: The port range on the given interface(s) used by P2P for communicator information exchange
    - Default: `56800-56900`

## Performance

### HCCS Performance

- **Small Block Bandwidth:** When transfering 128 512 KB blocks, P2P Provides 18.77 GB/s of bandwidth compared to
  11.24GB/s when using HCCL.
- **Communicator Setup Latency:** Communicator setup latency is around 2ms, compared to 236ms for HCCL when HCCP is
  already running and 3391ms when HCCP needs to be started.
- **Memory usage:** Each communicator only requires 16MB of memory, compared to the 200MB default for HCCL.
  Memory usage can be adjusted by changing the P2P_BLOCK_SIZE_BYTES and P2P_NUM_PINGPONG_BUFF variables in the
  codebase. However, when reducing memory requirements it is recommended to do careful benchmarking to ensure
  the desired performance is still met. We have not seen much benefit of increasing memory requirements beyond
  16 MB.

### RoCE Performance

RoCE support is currently still under development. We expect to achieve similar transfer bandwidth, however communicator
setup latency is likely to be higher than HCCS due to RoCE NIC configuration.

## TODOS
- [ ] Currently only works for communication between devices managed by different processes, not between 2 devices managed by the same process (e.g. multiple threads)
- [ ] Verify container support
- [ ] Add automatic server and topology detection to distinguish whether a connection is RoCE / HCCS
- [ ] RoCE support
- [ ] Support for IPv6 addresses
- [ ] Support for 910C Cross-node HCCS (`rtSetIpcMemorySuperPodPid`) + detect if two 910C NPUs in same superpod -> HCCS available
- [ ] Add support for multiple streams, currently things will go wrong if send on multiple streams at the same time due to not enough synchronization
- [ ] Clean up and restructure codebase for one sided operations
- [ ] Use ra_wrlist api for reduced host overhead
- [ ] Support for direct HBM buffer registration to avoid intermediate buffer, better perf for some scenarios (benchmark RoCE bandwidth for different block sizes)
- [ ] Check for which notifies can remove RA_SEND_FENCE
- [ ] Check memcpy_s results
- [ ] Improve error messages
- [ ] Add batch gather to launch multiple gathers at once
- [ ] Check and align chunk transfer to hugepage boundaries in case of non clean transfer sizes (e.g. 72k)
- [ ] Verify everything thread safe when 2 threads create communicator
- [ ] P2PUnregisterHostMem
- [ ] Check everywhere if there are fields we forgot to initialzie in class to default value, otherwise value is undefined (e.g. isIdSet)
- [ ] Run compiler static analysis, increase test coverage etc.
- [ ] Test concurrent (multithread communicator creation etc.)
- [ ] Properly add multi qp support
- [ ] Add scatter/gather dma (see sg_list, max_send_sge, max_recv_sge). Maybe can do direct transfer with scatter gather, avoiding SDMA?
- [ ] Add batch gather/scatter API, since at 31 * 73 KiB with 2MB chunks we don't get much RDMA-SDMA overlap resulting in only 14GB/s. If we do 256 * 71 KiB we get 20GB/s
- [ ] Add numa awareness, which gives a 1.5-2GB/s performance improvement
- [ ] Make sure FFTS task doesn't get too big or queue depth exceeded
- [ ] Check how much HBM and host DDR we can register to a QP and globally across QPs
- [ ] Check and debug max HBM and host mem can register to NIC

## Trace

```bash
# Build with debug symbols
cmake -S . -B build -D p2p-transfer_DEVELOPER_MODE=ON
cmake --build build

perf record -F 99 -g ./build/test/p2p-transfer_test_h2d
perf script > out.perf

cd perf
./FlameGraph-master/stackcollapse-perf.pl < ../out.perf > out.folded
./FlameGraph-master/flamegraph.pl out.folded > flamegraph.svg
```

