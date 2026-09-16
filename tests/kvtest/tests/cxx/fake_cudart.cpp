#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <deque>
#include <mutex>
#include <unordered_map>

namespace {
struct Transfer { void *dst; const void *src; size_t size; uint64_t sequence; };
struct Stream { std::deque<Transfer> queue; uint64_t sequence = 0; };
struct Event { Stream *stream = nullptr; uint64_t sequence = 0; };
std::mutex mutex;
std::unordered_map<void *, size_t> devices;
int deviceCount = 1;
int copies = 0;
int failCopyAt = 0;
int failEvent = 0;
int streamWaits = 0;
thread_local int selectedDevice = -1;
Stream *sharedStream = nullptr;

bool IsDevice(const void *ptr, size_t size) {
    const auto address = reinterpret_cast<uintptr_t>(ptr);
    for (const auto &item : devices) {
        const auto begin = reinterpret_cast<uintptr_t>(item.first);
        if (address >= begin && address - begin <= item.second && size <= item.second - (address - begin)) return true;
    }
    return false;
}
void Drain(Stream *stream, uint64_t end) {
    while (!stream->queue.empty() && stream->queue.front().sequence <= end) {
        const auto copy = stream->queue.front();
        std::memcpy(copy.dst, copy.src, copy.size);
        stream->queue.pop_front();
    }
}
}

extern "C" {
void FakeDeviceCount(int count) { deviceCount = count; }
void FakeFailCopyAfter(int count) { failCopyAt = copies + count; }
void FakeFailEvent() { failEvent = 1; }
int FakeCopyCount() { return copies; }
int FakeStreamWaits() { return streamWaits; }
size_t FakeAllocations() { return devices.size(); }
size_t FakePending() { return sharedStream ? sharedStream->queue.size() : 0; }
int cudaGetDeviceCount(int *count) { *count = deviceCount; return deviceCount == 0 ? 100 : 0; }
int cudaSetDevice(int device) { selectedDevice = device; return device == 0 ? 0 : 101; }
const char *cudaGetErrorString(int) { return "fake CUDA error"; }
int cudaHostRegister(void *, size_t, unsigned) { return selectedDevice == 0 ? 0 : 101; }
int cudaHostUnregister(void *) { return selectedDevice == 0 ? 0 : 101; }
int cudaMalloc(void **ptr, size_t size) {
    std::lock_guard<std::mutex> lock(mutex);
    *ptr = new char[size]; devices[*ptr] = size; return 0;
}
int cudaFree(void *ptr) {
    std::lock_guard<std::mutex> lock(mutex);
    if (sharedStream && !sharedStream->queue.empty()) return 1;
    devices.erase(ptr); delete[] static_cast<char *>(ptr); return 0;
}
int cudaStreamCreateWithFlags(void **ptr, unsigned flags) {
    if (flags != 1 || sharedStream) return 1;
    sharedStream = new Stream; *ptr = sharedStream; return 0;
}
int cudaStreamDestroy(void *ptr) { delete static_cast<Stream *>(ptr); sharedStream = nullptr; return 0; }
int cudaStreamSynchronize(void *ptr) {
    std::lock_guard<std::mutex> lock(mutex);
    ++streamWaits; auto *stream = static_cast<Stream *>(ptr); Drain(stream, stream->sequence); return 0;
}
int cudaEventCreateWithFlags(void **ptr, unsigned flags) {
    if (flags != 2) return 1;
    *ptr = new Event; return 0;
}
int cudaEventDestroy(void *ptr) { delete static_cast<Event *>(ptr); return 0; }
int cudaEventRecord(void *ptr, void *stream) {
    std::lock_guard<std::mutex> lock(mutex);
    if (failEvent) { failEvent = 0; return 1; }
    auto *event = static_cast<Event *>(ptr); event->stream = static_cast<Stream *>(stream);
    event->sequence = event->stream->sequence; return 0;
}
int cudaEventSynchronize(void *ptr) {
    std::lock_guard<std::mutex> lock(mutex);
    auto *event = static_cast<Event *>(ptr); Drain(event->stream, event->sequence); return 0;
}
int cudaMemcpyAsync(void *dst, const void *src, size_t size, int kind, void *ptr) {
    std::lock_guard<std::mutex> lock(mutex);
    if (selectedDevice != 0) return 101;
    if (++copies == failCopyAt) return 1;
    if (kind == 1 && (!IsDevice(dst, size) || IsDevice(src, size))) return 1;
    if (kind == 2 && (!IsDevice(src, size) || IsDevice(dst, size))) return 1;
    if (kind != 1 && kind != 2) return 1;
    auto *stream = static_cast<Stream *>(ptr);
    stream->queue.push_back({dst, src, size, ++stream->sequence}); return 0;
}
}
