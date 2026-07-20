#pragma once
#include <chrono>
#include <iostream>
#include <random>
#include <sched.h>
#include <thread>
#include <sstream>
#include <string>
#include <vector>
#include <unistd.h>

// HAS_LIBNUMA may be defined by CMake (preferred) or auto-detected below.
// Always include <numa.h> when HAS_LIBNUMA is set, regardless of whether it
// came from CMake or the __has_include fallback below.
#if !defined(HAS_LIBNUMA) && __has_include(<numa.h>)
#define HAS_LIBNUMA 1
#endif
#ifdef HAS_LIBNUMA
#include <numa.h>
#endif

static std::vector<int> ParseCpuList(const std::string &s) {
    std::vector<int> result;
    std::istringstream ss(s);
    std::string token;
    while (std::getline(ss, token, ',')) {
        try {
            auto dash = token.find('-');
            if (dash != std::string::npos) {
                int lo = std::stoi(token.substr(0, dash));
                int hi = std::stoi(token.substr(dash + 1));
                if (lo > hi) std::swap(lo, hi);
                for (int i = lo; i <= hi && i < CPU_SETSIZE; i++) {
                    if (i >= 0) result.push_back(i);
                }
            } else if (!token.empty()) {
                int cpu = std::stoi(token);
                if (cpu >= 0 && cpu < CPU_SETSIZE) result.push_back(cpu);
            }
        } catch (...) {}
    }
    return result;
}

static std::vector<int> GetAvailableCpus() {
    cpu_set_t mask;
    CPU_ZERO(&mask);
    if (sched_getaffinity(0, sizeof(mask), &mask) == 0) {
        std::vector<int> cpus;
        for (int i = 0; i < CPU_SETSIZE; i++) {
            if (CPU_ISSET(i, &mask)) cpus.push_back(i);
        }
        if (!cpus.empty()) return cpus;
    }
    int n = std::thread::hardware_concurrency();
    std::vector<int> cpus(n > 0 ? n : 1);
    for (int i = 0; i < (int)cpus.size(); i++) cpus[i] = i;
    return cpus;
}

static bool ApplyProcessAffinity(const std::vector<int> &cpus) {
    cpu_set_t mask;
    CPU_ZERO(&mask);
    for (int cpu : cpus) {
        if (cpu >= 0 && cpu < CPU_SETSIZE) CPU_SET(cpu, &mask);
    }
    return sched_setaffinity(0, sizeof(mask), &mask) == 0;
}

#ifdef HAS_LIBNUMA
static bool ApplyNumaAffinity(int node) {
    if (numa_available() < 0) return false;
    return numa_run_on_node(node) == 0;
}

// Pick a NUMA node at random from the configured nodes and bind the calling
// process to it. Returns true on success. Falls back to the cpu affinity
// path (caller's responsibility) on any failure: libnuma missing, no nodes,
// or numa_run_on_node failure. No warning emitted per design choice.
static bool ApplyRandomNumaAffinity() {
    if (numa_available() < 0) return false;
    int nodeCount = numa_num_configured_nodes();
    if (nodeCount <= 0) return false;
    unsigned seed = static_cast<unsigned>(
        std::chrono::steady_clock::now().time_since_epoch().count()) ^
        static_cast<unsigned>(getpid());
    std::mt19937 gen(seed);
    int chosen = static_cast<int>(gen() % static_cast<unsigned>(nodeCount));
    return numa_run_on_node(chosen) == 0;
}
#endif

// Unified affinity application from config values. Shared by RunServerMode
// and ChildProcessMain to avoid duplicating the NUMA-then-CPU fallback logic.
// randomNumaNode: when true, pick a NUMA node at random (mutually exclusive
// with numaNode >= 0; LoadConfig enforces this).
static void ApplyAffinityFromConfig(const std::string &cpuAffinity, int numaNode,
                                    bool randomNumaNode = false) {
#ifdef HAS_LIBNUMA
    if (randomNumaNode && ApplyRandomNumaAffinity()) {
        return;
    }
    if (!randomNumaNode && numaNode >= 0 && ApplyNumaAffinity(numaNode)) {
        return;
    }
#else
    (void)numaNode;
    (void)randomNumaNode;
#endif

    std::vector<int> cpus;
    if (!cpuAffinity.empty()) {
        cpus = ParseCpuList(cpuAffinity);
    }
    if (cpus.empty()) {
        cpus = GetAvailableCpus();
    }
    if (!ApplyProcessAffinity(cpus)) {
        std::cerr << "WARNING: ApplyProcessAffinity failed" << std::endl;
    }
}
