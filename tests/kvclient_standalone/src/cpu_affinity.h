#pragma once
#include <sched.h>
#include <thread>
#include <sstream>
#include <string>
#include <vector>

static std::vector<int> ParseCpuList(const std::string &s) {
    std::vector<int> result;
    std::istringstream ss(s);
    std::string token;
    while (std::getline(ss, token, ',')) {
        auto dash = token.find('-');
        if (dash != std::string::npos) {
            int lo = std::stoi(token.substr(0, dash));
            int hi = std::stoi(token.substr(dash + 1));
            for (int i = lo; i <= hi; i++) result.push_back(i);
        } else if (!token.empty()) {
            result.push_back(std::stoi(token));
        }
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
    for (int cpu : cpus) CPU_SET(cpu, &mask);
    return sched_setaffinity(0, sizeof(mask), &mask) == 0;
}
