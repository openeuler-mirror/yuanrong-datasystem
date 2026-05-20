#pragma once
#include <cassert>
#include <cmath>
#include <cstdio>
#include <cstdlib>
#include <functional>
#include <stdexcept>
#include <string>
#include <vector>

struct TestCase {
    std::string name;
    std::function<void()> fn;
};

inline std::vector<TestCase>& GetTests() {
    static std::vector<TestCase> tests;
    return tests;
}

#define TEST(name) \
    static void test_##name(); \
    static bool reg_##name = (GetTests().push_back({#name, test_##name}), true); \
    static void test_##name()

struct TestFailure : std::runtime_error {
    using std::runtime_error::runtime_error;
};

#define ASSERT_EQ(a, b) do { if ((a) != (b)) { \
    throw TestFailure( \
        std::string(__FILE__) + ":" + std::to_string(__LINE__) + \
        ": " #a " != " #b); } } while(0)

#define ASSERT_TRUE(x) do { if (!(x)) { \
    throw TestFailure( \
        std::string(__FILE__) + ":" + std::to_string(__LINE__) + \
        ": " #x); } } while(0)

#define ASSERT_FALSE(x) ASSERT_TRUE(!(x))

#define ASSERT_NEAR(a, b, eps) do { if (std::abs((a)-(b)) > (eps)) { \
    throw TestFailure( \
        std::string(__FILE__) + ":" + std::to_string(__LINE__) + \
        ": " #a "=" + std::to_string((double)(a)) + \
        " vs " #b "=" + std::to_string((double)(b)) + \
        " (eps=" + std::to_string((double)(eps)) + ")"); } } while(0)

#define RUN_TESTS() do { \
    int passed = 0, failed = 0; \
    for (auto& tc : GetTests()) { \
        printf("  %-50s ", tc.name.c_str()); \
        try { tc.fn(); printf("PASS\n"); passed++; } \
        catch (TestFailure& e) { printf("FAIL\n  %s\n", e.what()); failed++; } \
        catch (...) { printf("FAIL (exception)\n"); failed++; } \
    } \
    printf("\n%d passed, %d failed\n", passed, failed); \
    return failed; } while(0)
