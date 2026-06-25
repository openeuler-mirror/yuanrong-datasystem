#include "test_harness.h"
#include "common/cpu_affinity.h"

// --- ParseCpuList ---

TEST(ParseCpuList_Single) {
    auto cpus = ParseCpuList("3");
    ASSERT_EQ(cpus.size(), 1u);
    ASSERT_EQ(cpus[0], 3);
}

TEST(ParseCpuList_CommaList) {
    auto cpus = ParseCpuList("0,2,4");
    ASSERT_EQ(cpus.size(), 3u);
    ASSERT_EQ(cpus[0], 0);
    ASSERT_EQ(cpus[1], 2);
    ASSERT_EQ(cpus[2], 4);
}

TEST(ParseCpuList_Range) {
    auto cpus = ParseCpuList("0-7");
    ASSERT_EQ(cpus.size(), 8u);
    for (int i = 0; i < 8; i++) ASSERT_EQ(cpus[i], i);
}

TEST(ParseCpuList_Mixed) {
    auto cpus = ParseCpuList("0-3,7,15");
    ASSERT_EQ(cpus.size(), 6u);
    ASSERT_EQ(cpus[0], 0);
    ASSERT_EQ(cpus[3], 3);
    ASSERT_EQ(cpus[4], 7);
    ASSERT_EQ(cpus[5], 15);
}

TEST(ParseCpuList_ReverseRange) {
    auto cpus = ParseCpuList("7-4");
    ASSERT_EQ(cpus.size(), 4u);
    ASSERT_EQ(cpus[0], 4);
    ASSERT_EQ(cpus[3], 7);
}

TEST(ParseCpuList_Empty) {
    auto cpus = ParseCpuList("");
    ASSERT_EQ(cpus.size(), 0u);
}

TEST(ParseCpuList_InvalidChars) {
    auto cpus = ParseCpuList("abc,def");
    ASSERT_EQ(cpus.size(), 0u);
}

TEST(ParseCpuList_NegativeIgnored) {
    auto cpus = ParseCpuList("-1,0,1");
    ASSERT_EQ(cpus.size(), 2u);
    ASSERT_EQ(cpus[0], 0);
    ASSERT_EQ(cpus[1], 1);
}

TEST(ParseCpuList_Whitespace) {
    auto cpus = ParseCpuList(" 0 , 1 ");
    ASSERT_EQ(cpus.size(), 2u);
    ASSERT_EQ(cpus[0], 0);
    ASSERT_EQ(cpus[1], 1);
}

// --- GetAvailableCpus ---

TEST(GetAvailableCpus_NonEmpty) {
    auto cpus = GetAvailableCpus();
    ASSERT_TRUE(cpus.size() > 0u);
}

// --- ApplyProcessAffinity ---

TEST(ApplyProcessAffinity_WithAvailableCpus) {
    auto cpus = GetAvailableCpus();
    ASSERT_TRUE(ApplyProcessAffinity(cpus));
}

TEST(ApplyProcessAffinity_SingleCpu) {
    auto cpus = GetAvailableCpus();
    ASSERT_TRUE(cpus.size() > 0u);
    ASSERT_TRUE(ApplyProcessAffinity({cpus[0]}));
}

TEST(ApplyProcessAffinity_EmptyList) {
    ASSERT_FALSE(ApplyProcessAffinity({}));
}

// --- ApplyNumaAffinity ---

#ifdef HAS_LIBNUMA
TEST(ApplyNumaAffinity_Node0_IfAvailable) {
    if (numa_available() >= 0) {
        ASSERT_TRUE(ApplyNumaAffinity(0));
    }
}

TEST(ApplyNumaAffinity_OutOfRangeNode) {
    if (numa_available() >= 0) {
        int maxNode = numa_max_node();
        ASSERT_FALSE(ApplyNumaAffinity(maxNode + 100));
    }
}
#endif
