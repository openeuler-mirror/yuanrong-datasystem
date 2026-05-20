#include "test_harness.h"
#include "pipeline/data_pattern.h"

TEST(PatternGenerate_Size) {
    auto data = GeneratePatternData(1024, 0);
    ASSERT_EQ(data.size(), 1024u);
}

TEST(PatternGenerate_Content) {
    auto data = GeneratePatternData(256, 0);
    for (uint64_t i = 0; i < data.size(); i++) {
        ASSERT_EQ(data[i], static_cast<char>(i % 256));
    }
}

TEST(PatternGenerate_SenderId) {
    auto data = GeneratePatternData(10, 5);
    for (uint64_t i = 0; i < data.size(); i++) {
        ASSERT_EQ(data[i], static_cast<char>((5 + i) % 256));
    }
}

TEST(PatternVerify_Match) {
    auto data = GeneratePatternData(1024, 42);
    ASSERT_TRUE(VerifyPatternData(data.data(), data.size(), 42));
}

TEST(PatternVerify_Mismatch) {
    auto data = GeneratePatternData(1024, 0);
    ASSERT_FALSE(VerifyPatternData(data.data(), data.size(), 1));
}

TEST(PatternGenerate_ZeroSize) {
    auto data = GeneratePatternData(0, 0);
    ASSERT_EQ(data.size(), 0u);
}

TEST(PatternGenerate_LargeSize) {
    auto data = GeneratePatternData(1024 * 1024, 0);
    ASSERT_EQ(data.size(), 1024u * 1024);
    ASSERT_TRUE(VerifyPatternData(data.data(), data.size(), 0));
}

