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

// ---- VerifyBuffer / BuildVerifyConfig tests ----

TEST(PatternByteAt_MatchesPattern) {
    for (int s : {0, 1, 42, 255}) {
        for (uint64_t i = 0; i < 512; i++) {
            ASSERT_EQ(PatternByteAt(i, s), static_cast<char>((s + i) % 256));
        }
    }
}

TEST(VerifyBuffer_Off_AlwaysTrue) {
    VerifyConfig vc; vc.level = VerifyLevel::OFF;
    char buf[16] = {0};
    ASSERT_TRUE(VerifyBuffer(buf, 16, 16, 0, vc, nullptr));
    // even size mismatch passes when OFF
    ASSERT_TRUE(VerifyBuffer(buf, 8, 16, 0, vc, nullptr));
}

TEST(VerifyBuffer_Size_Match) {
    VerifyConfig vc; vc.level = VerifyLevel::SIZE;
    auto data = GeneratePatternData(64, 7);
    VerifyFailReason r = VerifyFailReason::CONTENT;
    ASSERT_TRUE(VerifyBuffer(data.data(), 64, 64, 7, vc, &r));
    ASSERT_TRUE(r == VerifyFailReason::NONE);
}

TEST(VerifyBuffer_Size_Mismatch) {
    VerifyConfig vc; vc.level = VerifyLevel::SIZE;
    auto data = GeneratePatternData(64, 7);
    VerifyFailReason r = VerifyFailReason::NONE;
    ASSERT_FALSE(VerifyBuffer(data.data(), 64, 128, 7, vc, &r));
    ASSERT_TRUE(r == VerifyFailReason::SIZE);
}

TEST(VerifyBuffer_Full_Match) {
    VerifyConfig vc; vc.level = VerifyLevel::FULL;
    auto data = GeneratePatternData(256, 3);
    ASSERT_TRUE(VerifyBuffer(data.data(), 256, 256, 3, vc, nullptr));
}

TEST(VerifyBuffer_Full_ContentMismatch) {
    VerifyConfig vc; vc.level = VerifyLevel::FULL;
    auto data = GeneratePatternData(256, 3);
    data[10] = static_cast<char>(static_cast<unsigned char>(data[10]) ^ 0xFF);
    VerifyFailReason r = VerifyFailReason::NONE;
    ASSERT_FALSE(VerifyBuffer(data.data(), 256, 256, 3, vc, &r));
    ASSERT_TRUE(r == VerifyFailReason::CONTENT);
}

TEST(VerifyBuffer_Sample_Match) {
    VerifyConfig vc; vc.level = VerifyLevel::SAMPLE;
    vc.sampleBytes = 8;
    vc.sampleStepBytes = 64;
    auto data = GeneratePatternData(1024, 0);
    ASSERT_TRUE(VerifyBuffer(data.data(), 1024, 1024, 0, vc, nullptr));
}

TEST(VerifyBuffer_Sample_CatchesHeadCorruption) {
    VerifyConfig vc; vc.level = VerifyLevel::SAMPLE;
    vc.sampleBytes = 8;
    vc.sampleStepBytes = 64;
    auto data = GeneratePatternData(1024, 0);
    data[2] = static_cast<char>(static_cast<unsigned char>(data[2]) ^ 0xFF);
    ASSERT_FALSE(VerifyBuffer(data.data(), 1024, 1024, 0, vc, nullptr));
}

TEST(VerifyBuffer_Sample_CatchesTailCorruption) {
    VerifyConfig vc; vc.level = VerifyLevel::SAMPLE;
    vc.sampleBytes = 8;
    vc.sampleStepBytes = 64;
    auto data = GeneratePatternData(1024, 0);
    data[1023] = static_cast<char>(static_cast<unsigned char>(data[1023]) ^ 0xFF);
    ASSERT_FALSE(VerifyBuffer(data.data(), 1024, 1024, 0, vc, nullptr));
}

TEST(VerifyBuffer_Sample_CatchesMiddleSampleCorruption) {
    VerifyConfig vc; vc.level = VerifyLevel::SAMPLE;
    vc.sampleBytes = 8;
    vc.sampleStepBytes = 64;
    auto data = GeneratePatternData(1024, 0);
    // offset 128 is a sampled middle segment start (step=64: 64, 128, ...)
    data[128] = static_cast<char>(static_cast<unsigned char>(data[128]) ^ 0xFF);
    ASSERT_FALSE(VerifyBuffer(data.data(), 1024, 1024, 0, vc, nullptr));
}

TEST(VerifyBuffer_Sample_SkipsUnsampledByte) {
    VerifyConfig vc; vc.level = VerifyLevel::SAMPLE;
    vc.sampleBytes = 4;
    vc.sampleStepBytes = 256;
    auto data = GeneratePatternData(1024, 0);
    // offset 100 sits between head[0..3] and the next sample at 256; not sampled
    data[100] = static_cast<char>(static_cast<unsigned char>(data[100]) ^ 0xFF);
    ASSERT_TRUE(VerifyBuffer(data.data(), 1024, 1024, 0, vc, nullptr));
}

TEST(VerifyBuffer_Sample_BufferSmallerThanSegment) {
    VerifyConfig vc; vc.level = VerifyLevel::SAMPLE;
    vc.sampleBytes = 4096;  // larger than buffer
    vc.sampleStepBytes = 1024;
    auto data = GeneratePatternData(32, 0);
    ASSERT_TRUE(VerifyBuffer(data.data(), 32, 32, 0, vc, nullptr));
    data[5] = static_cast<char>(static_cast<unsigned char>(data[5]) ^ 0xFF);
    ASSERT_FALSE(VerifyBuffer(data.data(), 32, 32, 0, vc, nullptr));
}

TEST(VerifyBuffer_ZeroSize) {
    VerifyConfig vc; vc.level = VerifyLevel::FULL;
    ASSERT_TRUE(VerifyBuffer(nullptr, 0, 0, 0, vc, nullptr));
}

TEST(VerifyBuffer_SizeCheckBeforeContent) {
    // size mismatch with FULL level reports SIZE, not CONTENT
    VerifyConfig vc; vc.level = VerifyLevel::FULL;
    auto data = GeneratePatternData(64, 0);
    VerifyFailReason r = VerifyFailReason::NONE;
    ASSERT_FALSE(VerifyBuffer(data.data(), 64, 65, 0, vc, &r));
    ASSERT_TRUE(r == VerifyFailReason::SIZE);
}

TEST(BuildVerifyConfig_AllLevels) {
    ASSERT_TRUE(BuildVerifyConfig("off", 0, 0, false).level == VerifyLevel::OFF);
    ASSERT_TRUE(BuildVerifyConfig("size", 0, 0, false).level == VerifyLevel::SIZE);
    ASSERT_TRUE(BuildVerifyConfig("sample", 0, 0, false).level == VerifyLevel::SAMPLE);
    ASSERT_TRUE(BuildVerifyConfig("full", 0, 0, false).level == VerifyLevel::FULL);
}

TEST(BuildVerifyConfig_UnknownLevelFallsBackToSize) {
    auto vc = BuildVerifyConfig("garbage", 0, 0, true);
    ASSERT_TRUE(vc.level == VerifyLevel::SIZE);
}

TEST(BuildVerifyConfig_DefaultsOnZero) {
    auto vc = BuildVerifyConfig("sample", 0, 0, true);
    ASSERT_EQ(vc.sampleBytes, 4096ULL);
    ASSERT_EQ(vc.sampleStepBytes, 1024ULL * 1024);
    ASSERT_TRUE(vc.failOp);
}

