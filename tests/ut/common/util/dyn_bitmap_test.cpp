/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2024. All rights reserved.
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

/**
 * Description: Dynamic bitmap test.
 */
#include "datasystem/common/util/dyn_bitmap.h"

#include "ut/common.h"

namespace datasystem {
namespace ut {

class DynBitmapTest : public CommonTest {
public:
    void SetUp() override
    {
        CommonTest::SetUp();
    }

    void TearDown() override
    {
        CommonTest::TearDown();
    }
};

/**
 * Test basic lifecycle operations: construction, resize, reserve, and clear
 */
TEST_F(DynBitmapTest, TestLifecycleOperations)
{
    // Test default constructor
    DynBitmap<DummyRWLock> bitmap1;
    EXPECT_TRUE(bitmap1.IsAllSet());

    // Test constructor with size
    DynBitmap<DummyRWLock> bitmap2(10);
    EXPECT_FALSE(bitmap2.IsAllSet());
    for (int i = 0; i < 10; i++) {
        EXPECT_FALSE(bitmap2.IsSet(i));
    }

    // Test constructor with negative size (should be treated as 0)
    DynBitmap<DummyRWLock> bitmap3(-1);
    EXPECT_TRUE(bitmap3.IsAllSet());

    // Test Reserve: smaller size should not change current state
    DynBitmap<DummyRWLock> bitmap4(5);
    EXPECT_TRUE(bitmap4.Set(4));
    bitmap4.Reserve(3);  // Reserve smaller size
    EXPECT_TRUE(bitmap4.IsSet(4));

    // Test Reserve: larger size
    bitmap4.Reserve(10);
    EXPECT_FALSE(bitmap4.IsSet(9));  // New bits should be unset

    // Test Resize: expand and shrink with data preservation
    DynBitmap<DummyRWLock> bitmap5(10);
    EXPECT_TRUE(bitmap5.Set(0));
    EXPECT_TRUE(bitmap5.Set(5));
    EXPECT_TRUE(bitmap5.Set(9));

    // Resize larger
    bitmap5.Resize(20);
    EXPECT_TRUE(bitmap5.IsSet(0));
    EXPECT_TRUE(bitmap5.IsSet(5));
    EXPECT_TRUE(bitmap5.IsSet(9));
    EXPECT_FALSE(bitmap5.IsSet(15));  // New bits should be unset

    // Set more bits in extended area
    EXPECT_TRUE(bitmap5.Set(15));
    EXPECT_TRUE(bitmap5.Set(19));

    // Resize smaller
    bitmap5.Resize(12);
    EXPECT_TRUE(bitmap5.IsSet(0));
    EXPECT_TRUE(bitmap5.IsSet(5));
    EXPECT_TRUE(bitmap5.IsSet(9));
    EXPECT_FALSE(bitmap5.IsSet(12));  // Out of range now
    EXPECT_FALSE(bitmap5.IsSet(19));  // Out of range now

    // Resize to negative (should be treated as 0)
    bitmap5.Resize(-1);
    EXPECT_TRUE(bitmap5.IsAllSet());

    // Test Clear
    DynBitmap<DummyRWLock> bitmap6(10);
    for (int i = 0; i < 10; i++) {
        EXPECT_TRUE(bitmap6.Set(i));
    }
    EXPECT_TRUE(bitmap6.IsAllSet());
    bitmap6.Clear();
    for (int i = 0; i < 10; i++) {
        EXPECT_FALSE(bitmap6.IsSet(i));
    }
    EXPECT_FALSE(bitmap6.IsAllSet());
}

/**
 * Test bit operations: Set, Unset (single bit and range)
 */
TEST_F(DynBitmapTest, TestBitOperations)
{
    DynBitmap<DummyRWLock> bitmap(20);

    // Test single bit Set operations
    EXPECT_TRUE(bitmap.Set(0));
    EXPECT_TRUE(bitmap.IsSet(0));
    EXPECT_TRUE(bitmap.Set(9));
    EXPECT_TRUE(bitmap.IsSet(9));

    // Test invalid single Set
    EXPECT_FALSE(bitmap.Set(-1));
    EXPECT_FALSE(bitmap.Set(20));
    EXPECT_FALSE(bitmap.Set(100));

    // Test range Set operations
    EXPECT_TRUE(bitmap.SetRange(5, 9));
    for (int i = 5; i <= 9; i++) {
        EXPECT_TRUE(bitmap.IsSet(i));
    }
    EXPECT_FALSE(bitmap.IsSet(4));
    EXPECT_FALSE(bitmap.IsSet(10));

    // Test single bit range Set
    DynBitmap<DummyRWLock> bitmap2(10);
    EXPECT_TRUE(bitmap2.SetRange(5, 5));
    EXPECT_TRUE(bitmap2.IsSet(5));
    EXPECT_FALSE(bitmap2.IsSet(4));
    EXPECT_FALSE(bitmap2.IsSet(6));

    // Test invalid range Set
    EXPECT_FALSE(bitmap.SetRange(-1, 5));
    EXPECT_FALSE(bitmap.SetRange(5, 20));
    EXPECT_FALSE(bitmap.SetRange(10, 5));

    // Test single bit Unset operations
    EXPECT_TRUE(bitmap.Unset(0));
    EXPECT_FALSE(bitmap.IsSet(0));
    EXPECT_TRUE(bitmap.Unset(9));
    EXPECT_FALSE(bitmap.IsSet(9));

    // Test invalid single Unset
    EXPECT_FALSE(bitmap.Unset(-1));
    EXPECT_FALSE(bitmap.Unset(20));
    EXPECT_FALSE(bitmap.Unset(100));

    // Test range Unset operations
    DynBitmap<DummyRWLock> bitmap3(20);
    EXPECT_TRUE(bitmap3.SetRange(0, 19));
    EXPECT_TRUE(bitmap3.UnsetRange(5, 9));
    for (int i = 5; i <= 9; i++) {
        EXPECT_FALSE(bitmap3.IsSet(i));
    }
    EXPECT_TRUE(bitmap3.IsSet(4));
    EXPECT_TRUE(bitmap3.IsSet(10));

    // Test single bit range Unset
    EXPECT_TRUE(bitmap3.UnsetRange(0, 0));
    EXPECT_FALSE(bitmap3.IsSet(0));

    // Test invalid range Unset
    EXPECT_FALSE(bitmap3.UnsetRange(-1, 5));
    EXPECT_FALSE(bitmap3.UnsetRange(5, 20));
    EXPECT_FALSE(bitmap3.UnsetRange(10, 5));
}

/**
 * Test query operations: IsSet, IsRangeSet, IsAllSet
 */
TEST_F(DynBitmapTest, TestQueryOperations)
{
    DynBitmap<DummyRWLock> bitmap(20);

    // Initially all unset
    EXPECT_FALSE(bitmap.IsAllSet());

    // Test IsSet for unset bits
    EXPECT_FALSE(bitmap.IsSet(0));
    EXPECT_FALSE(bitmap.IsSet(5));
    EXPECT_FALSE(bitmap.IsSet(19));

    // Test invalid IsSet
    EXPECT_FALSE(bitmap.IsSet(-1));
    EXPECT_FALSE(bitmap.IsSet(20));
    EXPECT_FALSE(bitmap.IsSet(100));

    // Set a range and test IsRangeSet
    EXPECT_TRUE(bitmap.SetRange(5, 9));
    EXPECT_TRUE(bitmap.IsRangeSet(5, 9));
    EXPECT_FALSE(bitmap.IsRangeSet(4, 9));   // Partial range
    EXPECT_FALSE(bitmap.IsRangeSet(5, 10));  // Partial range

    // Test invalid IsRangeSet
    EXPECT_FALSE(bitmap.IsRangeSet(-1, 5));
    EXPECT_FALSE(bitmap.IsRangeSet(5, 20));
    EXPECT_FALSE(bitmap.IsRangeSet(10, 5));

    // Set all bits and test IsAllSet
    EXPECT_TRUE(bitmap.SetRange(0, 19));
    EXPECT_TRUE(bitmap.IsAllSet());
    EXPECT_TRUE(bitmap.IsRangeSet(0, 19));

    // Unset one bit
    EXPECT_TRUE(bitmap.Unset(5));
    EXPECT_FALSE(bitmap.IsAllSet());
    EXPECT_FALSE(bitmap.IsRangeSet(0, 19));

    // Test empty bitmap
    DynBitmap<DummyRWLock> emptyBitmap;
    EXPECT_TRUE(emptyBitmap.IsAllSet());
}

/**
 * Test debug string generation: DebugSetString and DebugUnsetString
 */
TEST_F(DynBitmapTest, TestDebugStringGeneration)
{
    // Test DebugSetString
    DynBitmap<DummyRWLock> bitmap(10);
    EXPECT_EQ(bitmap.DebugSetString(), "");

    // Set continuous bits
    EXPECT_TRUE(bitmap.Set(1));
    EXPECT_TRUE(bitmap.Set(2));
    EXPECT_TRUE(bitmap.Set(3));
    EXPECT_EQ(bitmap.DebugSetString(), "1~3");

    // Add isolated bit
    EXPECT_TRUE(bitmap.Set(5));
    EXPECT_EQ(bitmap.DebugSetString(), "1~3,5");

    // Add another range
    EXPECT_TRUE(bitmap.Set(7));
    EXPECT_TRUE(bitmap.Set(8));
    EXPECT_EQ(bitmap.DebugSetString(), "1~3,5,7~8");

    // Test DebugUnsetString
    EXPECT_TRUE(bitmap.SetRange(0, 9));
    EXPECT_EQ(bitmap.DebugUnsetString(), "");

    // Unset continuous bits
    EXPECT_TRUE(bitmap.Unset(1));
    EXPECT_TRUE(bitmap.Unset(2));
    EXPECT_TRUE(bitmap.Unset(3));
    EXPECT_EQ(bitmap.DebugUnsetString(), "1~3");

    // Unset isolated bit
    EXPECT_TRUE(bitmap.Unset(5));
    EXPECT_EQ(bitmap.DebugUnsetString(), "1~3,5");

    // Unset another range
    EXPECT_TRUE(bitmap.Unset(7));
    EXPECT_TRUE(bitmap.Unset(8));
    EXPECT_EQ(bitmap.DebugUnsetString(), "1~3,5,7~8");

    // Test empty bitmap
    DynBitmap<DummyRWLock> emptyBitmap;
    EXPECT_EQ(emptyBitmap.DebugSetString(), "");
    EXPECT_EQ(emptyBitmap.DebugUnsetString(), "");
}

/**
 * Test boundary conditions and special cases
 */
TEST_F(DynBitmapTest, TestBoundaryConditions)
{
    // Test size at uint64_t boundary (64 bits)
    DynBitmap<DummyRWLock> bitmap64(64);
    EXPECT_TRUE(bitmap64.Set(0));    // First bit
    EXPECT_TRUE(bitmap64.Set(63));   // Last bit
    EXPECT_TRUE(bitmap64.IsSet(0));
    EXPECT_TRUE(bitmap64.IsSet(63));

    // Test entire range at boundary
    EXPECT_TRUE(bitmap64.SetRange(0, 63));
    EXPECT_TRUE(bitmap64.IsAllSet());

    // Test range crossing chunk boundary (128 bits, two uint64_t chunks)
    DynBitmap<DummyRWLock> bitmap128(128);
    EXPECT_TRUE(bitmap128.SetRange(60, 70));  // Crosses 64-bit boundary
    for (int i = 60; i <= 70; i++) {
        EXPECT_TRUE(bitmap128.IsSet(i));
    }
    EXPECT_FALSE(bitmap128.IsSet(59));
    EXPECT_FALSE(bitmap128.IsSet(71));

    // Test size 1
    DynBitmap<DummyRWLock> bitmap1(1);
    EXPECT_FALSE(bitmap1.IsSet(0));
    EXPECT_TRUE(bitmap1.Set(0));
    EXPECT_TRUE(bitmap1.IsSet(0));
    EXPECT_TRUE(bitmap1.IsAllSet());

    // Test size 0
    DynBitmap<DummyRWLock> bitmap0(0);
    EXPECT_TRUE(bitmap0.IsAllSet());
    EXPECT_FALSE(bitmap0.Set(0));
    EXPECT_FALSE(bitmap0.IsSet(0));
}

/**
 * Test complex scenarios with multiple operations
 */
TEST_F(DynBitmapTest, TestComplexScenarios)
{
    DynBitmap<DummyRWLock> bitmap(100);

    // Set a range
    EXPECT_TRUE(bitmap.SetRange(10, 20));
    EXPECT_TRUE(bitmap.IsRangeSet(10, 20));

    // Partially unset the range
    EXPECT_TRUE(bitmap.UnsetRange(15, 18));
    EXPECT_TRUE(bitmap.IsSet(10));
    EXPECT_TRUE(bitmap.IsSet(14));
    EXPECT_FALSE(bitmap.IsSet(15));
    EXPECT_FALSE(bitmap.IsSet(18));
    EXPECT_TRUE(bitmap.IsSet(19));

    // Set individual bits back
    EXPECT_TRUE(bitmap.Set(15));
    EXPECT_TRUE(bitmap.Set(16));
    EXPECT_TRUE(bitmap.IsRangeSet(15, 16));

    // Verify other bits remain unchanged
    EXPECT_FALSE(bitmap.IsSet(0));
    EXPECT_FALSE(bitmap.IsSet(5));
    EXPECT_FALSE(bitmap.IsSet(25));

    // Clear and verify all bits are unset
    bitmap.Clear();
    EXPECT_FALSE(bitmap.IsRangeSet(10, 20));
    for (int i = 0; i < 100; i++) {
        EXPECT_FALSE(bitmap.IsSet(i));
    }

    // Test alternating set/unset pattern
    DynBitmap<DummyRWLock> bitmap2(10);
    for (int i = 0; i < 10; i += 2) {
        EXPECT_TRUE(bitmap2.Set(i));
    }
    for (int i = 0; i < 10; i += 2) {
        EXPECT_TRUE(bitmap2.IsSet(i));
        EXPECT_FALSE(bitmap2.IsSet(i + 1));
    }
    EXPECT_EQ(bitmap2.DebugSetString(), "0,2,4,6,8");
    EXPECT_EQ(bitmap2.DebugUnsetString(), "1,3,5,7,9");

    // Fill gaps
    for (int i = 1; i < 10; i += 2) {
        EXPECT_TRUE(bitmap2.Set(i));
    }
    EXPECT_TRUE(bitmap2.IsAllSet());
    EXPECT_EQ(bitmap2.DebugSetString(), "0~9");
    EXPECT_EQ(bitmap2.DebugUnsetString(), "");
}

}  // namespace ut
}  // namespace datasystem