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
 * Description: dynamic bitmap
 */
#ifndef DATASYSTEM_DYN_BITMAP_H
#define DATASYSTEM_DYN_BITMAP_H
#include <algorithm>
#include <climits>
#include <cstdint>
#include <mutex>
#include <shared_mutex>
#include <sstream>
#include <vector>

namespace datasystem {

class DummyRWLock {
public:
    static inline void lock()
    {
    }
    static inline void unlock()
    {
    }
    static inline void lock_shared()
    {
    }
    static inline void unlock_shared()
    {
    }
};

template <class RWLOCK_TYPE>
class DynBitmap {
    static constexpr int UInt64Bits = sizeof(uint64_t) * CHAR_BIT;

public:
    DynBitmap(int size = 0)
    {
        if (size < 0) {
            size = 0;
        }
        size_ = size;
        if (size > 0) {
            data_.resize(BitChunkIndex(size), 0);
            UnsetRangeImpl(0, size - 1);
        }
    }
    ~DynBitmap()
    {
    }

    void Reserve(int size)
    {
        if (size > size_) {
            Resize(size);
        }
    }

    void Resize(int newSize)
    {
        if (newSize < 0) {
            newSize = 0;
        }
        std::unique_lock<RWLOCK_TYPE> l(lock_);
        int oldSize = size_;
        size_ = newSize;
        data_.resize(BitChunkIndex(size_), 0);
        if (newSize > oldSize) {
            UnsetRangeImpl(oldSize, newSize - 1);
        }
        if (!data_.empty() && size_ % UInt64Bits != 0) {
            data_.back() &= (1ULL << (size_ % UInt64Bits)) - 1;
        }
    }

    void Clear()
    {
        std::unique_lock<RWLOCK_TYPE> l(lock_);
        std::fill(data_.begin(), data_.end(), 0);
    }

    bool Set(int bitnum)
    {
        if (bitnum < 0 || bitnum >= size_)
            return false;
        std::unique_lock<RWLOCK_TYPE> l(lock_);
        data_[bitnum / UInt64Bits] |= (1ULL << (bitnum % UInt64Bits));
        return true;
    }

    bool SetRange(int start, int end)
    {
        if (start < 0 || end >= size_ || start > end)
            return false;
        std::unique_lock<RWLOCK_TYPE> l(lock_);
        SetRangeImpl(start, end);
        return true;
    }

    bool Unset(int bitnum)
    {
        if (bitnum < 0 || bitnum >= size_)
            return false;
        std::unique_lock<RWLOCK_TYPE> l(lock_);
        data_[bitnum / UInt64Bits] &= ~(1ULL << (bitnum % UInt64Bits));
        return true;
    }

    bool UnsetRange(int start, int end)
    {
        if (start < 0 || end >= size_ || start > end)
            return false;
        std::unique_lock<RWLOCK_TYPE> l(lock_);
        UnsetRangeImpl(start, end);
        return true;
    }

    bool IsSet(int bitnum) const
    {
        if (bitnum < 0 || bitnum >= size_)
            return false;
        std::shared_lock<RWLOCK_TYPE> l(lock_);
        return IsBitSet(bitnum);
    }

    bool IsRangeSet(int start, int end) const
    {
        if (start < 0 || end >= size_ || start > end)
            return false;
        std::shared_lock<RWLOCK_TYPE> l(lock_);
        for (int i = start; i <= end; ++i) {
            if (!(data_[i / UInt64Bits] & (1ULL << (i % UInt64Bits))))
                return false;
        }
        return true;
    }

    bool IsAllSet() const
    {
        if (size_ <= 0)
            return true;
        return IsRangeSet(0, size_ - 1);
    }

    std::string DebugSetString() const
    {
        std::shared_lock<RWLOCK_TYPE> l(lock_);
        if (size_ <= 0)
            return "";

        std::ostringstream oss;
        bool first = true;

        for (int i = 0; i < size_;) {
            while (i < size_ && !IsBitSet(i))
                i++;
            if (i >= size_)
                break;

            int start = i++;
            while (i < size_ && IsBitSet(i))
                i++;

            if (!first)
                oss << ",";
            first = false;
            int end = i - 1;
            oss << start << (start == end ? "" : "~" + std::to_string(end));
        }
        return oss.str();
    }

    std::string DebugUnsetString() const
    {
        std::shared_lock<RWLOCK_TYPE> l(lock_);
        if (size_ <= 0)
            return "";

        std::ostringstream oss;
        bool first = true;

        for (int i = 0; i < size_;) {
            while (i < size_ && IsBitSet(i))
                i++;
            if (i >= size_)
                break;

            int start = i++;
            while (i < size_ && !IsBitSet(i))
                i++;

            if (!first)
                oss << ",";
            first = false;
            int end = i - 1;
            oss << start << (start == end ? "" : "~" + std::to_string(end));
        }
        return oss.str();
    }

private:
    static inline int BitChunkIndex(int index)
    {
        return (index + UInt64Bits - 1) / UInt64Bits;
    }
    bool IsBitSet(int bitnum) const
    {
        return (data_[bitnum / UInt64Bits] & (1ULL << (bitnum % UInt64Bits))) != 0;
    }
    void SetRangeImpl(int start, int end)
    {
        int sb = start / UInt64Bits;
        int eb = end / UInt64Bits;
        if (sb == eb) {
            data_[sb] |= (GetHighMask(start % UInt64Bits) & GetLowMask(end % UInt64Bits));
        } else {
            data_[sb] |= GetHighMask(start % UInt64Bits);
            for (int i = sb + 1; i < eb; ++i)
                data_[i] = ~0ULL;
            data_[eb] |= GetLowMask(end % UInt64Bits);
        }
    }

    void UnsetRangeImpl(int start, int end)
    {
        int sb = start / UInt64Bits;
        int eb = end / UInt64Bits;
        if (sb == eb) {
            data_[sb] &= ~(GetHighMask(start % UInt64Bits) & GetLowMask(end % UInt64Bits));
        } else {
            data_[sb] &= ~GetHighMask(start % UInt64Bits);
            for (int i = sb + 1; i < eb; ++i)
                data_[i] = 0;
            data_[eb] &= ~GetLowMask(end % UInt64Bits);
        }
    }

    static inline uint64_t GetHighMask(int bit)
    {
        return bit == 0 ? ~0ULL : ~((1ULL << bit) - 1);
    }

    static inline uint64_t GetLowMask(int bit)
    {
        return bit == (UInt64Bits - 1) ? ~0ULL : ((1ULL << (bit + 1)) - 1);
    }

    std::vector<uint64_t> data_;
    int size_;
    mutable RWLOCK_TYPE lock_;
};

}  // namespace datasystem

#endif