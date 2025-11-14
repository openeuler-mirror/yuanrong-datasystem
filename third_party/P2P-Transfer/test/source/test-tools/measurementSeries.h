
/**
 * Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
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
#ifndef MEASUREMENT_SERIES_H
#define MEASUREMENT_SERIES_H
#include <algorithm>
#include <numeric>
#include <vector>

constexpr size_t SKIP_AMOUNT = 2;
constexpr size_t MEDIAN_DIV_2 = 2;

class MeasurementSeries {
public:
    void Add(double v)
    {
        data.push_back(v);
    }

    double Value()
    {
        if (data.empty()) {
            return 0.0;
        }

        std::sort(data.begin(), data.end());

        if (data.size() <= SKIP_AMOUNT)
            return std::accumulate(data.begin(), data.end(), 0.0) / data.size();

        return std::accumulate(data.begin() + 1, data.end() - 1, 0.0) / (data.size() - SKIP_AMOUNT);
    }

    double Median()
    {
        if (data.empty()) {
            return 0.0;
        }

        std::sort(data.begin(), data.end());
        auto mid = data.size() / MEDIAN_DIV_2;

        return (data.size() % MEDIAN_DIV_2 == 0) ? (data[mid] + data[mid + 1]) / MEDIAN_DIV_2 : data[mid];
    }

    double MinValue()
    {
        if (data.empty()) {
            return 0.0;
        }

        return *std::min_element(data.begin(), data.end());
    }

    double MaxValue()
    {
        if (data.empty()) {
            return 0.0;
        }

        return *std::max_element(data.begin(), data.end());
    }

    double Spread()
    {
        if (data.size() <= 1) {
            return 0.0;
        }

        double minVal = MinValue();
        double maxVal = MaxValue();

        return std::abs(maxVal - minVal) / Value();
    }

private:
    std::vector<double> data;
};

#endif