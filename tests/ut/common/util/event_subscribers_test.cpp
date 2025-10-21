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
 * Description: test event subscribe - notify mechanism
 */
#include <algorithm>
#include <mutex>

#include "common.h"
#include "datasystem/common/util/event_subscribers.h"
#include "datasystem/common/inject/inject_point.h"

namespace datasystem {
namespace ut {
class EventSubscribersTest : public CommonTest {
public:
    Status Execute(int num)
    {
        result_.emplace_back(num);
        return Status::OK();
    }

    enum EventType1 {EVENT0};
    using Event1 = EventSubscribers<EVENT0, std::function<Status()>>;
    enum Priority {
        ONE = 1,
        TWO,
        THREE
    };

    struct Subscriber1 {
        Subscriber1(EventSubscribersTest *base)
        {
            Event1::GetInstance().AddSubscriber(
                "Subscriber1", [base] { return base->Execute(ONE); }, ONE);
        }
        ~Subscriber1()
        {
            Event1::GetInstance().RemoveSubscriber("Subscriber1");
        }
    };
    struct Subscriber2 {
        Subscriber2(EventSubscribersTest *base)
        {
            Event1::GetInstance().AddSubscriber(
                "Subscriber2",
                [base] {
                    INJECT_POINT("event.notify.break");
                    return base->Execute(TWO);
                },
                TWO);
        }
        ~Subscriber2()
        {
            Event1::GetInstance().RemoveSubscriber("Subscriber2");
        }
    };
    struct Subscriber3 {
        Subscriber3(EventSubscribersTest *base)
        {
            Event1::GetInstance().AddSubscriber(
                "Subscriber3", [base] { return base->Execute(THREE); }, THREE);
        }
        ~Subscriber3()
        {
            Event1::GetInstance().RemoveSubscriber("Subscriber3");
        }
    };
    std::vector<int> result_;
};

TEST_F(EventSubscribersTest, MultiSubscribers)
{
    Subscriber2 s2(this);
    {
        Subscriber1 s1(this);
        Subscriber3 s3(this);
        Event1::GetInstance().NotifyAll();
        std::vector<int> expect = { 1, 2, 3 };
        ASSERT_EQ(result_, expect);
    }
    result_.clear();
    Event1::GetInstance().NotifyAll();
    std::vector<int> expect = { 2 };
    ASSERT_EQ(result_, expect);
}

TEST_F(EventSubscribersTest, BreakNotify)
{
    Subscriber3 s3(this);
    Subscriber1 s1(this);
    Subscriber2 s2(this);
    inject::Set("event.notify.break", "return(K_RUNTIME_ERROR)");

    auto status = Event1::GetInstance().NotifyAll();
    ASSERT_EQ(status.GetCode(), K_RUNTIME_ERROR);
    std::vector<int> expect = { 1 };
    ASSERT_EQ(result_, expect);
}
}  // namespace ut
}  // namespace datasystem