ICoordinatorDiscovery
=====================

.. cpp:class:: ICoordinatorDiscovery

    :header-file: #include <datasystem/utils/coordinator_discovery.h>
    :namespace: datasystem

    Coordinator 地址发现接口。用户可以实现该接口，并通过
    :cpp:member:`CoordinatorServiceDiscoveryOptions::coordinatorDiscovery` 注入 Coordinator 地址发现逻辑。

    **公共函数**

    .. cpp:function:: virtual Status GetCoordinators(std::vector<std::string> &serviceList) = 0

        获取 Coordinator 地址列表。

        实现可以在发现操作成功但当前没有候选地址时返回 ``OK`` 和空列表，具体消费者负责判断空列表
        是否可接受。当前 :cpp:class:`CoordinatorServiceDiscovery` 初始化时拒绝空列表；非空列表只缓存
        并使用首个 Coordinator 地址，后续查询不会重新调用该接口。

        参数：
            - **serviceList** - 传出参数。Coordinator 地址列表，每个地址的格式为 ``host:port``。

        返回：
            操作结果状态码。

    **实现示例**

    .. code-block:: cpp

        class CustomCoordinatorDiscovery : public datasystem::ICoordinatorDiscovery {
        public:
            datasystem::Status GetCoordinators(std::vector<std::string> &serviceList) override
            {
                serviceList = { "127.0.0.1:31511" };
                return datasystem::Status::OK();
            }
        };
