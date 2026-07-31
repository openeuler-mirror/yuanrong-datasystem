# 测试概要设计：越界样例

scope_check 应报越界（继承/锁/字段罗列/伪代码）。

## 4. 整体设计

### 4.1 模块划分

class Router : public BaseRouter {
    int field_a;
    int field_b;
    int field_c;
};

```cpp
for (int i = 0; i < keys.size(); i++) {
    if (keys[i].hash() % shard == owner) {
        route(keys[i]);
    }
}
```

### 4.3 关键设计机制

std::mutex route_lock_ 保护路由表。

#### D1. 新增亲和路由
