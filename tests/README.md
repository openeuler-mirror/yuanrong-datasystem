
## 测试用例介绍

CPP测试用例包含单元测试和系统测试，分别在ut和st目录下，使用gtest测试框架

## 测试用例名称规范

### 前缀
测试用例名称前缀有三种："DISABLED_"、"EXCLUSIVE_"、"LEVEL1_"，在gtest转换为ctest时会根据前缀给ctest配置不同参数
- DISABLED_：禁用用例
- EXCLUSIVE_：互斥执行，用例不与其他用例并发执行
- LEVEL1_：用例会被打上level1的标签
### 规范
1. 用例名称中不能带有"DISABLED_"，"EXCLUSIVE_"和"LEVEL1_"
2. "DISABLED_"要放在最前面，另外两个顺序随意
3. 对于运行时间超过30s的用例，建议设置为"LEVEL1_"

## 编译和运行用例

### 编译用例

```shell
bash build.sh -t build
```

### 编译并运行用例

```shell
# 编译、运行测试用例
bash build.sh -t run
# 编译、并行运行测试用例
bash build.sh -t run -u ${thread_num}
# 编译、运行测试用例，并收集覆盖率
bash build.sh -t run -c on
# 编译、运行指定标签的测试用例
bash build.sh -t run -l ${label}
```

### 运行单个用例

```shell
ctest -R test_suite.test_name
```

### 使用label运行用例

ut和st目录下的用例分别会打上"ut"和"st"的标签；用例名中以"LEVEL1_"开头的用例会打上"level1"的标签，否则会打上"level0"的标签
label的合法值：
- ut：包括ut的level0/level1用例
- st：包括st的level0/level1用例
- level0：包括level0的ut/st用例
- level1：包括level1的ut/st用例
- st level0：包括st的level0用例
- st level1：包括st的level1用例
- level*：包括全量用例

```shell
# 运行st用例
ctest -L st
# 运行level0的用例
ctest -L level0
# 运行所有level的用例
ctest -L level*
# 运行level0级别的st用例
ctest -L "st level0"

# 使用脚本运行所有ut
bash build.sh -t run_cases -l ut
# 使用脚本运行所有st
bash build.sh -t run_cases -l st
# 使用脚本运行level0的用例
bash build.sh -t run_cases -l level0
# 使用脚本运行st level0的用例
bash build.sh -t run_cases -l "st level0"
# 使用脚本运行所有level的用例
bash build.sh -t run_cases -l level*

```
