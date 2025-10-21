# vllm-ascend + yuanrong connector 调测指南

介绍如何将 vllm-ascend 和 YuanRong 分布式 KV 缓存系统结合，来进行调测。

## 环境准备

这里使用 vllm-ascend v0.10.0rc1 的配套镜像，镜像可以自己从 quay.io 拉取

```shell
# Update the vllm-ascend image
export IMAGE=ddl.test.huawei.com/datasystem/vllm-ascend:v0.10.0rc1-openeuler
docker run \
--name "容器名"  \
--privileged -itu root  -d --shm-size 64g    \
--net=host \
--device=/dev/davinci0:/dev/davinci0    \
--device=/dev/davinci1:/dev/davinci1    \
--device=/dev/davinci2:/dev/davinci2    \
--device=/dev/davinci3:/dev/davinci3    \
--device=/dev/davinci4:/dev/davinci4    \
--device=/dev/davinci5:/dev/davinci5    \
--device=/dev/davinci6:/dev/davinci6    \
--device=/dev/davinci7:/dev/davinci7    \
--device=/dev/davinci_manager:/dev/davinci_manager    \
--device=/dev/devmm_svm:/dev/devmm_svm    \
--device=/dev/hisi_hdc:/dev/hisi_hdc    \
-v /usr/local/dcmi:/usr/local/dcmi \
-v /usr/local/bin/npu-smi:/usr/local/bin/npu-smi \
-v /usr/local/Ascend/driver/lib64/:/usr/local/Ascend/driver/lib64/ \
-v /usr/local/Ascend/driver/version.info:/usr/local/Ascend/driver/version.info \
-v /etc/ascend_install.info:/etc/ascend_install.info \
-v /root/.cache:/root/.cache \
 -v /workspace:/workspace \
-it $IMAGE bash
```

镜像中自带了 vllm 和 vllm-ascend，如果需要切换版本或自己开发修改的话，可以 `pip uninstall` 卸载掉

## 编译安装

### vllm-ascend 准备

当前 vllm ascend + yuanrong connector 还没有合入 vllm ascend 主干，需要 git clone vllm-ascend 代码仓，checkout 到 v0.10.0rc1 的 tag，并 git am 0001-implement-yr-datasystem-connector-and-support-multimoda.patch。
具体地：
**1、下载 vllm-ascend 代码，切到 v0.10.0rc1 的 tag**

- vllm-ascend github 仓库地址：https://github.com/vllm-project/vllm-ascend

```shell
git clone https://github.com/vllm-project/vllm-ascend.git
cd vllm-ascend
git checkout v0.10.0rc1
```

**2、下载 yuanrong 补丁，应用到 vllm-ascend 代码**

- 补丁路径：datasystem/tests/kvconnector/patch/0001-implement-yr-datasystem-connector-and-support-multimoda.patch

```shell
git am 0001-implement-yr-datasystem-connector-and-support-multimoda.patch
```

注意！！！ 20250902 这个时间点，patch 中为了支持多模态，对 scheduler 做了一些修改（修改来源计算的同事），其中部分修改不兼容 v0.10.0rc1，导致 KVCache 满了的时候，调度会异常。请规避该问题，或者正面修复它。

**3、代码就绪后，执行以下命令完成编译安装**

```shell
python setup.py develop
```

### vllm 准备

vllm 可以直接使用镜像自带的版本，或者自己参考官网教程下载配套的版本.

## 安装数据系统依赖

yr_datasystem 是必选项，必须要安装使用。

dllm 后续会解除依赖，当前还需要安装，并且保证 dllm 和 yr_datasystem 是配套的。

```shell
pip install yr_datasystem-2.2-cp311-cp311-manylinux2014_aarch64.whl
pip install dllm-0.0.1-cp311-cp311-linux_aarch64.whl
```

## 部署执行

部署脚本路径：datasystem/tests/kvconnector/

- **kvconnector/deploy_script**： 部署脚本整合，详细可见该目录下的 README.md，部署相关的配置请见 deploy_env.sh 文件。
- **kvconnector/benchmark**: 提供了生成随机数据集和一键运行 vllm 自带 benchmark 的脚本

注意！！！当前 deploy_script 的部署脚本是支持多模态的，如果不运行多模态的话，需要在 deploy_env.sh 中修改 MODEL_PATH，并把 XX_VLLM_EXTRA_ARGS 中的内容注释掉。

## benchmark 测试

### 随机数据固定匹配命中率

kvconnector/benchmark/gen_random_dataset.py 可以生成固定匹配命中率的随机数据集。
具体配置可见:

```shell
def main():
    """
    Main function to generate random dataset with specified token lengths.
    This function loads a tokenizer, generates random prompts based on configuration,
    and saves them to a JSONL file.
    """
    # ==============================================================================
    # Configuration - Parts you need to modify
    # ==============================================================================
    config = {
        # Fill in your local model folder path or model name on Hugging Face here
        # For example: 'gpt2', './my_local_llama_model', 'Qwen/Qwen1.5-7B-Chat'
        'tokenizer_path': '/workspace/models/qwen2.5_7B',
        'num_groups': 300,
        'num_prompts_per_group': 10,
        'prefix_length': 6 * 1024,  # Prefix token count
        'suffix_length': 6 * 1024,  # Suffix token count
        'output_dir': '.',
        'output_file': 'dataset_12k_tokens_50p.jsonl',
        'seed': 42
    }
```
生成的数据集可以通过 run_bechmark.sh 运行:

```shell
python /workspace/path/vllm_ascend/vllm/benchmarks/benchmark_serving.py \
    --backend=openai \
    --base-url=http://192.168.0.1:18500 \
    --dataset-name=custom \
    --dataset_path=/workspace/path/vllm_ascend/benchmark/dataset_12k_tokens_50p.jsonl \
    --max-concurrency=8 \
    --custom-output-len=2 \
    --num-prompts=3000 \
    --model=/workspace/models/qwen2.5_7B
```