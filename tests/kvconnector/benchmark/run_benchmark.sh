#!/bin/bash
# Copyright (c) Huawei Technologies Co., Ltd. 2025. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Disable automatic loading of torch device backend
export TORCH_DEVICE_BACKEND_AUTOLOAD=0

# Run the benchmark script
# To capture logs, you can redirect stdout and stderr to a file:
# ./run_benchmark.sh > benchmark.log 2>&1
python /workspace/path/vllm_ascend/vllm/benchmarks/benchmark_serving.py \
    --backend=openai \
    --base-url=http://192.168.0.1:18500 \
    --dataset-name=custom \
    --dataset_path=/workspace/path/vllm_ascend/benchmark/dataset_12k_tokens_50p.jsonl \
    --max-concurrency=8 \
    --custom-output-len=2 \
    --num-prompts=3000 \
    --model=/workspace/models/qwen2.5_7B
