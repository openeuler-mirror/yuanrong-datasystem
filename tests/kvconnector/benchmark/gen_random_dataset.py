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

import os
import random
import string
import json
import logging
from tqdm import tqdm
# Import the generic AutoTokenizer
from transformers import AutoTokenizer

# Configure logging to output to stdout
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def gen_random_string(length=10):
    """Generate a random string of specified length"""
    return "".join(random.choices(string.ascii_letters + string.digits, k=length))


def gen_random_tokens(tokenizer, num_tokens):
    """
    Generate a piece of random text that, after tokenization, has exactly the specified number of tokens.

    Args:
        tokenizer: The transformers tokenizer instance to use.
        num_tokens: Target number of tokens.

    Returns:
        A randomly generated text string.
    """
    token_ids = []
    # Loop to generate until we have enough tokens
    while len(token_ids) < num_tokens:
        random_word = gen_random_string(random.randint(3, 8))
        # Add a space before the word to simulate word spacing in real text for more accurate tokenization
        token_ids.extend(tokenizer.encode(" " + random_word, add_special_tokens=False))

    # Precisely truncate to the target number of tokens
    final_token_ids = token_ids[:num_tokens]

    # Decode the token id list back to a text string
    return tokenizer.decode(final_token_ids)


def gen_random_prompts(tokenizer, num_groups, num_prompts_per_group, prefix_tokens, suffix_tokens):
    """
    Generate a random prompt dataset with length based on token count.

    Args:
        tokenizer: The transformers tokenizer instance to use.
        num_groups: Number of groups.
        num_prompts_per_group: Number of prompts per group.
        prefix_tokens: Number of prefix tokens.
        suffix_tokens: Number of suffix tokens.

    Returns:
        A list of randomly generated prompts.
    """
    prompts = []
    logging.info(
        f"Starting to generate dataset (Number of groups: {num_groups}, Prompts per group: {num_prompts_per_group})...")

    for _ in tqdm(range(num_groups), desc="Generating groups"):
        prefix = gen_random_tokens(tokenizer, prefix_tokens)
        for _ in range(num_prompts_per_group):
            suffix = gen_random_tokens(tokenizer, suffix_tokens)
            prompt = prefix + " " + suffix
            prompts.append(prompt)

    random.shuffle(prompts)
    return prompts


def save_to_file(prompts, output_file):
    """Save generated prompts to a JSONL file"""
    with open(output_file, 'w', encoding='utf-8') as f:
        for prompt in tqdm(prompts, desc="Writing to file"):
            data = {"prompt": prompt}
            json_line = json.dumps(data, ensure_ascii=False)
            f.write(json_line + '\n')

    logging.info(f"Successfully saved {len(prompts)} entries to {output_file}")


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

    # Set random seed
    random.seed(config['seed'])

    # ==============================================================================
    # Load generic tokenizer from specified path
    # ==============================================================================
    tokenizer_path = config['tokenizer_path']
    logging.info(f"Loading tokenizer from '{tokenizer_path}'...")
    try:
        # AutoTokenizer can automatically identify and load the correct tokenizer
        # trust_remote_code=True is necessary for loading some community custom models
        tokenizer = AutoTokenizer.from_pretrained(tokenizer_path, trust_remote_code=True)
        # For some models (like Llama), they may not have a pad_token by default.
        if tokenizer.pad_token is None:
            tokenizer.pad_token = tokenizer.eos_token
            logging.info("Tokenizer pad_token not set, has been set to eos_token.")

        logging.info("Tokenizer loaded successfully.")
    except Exception as e:
        logging.error(f"Error: Unable to load tokenizer from path '{tokenizer_path}'.")
        logging.error(
            f"Please confirm if the path is correct and if the folder contains files like " +
            "'tokenizer.json' or 'tokenizer_config.json'."
        )
        logging.error(f"Detailed error message: {e}")
        return  # Exit the program if tokenizer loading fails

    # Create output directory
    os.makedirs(config['output_dir'], exist_ok=True)
    output_path = os.path.join(config['output_dir'], config['output_file'])

    # Generate dataset
    total_prompts = config['num_groups'] * config['num_prompts_per_group']
    total_tokens = config['prefix_length'] + config['suffix_length']
    logging.info(f"Will generate a total of {total_prompts} entries, each with approximately {total_tokens} tokens.")
    logging.info(f"(Number of groups: {config['num_groups']}, Prompts per group: {config['num_prompts_per_group']})")

    prompts = gen_random_prompts(tokenizer, config['num_groups'], config['num_prompts_per_group'],
                                 config['prefix_length'], config['suffix_length'])

    # Save dataset
    save_to_file(prompts, output_path)


if __name__ == "__main__":
    main()
