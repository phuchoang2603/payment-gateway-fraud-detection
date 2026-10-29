import sys
import urllib.request

PREFIXES = (
    "vllm:prefix_cache_queries",
    "vllm:prefix_cache_hits",
    "vllm:num_preemptions",
    "vllm:spec_decode_num_draft_tokens",
    "vllm:spec_decode_num_accepted_tokens",
    "vllm:prompt_tokens",
    "vllm:generation_tokens",
    "vllm:kv_cache_usage_perc",
)

with urllib.request.urlopen(sys.argv[1], timeout=10) as response:
    for line in response.read().decode().splitlines():
        if line.startswith(PREFIXES):
            print(line)
