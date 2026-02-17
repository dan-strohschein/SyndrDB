#!/bin/bash
# Start vLLM serving the fine-tuned SyndrQL IR model.
#
# Usage:
#   ./setup-vllm.sh                          # defaults
#   ./setup-vllm.sh /path/to/syndrql-merged  # custom model path
#
# The model is served with an OpenAI-compatible API at http://127.0.0.1:8000
# Use the /v1/chat/completions endpoint (not /v1/completions) since the model
# was fine-tuned with chat template format.
#
# Test with:
#   curl http://127.0.0.1:8000/v1/chat/completions \
#     -H "Content-Type: application/json" \
#     -d '{
#       "model": "syndrql-model",
#       "messages": [
#         {"role": "system", "content": "You are a SyndrQL query generator. Given a natural language request, respond with a single JSON object matching the AIAssistantResponse schema. Output ONLY valid JSON — no explanation, no markdown, no extra text."},
#         {"role": "user", "content": "Show me all customers"}
#       ],
#       "temperature": 0.1,
#       "max_tokens": 512
#     }'

MODEL_PATH="${1:-./syndrql-merged}"

python -m vllm.entrypoints.openai.api_server \
  --model "$MODEL_PATH" \
  --served-model-name syndrql-model \
  --dtype auto \
  --max-model-len 2048 \
  --gpu-memory-utilization 0.75 \
  --host 127.0.0.1 \
  --port 8000
