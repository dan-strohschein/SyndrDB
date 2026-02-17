"""
Fine-tune Qwen2.5-3B-Instruct to convert natural language → SyndrQL IR JSON.

Usage:
    python train_syndrql.py [--data training_data.jsonl] [--epochs 5] [--output ./syndrql-adapter]

The model learns to respond with a single JSON object matching the
AIAssistantResponse schema (the "ir" field from training data).
Chat template format is used so the instruct model retains its
system/user/assistant turn structure.

IMPORTANT: Uses Unsloth's train_on_responses_only() to mask the loss on
system and user tokens.  Without this, the model trains on ALL tokens
(including the prompt), which causes training collapse — the model produces
gibberish instead of JSON at inference time.
"""

from unsloth import FastLanguageModel
from unsloth.chat_templates import train_on_responses_only
from trl import SFTTrainer, SFTConfig
from datasets import load_dataset
import torch
import json
import argparse

# ---------------------------------------------------------------------------
# CLI args
# ---------------------------------------------------------------------------
parser = argparse.ArgumentParser(description="Fine-tune SyndrQL IR model")
parser.add_argument("--data", default="training_data.jsonl",
                    help="Path to training JSONL (fields: nl, ir, syndrql)")
parser.add_argument("--epochs", type=int, default=5,
                    help="Number of training epochs")
parser.add_argument("--output", default="./syndrql-adapter",
                    help="Directory to save the LoRA adapter")
parser.add_argument("--merged-output", default="./syndrql-merged",
                    help="Directory to save the merged full model")
parser.add_argument("--batch-size", type=int, default=4,
                    help="Per-device training batch size")
parser.add_argument("--grad-accum", type=int, default=4,
                    help="Gradient accumulation steps")
parser.add_argument("--lr", type=float, default=2e-4,
                    help="Learning rate")
args = parser.parse_args()

# ---------------------------------------------------------------------------
# System prompt — must be identical at training AND inference time
# ---------------------------------------------------------------------------
SYSTEM_PROMPT = (
    "You are a SyndrQL query generator. Given a natural language request, "
    "respond with a single JSON object matching the AIAssistantResponse schema. "
    "Output ONLY valid JSON — no explanation, no markdown, no extra text."
)

# ---------------------------------------------------------------------------
# Qwen2.5 chat template markers
# ---------------------------------------------------------------------------
# Qwen2.5-Instruct uses the ChatML format:
#   <|im_start|>system\n{system}<|im_end|>\n
#   <|im_start|>user\n{user}<|im_end|>\n
#   <|im_start|>assistant\n{assistant}<|im_end|>\n
#
# We tell train_on_responses_only() where instructions vs responses begin
# so the loss is ONLY computed on assistant tokens.
INSTRUCTION_PART = "<|im_start|>user\n"
RESPONSE_PART = "<|im_start|>assistant\n"

# ---------------------------------------------------------------------------
# Load base model in 4-bit (QLoRA)
# ---------------------------------------------------------------------------
print("Loading base model...")
model, tokenizer = FastLanguageModel.from_pretrained(
    model_name="Qwen/Qwen2.5-3B-Instruct",
    max_seq_length=2048,
    dtype=None,
    load_in_4bit=True,
)

# ---------------------------------------------------------------------------
# Add LoRA adapters
# ---------------------------------------------------------------------------
model = FastLanguageModel.get_peft_model(
    model,
    r=16,
    target_modules=[
        "q_proj", "k_proj", "v_proj", "o_proj",
        "gate_proj", "up_proj", "down_proj",
    ],
    lora_alpha=16,
    lora_dropout=0,
    bias="none",
    use_gradient_checkpointing="unsloth",
)

# ---------------------------------------------------------------------------
# Load and format training data using chat template
# ---------------------------------------------------------------------------
print(f"Loading training data from {args.data}...")
dataset = load_dataset("json", data_files=args.data, split="train")
print(f"Loaded {len(dataset)} examples")


def format_chat(example):
    """Format each example as a chat conversation.

    The user message is the natural language query.
    The assistant response is the compact IR JSON — this is what the model
    learns to produce.  Using compact JSON (no whitespace) teaches the model
    to output a single block with no prose.
    """
    user_msg = example["nl"]

    # The target is the IR object — compact JSON, no whitespace
    ir_output = json.dumps(example["ir"], separators=(",", ":"))

    messages = [
        {"role": "system", "content": SYSTEM_PROMPT},
        {"role": "user", "content": user_msg},
        {"role": "assistant", "content": ir_output},
    ]
    return {"text": tokenizer.apply_chat_template(messages, tokenize=False)}


dataset = dataset.map(format_chat)

# ---------------------------------------------------------------------------
# Verify a formatted example looks correct
# ---------------------------------------------------------------------------
print("\n--- Sample formatted training example (first 500 chars) ---")
print(dataset[0]["text"][:500])
print("---\n")

# ---------------------------------------------------------------------------
# Train with response-only loss masking
# ---------------------------------------------------------------------------
print(f"Starting training: {args.epochs} epochs, batch={args.batch_size}, "
      f"grad_accum={args.grad_accum}, lr={args.lr}")

trainer = SFTTrainer(
    model=model,
    tokenizer=tokenizer,
    train_dataset=dataset,
    args=SFTConfig(
        per_device_train_batch_size=args.batch_size,
        gradient_accumulation_steps=args.grad_accum,
        warmup_steps=10,
        num_train_epochs=args.epochs,
        learning_rate=args.lr,
        fp16=not torch.cuda.is_bf16_supported(),
        bf16=torch.cuda.is_bf16_supported(),
        logging_steps=10,
        output_dir=args.output,
        optim="adamw_8bit",
        seed=42,
        max_seq_length=2048,
        dataset_text_field="text",
    ),
)

# ---------------------------------------------------------------------------
# CRITICAL: Mask loss on system + user tokens.
#
# Without this, SFTTrainer computes loss on ALL tokens including the system
# prompt and user question.  The model spends most of its capacity learning
# to predict "You are a SyndrQL query generator..." instead of learning to
# produce IR JSON.  This causes training collapse — the model outputs
# gibberish at inference time.
#
# train_on_responses_only() replaces the data collator so that only tokens
# after "<|im_start|>assistant\n" contribute to the loss.
# ---------------------------------------------------------------------------
trainer = train_on_responses_only(
    trainer,
    instruction_part=INSTRUCTION_PART,
    response_part=RESPONSE_PART,
)

stats = trainer.train()
print(f"Training completed in {stats.metrics['train_runtime']:.0f}s")
print(f"Final loss: {stats.metrics.get('train_loss', 'N/A')}")

# ---------------------------------------------------------------------------
# Save adapter (~200MB)
# ---------------------------------------------------------------------------
print(f"Saving adapter to {args.output}...")
model.save_pretrained(args.output)
tokenizer.save_pretrained(args.output)

# ---------------------------------------------------------------------------
# Merge into full model for deployment (~7GB)
# ---------------------------------------------------------------------------
print(f"Merging and saving full model to {args.merged_output}...")
model.save_pretrained_merged(
    args.merged_output, tokenizer, save_method="merged_16bit"
)
print("Done.")
