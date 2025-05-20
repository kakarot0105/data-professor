# src/data_professor/qa.py

import os
import json
import re
import openai
from huggingface_hub import InferenceApi
from transformers import AutoTokenizer, AutoModelForCausalLM, pipeline

class QAEngine:
    """
    QA engine that:
      - Answers simple stats queries via regex (row count, missing values, distinct counts).
      - Lists columns when requested.
      - Delegates free-form Q&A to OpenAI or Hugging Face if configured.
      - Falls back to a small local model (GPT-2) if no API key is available.
    """

    def __init__(self, llm_config: dict):
        self.raw_provider = llm_config.get("provider", "auto").lower()
        self.api_key      = llm_config.get("api_key", "").strip()
        self.remote_model = llm_config.get("model")
        self.local_model  = llm_config.get("local_model", "gpt2")

        # Determine provider
        if self.raw_provider in ("openai", "huggingface") and self.api_key:
            self.provider = self.raw_provider
        elif self.raw_provider == "auto" and self.api_key:
            self.provider = "openai"
        else:
            self.provider = "local"

        # Initialize remote services
        if self.provider == "openai":
            openai.api_key = self.api_key
        elif self.provider == "huggingface":
            if self.api_key:
                self.hf = InferenceApi(repo_id=self.remote_model, token=self.api_key)
            else:
                self.hf = InferenceApi(repo_id=self.remote_model)

        # Initialize local model if needed
        if self.provider == "local":
            self._init_local_model()

    def _init_local_model(self):
        self.tokenizer = AutoTokenizer.from_pretrained(self.local_model)
        if self.tokenizer.pad_token is None:
            self.tokenizer.pad_token = self.tokenizer.eos_token
        self.model = AutoModelForCausalLM.from_pretrained(self.local_model)
        self.generator = pipeline(
            "text-generation",
            model=self.model,
            tokenizer=self.tokenizer,
            device=-1,  # CPU; set to 0 for GPU
            framework="pt"
        )

    def _build_prompt(self, question: str, metadata: dict) -> str:
        meta_json = json.dumps(metadata, indent=2)
        # Truncate metadata for local model to avoid context overflow
        if self.provider == "local":
            toks = self.tokenizer(meta_json, truncation=True, max_length=800)
            meta_json = self.tokenizer.decode(toks["input_ids"], skip_special_tokens=True)
        return (
            "You are a data-professor assistant. Here is the dataset metadata:\n"
            f"{meta_json}\n\n"
            f"Question: {question}\n"
            "Answer concisely:"
        )

    def answer(self, question: str, metadata: dict) -> str:
        q = question.strip().lower()

        # List columns
        if re.search(r"\b(list|name|show)\b.*\bcolumns\b", q):
            cols = ", ".join(metadata.keys())
            return f"The dataset has the following columns: {cols}."

        # Total rows
        if re.search(r"\b(how many|what is the)\s+(rows|records|entries)\b", q) or \
           re.search(r"\b(row|record|entry)\s+count\b", q):
            first = next(iter(metadata.values()), {})
            total = first.get("total_rows")
            if total is not None:
                return f"The dataset has {total} rows."

        # Missing values (nulls + empty strings)
        m = re.search(r"(?:nulls?|missing values|empty strings?)\s*(?:in\s*['\"]?(\w+)['\"]?)?", q)
        if m:
            col = m.group(1)
            if col and col in metadata:
                stats = metadata[col]
                nulls = stats.get("null_count", 0)
                empties = stats.get("empty_count", 0)
                total_missing = nulls + empties
                return (f"Column '{col}' has {total_missing} missing values "
                        f"({nulls} nulls and {empties} empty strings).")
            else:
                total_missing = sum(
                    md.get("null_count", 0) + md.get("empty_count", 0)
                    for md in metadata.values()
                )
                return f"The dataset has {total_missing} missing values across all columns."

        # Distinct values
        m = re.search(r"(?:distinct values?|unique values?)\s*(?:in\s*['\"]?(\w+)['\"]?)?", q)
        if m:
            col = m.group(1)
            if col and col in metadata:
                cnt = metadata[col].get("distinct_count", 0)
                return f"Column '{col}' has {cnt} distinct values."

        # Fallback to LLM
        prompt = self._build_prompt(question, metadata)

        if self.provider == "openai":
            resp = openai.ChatCompletion.create(
                model=self.remote_model,
                messages=[
                    {"role": "system", "content": "Answer based on metadata."},
                    {"role": "user",   "content": prompt}
                ],
                temperature=0.0,
                max_tokens=256
            )
            return resp.choices[0].message.content.strip()

        if self.provider == "huggingface":
            out = self.hf(inputs=prompt, parameters={"max_new_tokens":256})
            if isinstance(out, list):
                return out[0].get("generated_text", "").strip()
            return out.get("generated_text", "").strip()

        # Local fallback
        out = self.generator(
            prompt,
            max_new_tokens=128
        )
        return out[0]["generated_text"].strip()

