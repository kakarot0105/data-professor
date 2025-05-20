# data-professor

[![PyPI version](https://badge.fury.io/py/data-professor.svg)](https://badge.fury.io/py/data-professor)  [![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

A conversational data exploration toolkit. Load tabular data via Spark, profile it, and ask questions in plain English—locally or via LLM APIs.

---

## 🔍 Features

* **Universal Connectors**: CSV, Parquet, Delta Lake, S3, JDBC (via Spark)
* **Automated Profiling**: row count, nulls, empty strings, distinct counts, numeric summaries
* **Interactive Chatbot**: `data-professor chat` REPL for Q\&A
* **Hybrid QA Engine**:

  * *Rule-based* for instant stats replies
  * *LLM-backed* fallback (OpenAI, Hugging Face, or local GPT-2)
* **Customizable**: YAML config, pluggable models, extend connectors and rules

---

## 🚀 Installation

```bash
pip install data-professor
```

Or from GitHub master:

```bash
pip install git+https://github.com/your-org/data-professor.git
```

---

## ⚙️ Configuration

Create or edit `config/default.yaml`:

```yaml
source:
  path: "s3://my-bucket/data/"   # or local file/folder
  format: "parquet"              # parquet, csv, delta, etc.
  options:                        # optional read options
    header: "true"
    inferSchema: "true"
spark:
  master: "local[*]"
llm:
  provider: "auto"               # openai, huggingface, or auto (uses API key)
  api_key: "${OPENAI_API_KEY}"  # or leave blank for local fallback
  model: "gpt-4"
  local_model: "gpt2"
```

---

## 💬 Quickstart (CLI)

```bash
data-professor chat \
  --config config/default.yaml \
  --source /path/to/data.csv \
  --format csv \
  --options '{"header":"true","inferSchema":"true"}'
```

Type queries like:

* `How many rows are there?`
* `List the columns.`
* `How many nulls in column age?`
* `Show sample values from column user_id.`

Enter `exit`, `quit`, or `bye` to end.

---

## 🐍 Quickstart (Python)

```python
from data_professor import DataProfessor

# initialize (reads config)
prof = DataProfessor(config_path="config/default.yaml")

# load data and profile
prof.load_source(
    source="data/my_data.parquet",
    format="parquet",
    options={}
)

# ask questions
print(prof.ask("How many nulls are there in 'user_id' ?"))
```

---

## 🔧 Extending

* Add new connectors in `src/data_professor/connectors/`
* Enhance profiling logic in `src/data_professor/profiler.py`
* Update/fine-tune rule patterns or LLM prompt in `src/data_professor/qa.py`

---

## 🤝 Contributing

1. Fork the repo and create a feature branch
2. Write tests (`pytest`) under `tests/`
3. Ensure all checks pass
4. Submit a PR with a clear description

---

## 📄 License

Released under the [MIT License](https://opensource.org/licenses/MIT).
