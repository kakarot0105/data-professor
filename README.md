### Interactive Chatbot Mode

Once installed, you can start an interactive REPL to ask questions about your dataset:

```bash
data-professor chat \
  --config config/default.yaml \
  --source s3://my-bucket/path/ \
  --format parquet