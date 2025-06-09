# BatchFactory

Composable, cache‑aware batch processing pipelines for LLMs, APIs, and dataset generation.

> **Status: alpha – expect breaking changes.** Names and APIs will shift rapidly.

---

## Install

```bash
pip install batchfactory   # coming soon: pip install -U batchfactory
```

---

## Quick start

```python
import batchfactory as bf

project = bf.CacheFolder("example1", 1, 0, 0)
broker = bf.brokers.ConcurrentLLMCallBroker(project("cache/llm_broker.jsonl"))

# Build a small graph that rewrites passages into short English poems

g = (
    bf.ReadMarkdownLinesOp("./data/*.txt", "keyword", directory_str_field="directory")
    | bf.ShuffleOp(42)
    | bf.TakeFirstNOp(3)
    | bf.GenerateLLMRequestOp(
        'Rewrite the passage from "{directory}" titled "{keyword}" as a four-line English poem.',
        model="gpt-4o-mini@openai",
    )
    | bf.ConcurrentLLMCallOp(project("cache/llm_call1.jsonl"), broker)
    | bf.ExtractResponseTextOp()
    | bf.SaveJsonlOp(project("out/poems.jsonl"),output_fields=["keyword","text","directory"])
    | bf.PrintTextOp()
)

g = g.compile()
g.resume()
g.pump(dispatch_broker=True, reset_input=True)
```

---

## Core ideas (WIP)

* **Batch‑centric:** every op consumes/produces lists of *Entry* objects.
* **Atomic Ops:** small, single‑purpose units you compose with `|` or `wire()`.
* **Graph‑first:** compile once, then `resume()` and `pump()` as external brokers finish work.
* **Cache‑mindful:** transparent on‑disk ledgers allow you to pause and restart without recomputation.
* **Broker pattern:** offload long‑running or external work (LLMs, search APIs, human labeling) to pluggable brokers.
* **Loop‑aware:** native `For`, `While`, and `If` controls let graphs loop through subgraphs cleanly—ideal for multi-agent or staged workflows.

---

## Roadmap

* Stabilise op names
* Replace `OpGraph` executor with pluggable schedulers
* Public docs & more examples

---

© 2025 · MIT License
