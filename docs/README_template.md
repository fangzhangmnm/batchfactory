# BatchFactory

Composable, cache‑aware pipelines for **parallel LLM workflows**, API calls, and dataset generation.

> **Status — `v0.4` beta.** More robust and battle-tested on small projects. Still evolving quickly — APIs may shift.

![BatchFactory cover](https://raw.githubusercontent.com/fangzhangmnm/batchfactory/main/docs/assets/batchfactory.jpg)

[📦 GitHub Repository →](https://github.com/fangzhangmnm/batchfactory)

---

## Install

```bash
pip install batchfactory            # latest tag
pip install --upgrade batchfactory  # grab the newest patch
```

---

## Quick‑start

<!-- QUICK_START_EXAMPLE_PLACEHOLDER -->

Run it twice – everything after the first run is served from the on‑disk ledger.

---

## 🚀 Why BatchFactory?

BatchFactory lets you build **cache‑aware, composable pipelines** for LLM calls, embeddings, and data transforms—so you can go from idea to production with zero boilerplate.

* **Composable Ops** – chain 30‑plus ready‑made Ops (and your own) using simple pipe syntax.
* **Transparent Caching & Cost Tracking** – every expensive call is hashed, cached, resumable, and audited.
* **Pluggable Brokers** – swap in LLM, embedding, search, or human‑in‑the‑loop brokers at will.
* **Self‑contained datasets** – pack arrays, images, audio—any data—into each entry so your entire workflow travels as a single, copy‑anywhere `.jsonl` file.
* **Ready‑to‑Copy Demos** – learn the idioms fast with five concise example pipelines.

---

## 🧩 Three killer moves

| 🏭 Mass data distillation & cleanup | 🎭 Multi‑agent, multi‑round workflows | 🌲 Hierarchical spawning (`ListParallel`) |
|---|---|---|
| Chain `GenerateLLMRequest → CallLLM → ExtractResponseText` after keyword / file sources to **mass‑produce**, **filter**, or **polish** datasets—millions of Q&A rows, code explanations, translation pairs—with built‑in caching & cost tracking. | With `Repeat`, `If`, `While`, and chat helpers, you can script complex role‑based collaborations—e.g. *Junior Translator → Senior Editor → QA → Revision*—and run full multi‑agent, multi‑turn simulations in just a few lines of code. Ideal for workflows inspired by **TransAgents**, **MATT**, or **ChatDev**. | `ListParallel` breaks a complex item into fine‑grained subtasks, runs them **concurrently**, then reunites the outputs—perfect for **long‑text summarisation**, **RAG chunking**, or any tree‑structured pipeline. |

---


### Spawn snippet (Text Segmentation)

<!-- TEXT_SEGMENTATION_EXAMPLE_PLACEHOLDER -->

---

### Loop snippet (Role‑Playing)

<!-- ROLEPLAY_EXAMPLE_PLACEHOLDER -->

---

### Text Embedding snippet

<!-- EMBEDDING_EXAMPLE_PLACEHOLDER -->

---

## Core concepts (one‑liner view)


| Term          | Story in one sentence                                                                                                                              |               |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- |
| **Entry**     | Tiny record with immutable `idx`, mutable `data`, auto‑incrementing `rev`.                                                                         |               |
| **Op**        | Atomic node; compose with `|`or`wire()`. |
| **Graph**     | A chain of `Op`s wired together — supports flexible pipelines and subgraphs.                                                                       |               |
| **Executor**  | Internal engine that tracks graph state, manages batching, resumption, and broker dispatch. Created automatically when you call `graph.execute()`. |               |
| **Broker**    | Pluggable engine for expensive or async jobs (LLM APIs, search, human labelers).                                                                   |               |
| **Ledger**    | Append‑only JSONL backing each broker & graph — enables instant resume and transparent caching.                                                    |               |
| **execute()** | High-level command that runs the graph: creates an `Executor`, resumes from cache, and dispatches brokers as needed.                               |               |

---

## 📚 Example Gallery

| ✨ Example               | Shows                                         |
|-------------------------|-----------------------------------------------|
| **1_quickstart**        | Linear LLM transform with caching & auto‑resume |
| **2_roleplay**          | Multi‑agent, multi‑turn roleplay with chat agents |
| **3_text_segmentation** | Divide‑and‑conquer pipeline for text segmentation |
| **4_prompt_management** | Prompt + data templating in one place          |
| **5_embeddings**        | Embeddings + cosine similarity workflow        |

---

### Available Ops

<!-- ALL_OPS_PLACEHOLDER -->

---

© 2025 · MIT License
