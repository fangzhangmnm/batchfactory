# BatchFactory

Composable, cache‑aware pipelines for **parallel LLM workflows**, API calls, and dataset generation.

> **Status — `v0.3` alpha.** More robust and battle-tested on small projects. Still evolving quickly — APIs may shift.

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

## Why BatchFactory?  **Three killer moves**

| 🏭 Mass data distillation & cleanup | 🎭 Multi-agent, multi-round workflows | 🌲 Hierarchical spawning (`ListParallel`) |
|---|---|---|
| Chain `GenerateLLMRequest → ConcurrentLLMCall → ExtractResponseText` after keyword / file sources to **mass-produce**, **filter**, or **polish** datasets—millions of Q&A rows, code explanations, translation pairs—with built-in caching & cost tracking. | With `Repeat`, `If`, `While`, and chat helpers, you can script complex role-based collaborations—e.g. *Junior Translator → Senior Editor → QA → Revision*—and run full multi-agent, multi-turn simulations in just a few lines of code. Ideal for workflows inspired by **TransAgents**, **MATT**, or **ChatDev**. | `ListParallel` breaks a complex item into fine-grained subtasks, runs them **concurrently**, then reunites the outputs—ideal for **long-text summarisation**, **RAG chunking**, or any tree-structured pipeline. |


---

### Spawn snippet (Text Segmentation)

<!-- TEXT_SEGMENTATION_EXAMPLE_PLACEHOLDER -->

---

### Loop snippet (Role‑Playing)

<!-- ROLEPLAY_EXAMPLE_PLACEHOLDER -->

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

| ✨ Example                 | Demonstrates                                        |
| ------------------------- | --------------------------------------------------- |
| **1\_quickstart**         | Linear LLM transform with caching and auto‑resuming |
| **2\_roleplay**           | Multi-agent, multi-turn roleplay using chat agents  |
| **3\_text\_segmentation** | Divide‑and‑conquer pipeline for text segmentation   |

---

## ⚙️ Broker & Cache Highlights

* Every expensive call is **hashed** to a unique `job_idx` — repeated prompts are automatically **deduplicated**.
* Control how failures propagate with `BrokerFailureBehavior = RETRY | STAY | EMIT`.
* On restart, `execute()` resumes from cached state and dispatches **only missing or incomplete jobs** — no manual checkpoints needed.

---

## 🛣️ Roadmap → v0.4

* Native **vector store** and **semantic search** nodes
* Streamlined **cost tracking** and **progress reporting**

---

### Available Ops

<!-- ALL_OPS_PLACEHOLDER -->

---

© 2025 · MIT License
