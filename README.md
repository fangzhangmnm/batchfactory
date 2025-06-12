# BatchFactory

Composable, cache‑aware pipelines for **parallel LLM workflows**, API calls, and dataset generation.

> **Status — `v0.2` alpha.**  Stable enough for prototypes; expect fast‑moving APIs.

---

## Install

```bash
pip install batchfactory            # latest tag
pip install --upgrade batchfactory  # grab the newest patch
```

---

## Quick‑start

```python
import batchfactory as bf
from batchfactory.op import *

project = bf.CacheFolder("quickstart", 1, 0, 0)
broker  = bf.brokers.ConcurrentLLMCallBroker(project["cache/llm_broker.jsonl"])

# Rewrite the first three passages of every *.txt file into four‑line poems.

g = (
    ReadMarkdownLines("./data/*.txt", keyword_key="keyword", directory_key="directory")
    | Shuffle(42)
    | TakeFirstN(3)
    | GenerateLLMRequest(
        'Rewrite the passage from "{directory}" titled "{keyword}" as a four‑line poem.',
        model="gpt-4o-mini@openai",
    )
    | ConcurrentLLMCall(project["cache/llm_call.jsonl"], broker)
    | ExtractResponseText()
    | WriteJsonl(project["out/poems.jsonl"], output_keys=["keyword", "text", "directory"])
    | Print()
)

g.compile().execute(dispatch_brokers=True)
```

Run it twice – everything after the first run is served from the on‑disk ledger.

---

## Why BatchFactory?  **Three killer moves**

## Why BatchFactory?  **Three killer moves**

| 🏭 Mass data distillation & cleanup | 🎭 Multi-agent, multi-round workflows | 🌲 Hierarchical spawning (`ListParallel`) |
|---|---|---|
| Chain `GenerateLLMRequest → ConcurrentLLMCall → ExtractResponseText` behind keyword or file sources to **bulk-create, filter, or refine datasets** (think millions of Q&A rows, code explanations, translation pairs) with caching and cost tracking built-in. | `Repeat` plus chat helpers let you spin up translation swarms, code-review pairs, or tutoring agents in **5 minutes of code** – conversations live in `chat_history`, cost and revisions are automatic. | `ListParallel` explodes complex items into fine-grained subtasks, runs them **in parallel**, then auto-collects results – perfect for beat → scene → arc analysis or any long, messy document pipeline. |


---

### Loop snippet (Role‑Playing)

```python
Teacher = Character("teacher_name", TEACHER_PROMPT)
Student = Character("student_name", STUDENT_PROMPT)

g = ( ReadMarkdownLines("story.txt", "keyword")
      | SetField({"teacher_name":"Alice", "student_name":"Bob"})
      | Teacher("老师，请先讲解课文", 0)
      | Repeat( Student("同学提问或回答", 1)
                | Teacher("回应或继续讲解", 2), 3)
      | Teacher("请总结", 3)
      | ChatHistoryToText() | Print() )
```

### Spawn snippet (chapter → paragraph → synopsis with `ListParallel`)
```python
project = bf.CacheFolder("spawn_demo", 1, 0, 0)
broker  = bf.brokers.ConcurrentLLMCallBroker(project["cache/llm.jsonl"])

def ParaSummary():
    s = GenerateLLMRequest("Summarise:\n{paragraph}", model="gpt-4o-mini@openai")
    s |= ConcurrentLLMCall(project["cache/ps.jsonl"], broker)
    s |= ExtractResponseText()
    return s

g = ( ReadMarkdownLines("novel/*.md", "chapter")         # each entry = a chapter
      | ListParallel(ParaSummary(),                      # spawn per paragraph
                     in_lists_keys="paragraphs",
                     out_items_keys="paragraph",
                     in_items_keys="summary",
                     out_lists_keys="para_summaries")
      | GenerateLLMRequest("Chapter synopsis:\n{para_summaries}",
                           model="gpt-4o-mini@openai")
      | ConcurrentLLMCall(project["cache/ch_sum.jsonl"], broker) )
```

---

## Core concepts (one‑liner view)

| Term             | Story in one sentence                                                                                                                               |
| ---------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Entry**        | Tiny record with immutable `idx`, mutable `data`, auto‑incrementing `rev`.                                                                          |
| **Op**           | Atomic node; compose with `|`or explicit`wire()`.                                                                                                   |
| **GraphSegment** | A lightweight helper: `.to_segment()` just wraps a node so `|`and`wire()` work on it—each actual op still appears exactly once.                     |
| **execute()**    | High‑level driver that *resumes*, *pumps*, and *dispatches* brokers.                                                                                |
| **Broker**       | Pluggable engine handling expensive / async jobs (LLM, search, human labels).                                                                       |
| **Ledger**       | Append‑only JSONL cache behind every broker & graph enabling instant restart.                                                                       |

*(You *can* call `pump()` manually, but 99 % of users stick to `execute()`.)*

---

## Primitive index (short list)

| Family              | Node                                                           | Blurb                             |
| ------------------- | -------------------------------------------------------------- | --------------------------------- |
| **Sources**         | `ReadMarkdownLines`, `FromList`                                | ingest files or raw dicts         |
| **Transforms**      | `Apply`, `Filter`, `SetField`                                  | python‑powered field tweaks       |
| **Spawn / Collect** | `SpawnFromList`, `CollectAllToList`                            | map‑reduce with unique child ids  |
| **Control flow**    | `If`, `While`, `Repeat`                                        | branch, loop, iterate             |
| **LLM**             | `GenerateLLMRequest → ConcurrentLLMCall → ExtractResponseText` | prompt, call, harvest             |
| **Utilities**       | `CleanupLLMData`, `PrintTotalCost`                             | tidy temporary fields, audit cost |
| **Parallel helper** | `ListParallel` | one-liner spawn → subgraph → collect |
| **Spawn / Collect (low-level)** | `SpawnFromList`, `CollectAllToList` | manual map-reduce with unique child ids |

*(Shared‑idx ops `Replicate` / `Collect` are deprecated and vanish in v0.3.)*

---

## Example gallery

| ✨ Example                       | Demonstrates                                          |
| ------------------------------- | ----------------------------------------------------- |
| **01 – Basic pipeline**         | linear LLM transform & caching                        |
| **02 – Role‑playing loop**      | concise multi‑agent RPG using `Repeat` + chat helpers |
| **03 – Split & summarise**      | fan‑out/fan‑in summarisation (deprecated style)       |
| **04 – Long‑text segmentation** | Spawn + CollectAll power pattern                      |
| **05 – Math ops (unit)**        | loop + conditional logic under pure Python            |

---

## Broker & cache highlights

* Each expensive call is hashed → `job_idx` — **duplicate prompts are free**.
* `BrokerFailureBehavior = RETRY | STAY | EMIT` lets you decide how failures propagate.
* On restart, `execute()` reuses cached results and sends *only* the missing jobs.

---

## Roadmap → v0.3

* Enforce **unique `idx`** end‑to‑end → new `JoinByParent` replaces deprecated shared‑idx ops.
* Built‑in vector‑store & semantic‑search nodes.
* Streamlined cost & progress reporting.
* More batteries‑included tutorials.

---

© 2025 · MIT License
