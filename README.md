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

```python
import batchfactory as bf
from batchfactory.op import *

project = bf.ProjectFolder("quickstart", 1, 0, 5)
broker  = bf.brokers.LLMBroker(project["cache/llm_broker.jsonl"])

PROMPT = """
Write a poem about {keyword}.
"""

g = bf.Graph()
g |= ReadMarkdownLines("./demo_data/greek_mythology_stories.md")
g |= Shuffle(42) | TakeFirstN(5)
g |= GenerateLLMRequest(PROMPT, model="gpt-4o-mini@openai")
g |= CallLLM(project["cache/llm_call.jsonl"],broker)
g |= ExtractResponseText()
g |= MapField(lambda headings,keyword: headings+[keyword], ["headings", "keyword"], "headings")
g |= WriteMarkdownEntries(project["out/poems.md"])

g.execute(dispatch_brokers=True)
```

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

```python
g |= MapField(lambda x: split_text(label_line_numbers(x)), "text", "text_segments")
spawn_chain = AskLLM(LABEL_SEG_PROMPT, "labels", 1)
spawn_chain |= MapField(text_to_integer_list, "labels")
g | ListParallel(spawn_chain, "text_segments", "text", "labels", "labels")
g |= MapField(flatten_list, "labels")
g |= MapField(split_text_by_line_labels, ["text", "labels"], "text_segments")
g |= ExplodeList(["filename","text_segments"],["filename","text"])
```

---

### Loop snippet (Role‑Playing)

```python
Teacher = Character("teacher_name", "You are a teacher named {teacher_name}. "+FORMAT_REQ)
Student = Character("student_name", "You are a student named {student_name}. "+FORMAT_REQ)

g = bf.Graph()
g |= ReadMarkdownLines("./demo_data/greek_mythology_stories.md") | TakeFirstN(1)
g |= SetField("teacher_name", "Teacher","student_name", "Student")

g |= Teacher("Please introduce the text from {headings} titled {keyword}.", 0)
loop_body = Student("Please ask questions or respond.", 1)
loop_body |= Teacher("Please respond to the student or continue explaining.", 2)
g |= Repeat(loop_body, 3)
g |= Teacher("Please summarize.", 3)
g |= ChatHistoryToText(template="**{role}**: {content}\n\n")
g |= MapField(lambda headings,keyword: headings+[keyword], ["headings", "keyword"], "headings")
g |= WriteMarkdownEntries(project["out/roleplay.md"])
```

---

### Text Embedding snippet

```python
embedding_broker  = bf.brokers.LLMEmbeddingBroker(project["cache/embedding_broker.jsonl"])
g |= GenerateLLMEmbeddingRequest("keyword", model="text-embedding-3-small@openai")
g |= CallLLMEmbedding(project["cache/embedding_call.jsonl"], embedding_broker)
g |= ExtractResponseEmbedding()
g |= DecodeBase64Embedding()
```

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

| Operation | Description |
|-----------|-------------|
| `Apply` | Apply a function to modify the entry data. |
| `BeginIf` | Switch to port 1 if criteria is met. See `If` function for usage. |
| `CallLLM` | Dispatch concurrent API calls for LLM — may induce API billing from external providers. |
| `CallLLMEmbedding` | Dispatch concurrent API calls for embedding models — may induce API billing from external providers. |
| `ChatHistoryToText` | Format the chat history into a single text. |
| `CheckPoint` | A no-op checkpoint that saves inputs to the cache, and resumes from the cache. |
| `CleanupLLMData` | Clean up internal fields for LLM processing, such as `llm_request`, `llm_response`, `status`, and `job_idx`. |
| `CleanupLLMEmbeddingData` | Clean up the internal fields for LLM processing, such as `embedding_request`, `embedding_response`, `status`, `job_idx`. |
| `Collect` | Collect data from port 1, merge to 0. |
| `CollectAllToList` | Collect items from spawn entries on port 1 and merge them into a list (or lists if multiple items provided). |
| `DecodeBase64Embedding` | Decode the base64 encoded embedding into python array. |
| `EndIf` | Join entries from either port 0 or port 1. See `If` function for usage. |
| `ExplodeList` | Explode an entry to multiple entries based on a list (or lists). |
| `ExtractResponseEmbedding` | Extract the embedding object (base64 encoded numpy array) from the LLM response and store it to entry data. |
| `ExtractResponseText` | Extract the text content from the LLM response and store it to entry data. |
| `Filter` | Filter entries based on a custom criteria function. |
| `FilterFailedEntries` | Drop entries that have a status "failed". |
| `FilterMissingFields` | Drop entries that do not have specific fields. |
| `FromList` | Create entries from a list of dictionaries or objects, each representing an entry. |
| `GenerateLLMEmbeddingRequest` | Generate LLM embedding requests from input_key. |
| `GenerateLLMRequest` | Generate LLM requests from a given prompt, formatting it with the entry data. |
| `If` | Switch to true_chain if criteria is met, otherwise stay on false_chain. |
| `ListParallel` | Spawn entries from a list (or lists), process them in parallel, and collect them back to a list (or lists). |
| `MapField` | Map a function to specific field(s) in the entry data. |
| `PrintEntry` | Print the first n entries information. |
| `PrintField` | Print the specific field(s) from the first n entries. |
| `PrintTotalCost` | Print the total accumulated API cost for the output batch. |
| `ReadJsonl` | Read JSON Lines files. (also supports json array) |
| `ReadMarkdownEntries` | Read Markdown files and extract nonempty text under every headings with markdown headings as a list. |
| `ReadMarkdownLines` | Read Markdown files and extract non-empty lines as keyword with markdown headings as a list. |


| Operation | Description |
|-----------|-------------|
| `ReadTxtFolder` | Collect all txt files in a folder. |
| `RemoveField` | Remove fields from the entry data. |
| `RenameField` | Rename fields in the entry data. |
| `Repeat` | Repeat the loop body for a fixed number of rounds. |
| `RepeatNode` | Repeat the loop body for a fixed number of rounds. See `Repeat` function for usage. |
| `Replicate` | Replicate an entry to all output ports. |
| `SetField` | Set fields in the entry data to specific values. |
| `Shuffle` | Shuffle the entries in a batch randomly. |
| `Sort` | Sort the entries in a batch |
| `SortMarkdownEntries` | Sort Markdown entries based on headings and (optional) keyword. |
| `SpawnFromList` | Spawn multiple spawn entries to port 1 based on a list (or lists). |
| `TakeFirstN` | Takes the first N entries from the batch. discards the rest. |
| `ToList` | Output a list of specific field(s) from entries. |
| `TransformCharacterDialogueForLLM` | Map custom character roles to valid LLM roles (user/assistant/system). Must be called after GenerateLLMRequest. |
| `UpdateChatHistory` | Appending the LLM response to the chat history. |
| `While` | Executes the loop body while the criteria is met. |
| `WhileNode` | Executes the loop body while the criteria is met. See `While` function for usage. |
| `WriteJsonl` | Write entries to a JSON Lines file. |
| `WriteMarkdownEntries` | Write entries to Markdown file(s), with heading hierarchy defined by headings and text as content. |
| `WriteMarkdownLines` | Write keyword lists to Markdown file(s) as lines, with heading hierarchy defined by headings:list. |
| `remove_cot` | Remove the chain of thought (CoT) from the LLM response. Use MapField to wrap it. |
| `remove_speaker_tag` | Remove speaker tags. Use MapField to wrap it. |
| `split_cot` | Split the LLM response into text and chain of thought (CoT). Use MapField to wrap it. |

---

© 2025 · MIT License
