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

PROMPT = """
Write a poem about {keyword}.
"""

with bf.ProjectFolder("quickstart", 1, 0, 5) as project:
    g = bf.Graph()
    g |= ReadMarkdownLines("./demo_data/greek_mythology_stories.md")
        # load keywords
    g |= Shuffle(seed=42) | TakeFirstN(5)
        # random sample
    g |= AskLLM(PROMPT, model="gpt-4o-mini@openai")
        # generate poems
    g |= MapField(lambda headings, keyword: headings + [keyword], ["headings", "keyword"], "headings")
        # tag heading
    g |= WriteMarkdownEntries(project["out/poems.md"])
        # save results

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

## Core concepts (one‑liner view)


| Term          | Story in one sentence                                                                                                                              |               |
| ------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- | ------------- |
| **Entry**     | Tiny record with immutable `idx`, mutable `data`, auto‑incrementing `rev`.                                                                         |               |
| **Op**        | Atomic node; compose with `\|`or`wire()`. |
| **Graph**     | A chain of `Op`s wired together — supports flexible pipelines and subgraphs.                                                                       |               |
| **Executor**  | Internal engine that tracks graph state, manages batching, resumption, and broker dispatch. Created automatically when you call `graph.execute()`. |               |
| **Broker**    | Pluggable engine for expensive or async jobs (LLM APIs, search, human labelers).                                                                   |               |
| **Ledger**    | Append‑only JSONL backing each broker & graph — enables instant resume and transparent caching.                                                    |               |
| **execute()** | High-level command that runs the graph: creates an `Executor`, resumes from cache, and dispatches brokers as needed.                               |               |

---

### Spawn snippet (Text Segmentation)

```python
g |= MapField(create_input_chunks, "text", "text_segments")
    spawn_chain = AskLLM(LABEL_SEG_PROMPT, model=model, output_key="labels")
    spawn_chain |= MapField(F.text_to_integer_list, "labels")
    g | ListParallel(spawn_chain, "text_segments", "text", "labels", "labels")
    g |= MapField(post_processing, ["text", "labels"], ["text_segments", "labels"])
    g |= ExplodeList(["text_segments"],["text"])
```

---

### Loop snippet (Role‑Playing)

```python
###### Create the characters and their settings ######
    Teacher = AICharacter("Teacher", "You are a teacher. "+FORMAT_REQ, model=model)
    Student = AICharacter("Student", "You are a student. "+FORMAT_REQ, model=model)

    ###### Introduction of the Role Playing Session ######
    g |= Teacher("Please introduce the text from {headings} titled {keyword}.")
    ###### Main Role Playing Loop ######
    loop_body = Student("Please ask questions or respond.")
    loop_body |= Teacher("Please respond to the student or continue explaining.")
    g |= Repeat(loop_body, 3)
    ###### Wrap Up ######
    g |= Teacher("Please summarize.")
    g |= ChatHistoryToText(template="**{role}**: {content}\n\n")

    ###### Export the Role Playing Session ######
    g |= MapField(lambda headings,keyword: headings+[keyword], ["headings", "keyword"], "headings")
    g |= WriteMarkdownEntries(project["out/roleplay.md"])
```

---

### Text Embedding snippet

```python
g |= EmbedText("keyword", model="text-embedding-3-small@openai", output_format="list")
```

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
| **`AICharacter`** | Create a callable AI-character that yields a dialogue subgraph. |
| **`AskLLM`** | Ask the LLM with a given prompt and model, returning the response text. |
| **`EmbedText`** | Get the embedding vector for the input text. |
| `Apply` | Apply a function to modify the entry data. |
| `CheckPoint` | A no-op checkpoint that saves inputs to the cache, and resumes from the cache. |
| `CollectAllToList` | Collect items from spawn entries on port 1 and merge them into a list (or lists if multiple items provided). |
| `CollectField` | Collect field(s) from port 1, merge to 0. |
| `ExplodeList` | Explode an entry to multiple entries based on a list (or lists). |
| `Filter` | Filter entries based on a custom criteria function. |
| `FilterFailedEntries` | Drop entries that have a status "failed". |
| `FilterMissingFields` | Drop entries that do not have specific fields. |
| `FromList` | Create entries from a list of dictionaries or objects, each representing an entry. |
| `If` | Switch to true_chain if criteria is met, otherwise stay on false_chain. |
| `ListParallel` | Spawn entries from a list (or lists), process them in parallel, and collect them back to a list (or lists). |
| `MapField` | Map a function to specific field(s) in the entry data. |
| `PrintEntry` | Print the first n entries information. |
| `PrintField` | Print the specific field(s) from the first n entries. |
| `ReadJsonl` | Read JSON Lines files. (also supports json array) |
| `ReadMarkdownEntries` | Read Markdown files and extract nonempty text under every headings with markdown headings as a list. |
| `ReadMarkdownLines` | Read Markdown files and extract non-empty lines as keyword with markdown headings as a list. |
| `ReadTxtFolder` | Collect all txt files in a folder. |
| `RemoveField` | Remove fields from the entry data. |
| `RenameField` | Rename fields in the entry data. |
| `Repeat` | Repeat the loop body for a fixed number of rounds. |
| `Replicate` | Replicate an entry to all output ports. |
| `SetField` | Set fields in the entry data to specific values. |
| `Shuffle` | Shuffle the entries in a batch randomly. |
| `Sort` | Sort the entries in a batch |
| `SortMarkdownEntries` | Sort Markdown entries based on headings and (optional) keyword. |
| `SpawnFromList` | Spawn multiple spawn entries to port 1 based on a list (or lists). |
| `TakeFirstN` | Takes the first N entries from the batch. discards the rest. |
| `ToList` | Output a list of specific field(s) from entries. |
| `While` | Executes the loop body while the criteria is met. |
| `WriteJsonl` | Write entries to a JSON Lines file. |
| `WriteMarkdownEntries` | Write entries to Markdown file(s), with heading hierarchy defined by headings and text as content. |
| `WriteMarkdownLines` | Write keyword lists to Markdown file(s) as lines, with heading hierarchy defined by headings:list. |
| `WriteTxtFolder` | Write entries to a folder as txt files. |

---

© 2025 · MIT License
