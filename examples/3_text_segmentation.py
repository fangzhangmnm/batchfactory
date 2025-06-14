import batchfactory as bf
from batchfactory.op import *
import itertools as itt

def lines(text):
    return [line.strip() for line in text.split('\n') if line.strip()]

def split_text(text, max_length=8192)->list[str]:
    groups = [""]
    for line in lines(text):
        if groups[-1] == "" or (len(groups[-1]) + len(line) + 1 <= max_length):
            groups[-1] += (line + "\n")
        else:
            groups.append(line + "\n")
    return groups

def label_line_numbers(text,offset=1):
    return "\n".join(f"{i+offset}: {line}" for i, line in enumerate(lines(text)))

def split_text_by_line_labels(text, line_labels, offset=1):
    groups = [""]
    for i, line in enumerate(lines(text)):
        if i+offset in line_labels and groups[-1] != "":
            groups.append("")
        groups[-1] += line + "\n\n"
    return groups

def flatten_list(lst):
    return list(itt.chain.from_iterable(lst))

def print_labeled_lines(text, line_labels, offset=1):
    flagged_lines = [line for i, line in enumerate(lines(text)) if i + offset in line_labels]
    for i, line in enumerate(flagged_lines):
        print(f"{i + offset}: {line.strip()}")

def text_to_integer_list(text):
    try:
        return [int(i) for i in text.split()]
    except ValueError:
        return []

LABEL_SEG_PROMPT = """
Please label the following text by identifying different Scenes.

A Scene is a unit of story with a clear beginning, middle, and end, structured around conflict or change. It often contains multiple beats and actions.

A Scene should be approximately 400–800 words long. Try to divide a chapter into multiple scenes.

I will provide you with a text in which each line is labeled with a number.

Your task is to output the line numbers that indicate the start of each scene, including chapter boundaries.

Note that the given text may begin in the middle of a scene, so the first line might not mark the start of a new scene.

Please output only the line numbers, separated by spaces, with no additional text or formatting.

The text is as follows:

```
{text}
```

Please provide the line numbers marking the start of each scene in the text above, separated by spaces, with no additional text or formatting.  
Your Output:
"""

project = bf.ProjectFolder("text_segmentation", 1, 0, 2)
broker  = bf.brokers.ConcurrentLLMCallBroker(project["cache/llm_broker.jsonl"])
model = "o3-mini-2025-01-31@openai"

def AskLLM(prompt, output_key, identifier):
    g = GenerateLLMRequest(prompt, model=model)
    g |= ConcurrentLLMCall(project[f"cache/llm_call_{identifier}.jsonl"], broker, failure_behavior="retry")
    g |= ExtractResponseText(output_key=output_key)
    g |= MapField(remove_cot, "text")
    g |= CleanupLLMData()
    return g

g = ReadTxtFolder(project["in/books"])
g |= MapField(lambda x: x.split('.')[0], "filename", "directory")

# START_EXAMPLE_EXPORT
g |= MapField(lambda x: split_text(label_line_numbers(x)), "text", "text_segments")
spawn_chain = AskLLM(LABEL_SEG_PROMPT, "labels", 1)
spawn_chain |= MapField(text_to_integer_list, "labels")
g | ListParallel(spawn_chain, "text_segments", "text", "labels", "labels")
g |= MapField(flatten_list, "labels")
g |= MapField(split_text_by_line_labels, ["text", "labels"], "text_segments")
g |= ExplodeList(["directory", "text_segments"], ["directory", "text"])
# END_EXAMPLE_EXPORT

g |= RenameField("list_idx", "keyword")
g |= MapField(lambda x: f"Chapter {x+1}", "keyword")
g |= WriteMarkdownEntries(project["out/chapterized.md"], "text")




def get_alice(path):
    import os, requests
    if not os.path.exists(path):
        r = requests.get("https://www.gutenberg.org/files/11/11-0.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write(r.text)


get_alice(project["in/books/Alice in Wonderland.txt"])
g.execute(dispatch_brokers=True)