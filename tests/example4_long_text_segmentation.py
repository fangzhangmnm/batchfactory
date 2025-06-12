import requests, os
import batchfactory as bf
from batchfactory.op import *
from batchfactory import hash_text
from itertools import chain

    
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
    return list(chain.from_iterable(lst))

def print_labeled_lines(text, line_labels, offset=1):
    flagged_lines = [line for i, line in enumerate(lines(text)) if i + offset in line_labels]
    for i, line in enumerate(flagged_lines):
        print(f"{i + offset}: {line.strip()}")

def get_int_list(text):
    try:
        return [int(i) for i in text.split()]
    except ValueError:
        return []

label_seg_prompt = """
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

def test_example4_long_text_segmentation():
    project = bf.CacheFolder("example4_long_text_segmentation", 1, 0, 1)
    project.delete_all()
    broker = bf.brokers.ConcurrentLLMCallBroker(project["cache/llm_broker.jsonl"])
    model = "deepseek-v3-0324@lambda"

    def AskLLM(prompt, output_key, identifier):
        g = GenerateLLMRequest(prompt, model=model)
        g |= ConcurrentLLMCall(project[f"cache/llm_call_{identifier}.jsonl"], broker, failure_behavior="retry")
        g |= SplitCot()
        g |= ExtractResponseText(output_key=output_key)
        g |= CleanupLLMData()
        return g

    g = ReadTxtFolder("./data/books/*.txt")
    g |= Apply(lambda x:x.split('.')[0], "filename", "directory")
    g |= Apply(lambda x: split_text(label_line_numbers(x)), "text", "text_segments")
    g1 = AskLLM(label_seg_prompt, "labels", 1)
    g1 |= PrintField("labels")
    g1 |= Apply(get_int_list, "labels", "labels")
    g1 |= PrintField("labels")
    g | ListParallel(g1,"text_segments","text","labels","labels")
    g |= Apply(flatten_list, "labels", "labels")
    g |= Apply(split_text_by_line_labels, ["text", "labels"], "text_segments")
    g |= PrintField("labels")

    g |= ExplodeList(["directory","text_segments"],["directory","text"])
    g |= RenameField("list_idx","keyword")


    g |= AskLLM("请用中文简单的概括下下面一段小说的情节大纲：\n\n```\n{text}\n```只要输出情节大纲。\n情节大纲：","outline", 2)
    g |= PrintField("outline")

    g |= WriteMarkdownEntries(project["out/chapterized.md"],context_key="text")
    g |= WriteMarkdownEntries(project["out/chapter_outline.md"],context_key="outline")
    g |= WriteJsonl(project["out/chapterized.jsonl"],["directory","keyword","outline"])

    g = g.compile()
    g.execute(dispatch_brokers=True, mock=False)

    print("Example 4 pipeline executed successfully.")

if __name__ == "__main__":
    test_example4_long_text_segmentation()
    print("Example 4 long text segmentation executed successfully.")