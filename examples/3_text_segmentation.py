import batchfactory as bf
from batchfactory.op import *
import batchfactory.op.functional as F
from batchfactory.lib.utils import download_if_missing

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

model = "gpt-4o-mini@openai" # for demo only, need a better model for this task

def create_input_chunks(text):
    lines = F.lines(text)
    lines = F.label_line_numbers(lines)
    chunks = F.chunk_lines(lines, chunk_length=8192)
    return [F.join_lines(chunk) for chunk in chunks]

def post_processing(text, labels):
    lines = F.lines(text)
    labels = F.flatten_list(labels)
    chunks = F.split_lines(lines, labels)
    return [F.join_lines(chunk,separator="\n\n") for chunk in chunks], labels


with bf.ProjectFolder("text_segmentation", 1, 0, 5) as project:

    g = ReadTxtFolder(project["in/books"])

    # START_EXAMPLE_EXPORT
    g |= MapField(create_input_chunks, "text", "text_segments")
    spawn_chain = AskLLM(LABEL_SEG_PROMPT, model=model, output_key="labels")
    spawn_chain |= MapField(F.text_to_integer_list, "labels")
    g | ListParallel(spawn_chain, "text_segments", "text", "labels", "labels")
    g |= MapField(post_processing, ["text", "labels"], ["text_segments", "labels"])
    g |= ExplodeList(["text_segments"],["text"])
    # END_EXAMPLE_EXPORT

    g |= MapField(lambda filename, list_idx: [filename, f"Chapter {list_idx+1}"], ["filename", "list_idx"], "headings")
    g |= WriteMarkdownEntries(project["out/chapterized"],filename_key="filename")

def download_alice(path="./data/examples/alice.txt"):
    url = "https://www.gutenberg.org/files/11/11-0.txt"
    return download_if_missing(url, path, binary=False)

download_alice(project["in/books/Alice in Wonderland.txt"])
g.execute(dispatch_brokers=True)