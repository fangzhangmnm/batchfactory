import requests, os
import batchfactory as bf
from batchfactory.op import *

def download_book(url , filename):
    os.makedirs(os.path.dirname(filename), exist_ok=True)
    if os.path.exists(filename):
        with open(filename, "r", encoding="utf-8") as file:
            text = file.read()
            return text
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to download book. Status code: {response.status_code}")
    with open(filename, "w", encoding="utf-8") as file:
        file.write(response.text)
    print(f"Book downloaded and saved as '{filename}'")
    return response.text


def get_nonempty_lines(text):
    return [line for line in text.split('\n') if line.strip()]

def group_lines_by_max_length(lines:list, max_length=8192)->list[list]:
    groups = [[]]
    current_length = 0
    for line in lines:
        line_length = len(line)
        if current_length == 0 or current_length + line_length + 1 <= max_length:
            groups[-1].append(line)
            current_length += line_length + 1
        else:
            groups.append([line])
            current_length = line_length + 1
    return groups

def label_line_numbers(text):
    lines = get_nonempty_lines(text)
    return "\n".join(f"{i+1}: {line}" for i, line in enumerate(lines))

def segment_text(text, max_length=8192):
    lines = get_nonempty_lines(text)
    grouped_lines = group_lines_by_max_length(lines, max_length=max_length)
    text_segments = ["\n".join(group) for group in grouped_lines]
    return text_segments

def segment_text_by_starts(text, starts):
    lines = get_nonempty_lines(text)
    grouped_lines = [""]
    for i, line in enumerate(lines):
        if i in starts:
            if grouped_lines[-1]:
                grouped_lines.append("")
        grouped_lines[-1] += line + "\n\n"
    return grouped_lines

def collect_starts(starts_groups:list[list[int]], lines_each_group):
    global_starts=[]
    earlier_nlines = 0
    for starts, lines_this_group in zip(starts_groups, lines_each_group):
        global_starts.extend([start + earlier_nlines for start in starts])
        earlier_nlines += lines_this_group
    return sorted(set(global_starts))

def print_flagged_lines(text, starts):
    lines = get_nonempty_lines(text)
    flagged_lines = [lines[start] for start in starts if start < len(lines)]
    print("Flagged Lines:")
    for start, line in zip(starts, flagged_lines):
        print(f"{start + 1}: {line.strip()}")

label_seg_prompt = """
Please label the following text by different Scenes.

A Scene is a unit of story with a clear beginning, middle, and end, structured around conflict or change. It often contains multiple beats and actions.

A Scene should be around 400-800 words long.

I will provide you with a text where each line is labeled by a number.

Your task is to output the Line numbers that indicates the start of a scene, including chapter boundaries.

Note that the given text might start from the middle of a scene, so the first line might not be the start of a scene.

Please only output the line numbers, separated by space, without any additional text or formatting.
The text is as follows:

```
{text}
```

Please provide the line numbers of the start of each scene in the text above, separated by spaces, without any additional text or formatting.
Your Output:
"""

def test_example4_long_text_segmentation():
    project = bf.CacheFolder("example4_long_text_segmentation", 1, 0, 0)
    # project.delete_all()
    broker = bf.brokers.ConcurrentLLMCallBroker(project["cache/llm_broker.jsonl"])
    # model = "deepseek-v3-0324@lambda"
    model = "deepseek-r1-671b@lambda"

    def AskLLM(prompt, out_field, identifier):
        g = GenerateLLMRequest(prompt, model=model)
        g |= ConcurrentLLMCall(project[f"cache/llm_call_{identifier}.jsonl"], broker, failure_behavior="retry")
        g |= SplitCot()
        g |= ExtractResponseText(output_field=out_field)
        g |= CleanupLLMData()
        return g


    text = download_book(url = "https://www.gutenberg.org/files/11/11-0.txt", filename='./data/books/example_text.txt')
    # text = download_book(url = None, filename='./data/books/example_text_2.txt')
    g = FromList([{"text":text}])
    g |= Apply(segment_text, "text", "text_segments")


    g |= (n1 := SpawnFromList("text_segments", "text"))

    g1 = Apply(label_line_numbers, "text", "text")
    g1 |= AskLLM(label_seg_prompt, "starts", 1)
    g1 |= Apply(lambda x: list(int(i)-1 for i in x.split(" ")), "starts", "starts")  
    # g1 |= Apply(print_flagged_lines, ["text", "starts"], [])


    g.wire(n1,g1,1,0);

    g |= (n2 := CollectAllToList("starts", "starts"))
    g.wire(g1,n2,0,1)

    g |= Apply(lambda texts:[len(get_nonempty_lines(text)) for text in texts], "text_segments", "n_lines")
    g |= Apply(collect_starts, ["starts", "n_lines"], "starts")
    g |= Apply(print_flagged_lines, ["text", "starts"], [])
    g |= Apply(segment_text_by_starts, ["text", "starts"], "text_segments")
    g |= (n3 := ToList())

    g |= WriteJsonl(project["out/output.jsonl"])


    g = g.compile()
    g.execute(dispatch_brokers=True, mock=False)

    entries = n3.get_output()
    print("Example 4 pipeline executed successfully.")

    text_segments = entries[0].data['text_segments']

    out_path = project['./out/output.txt']
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    with open(out_path, 'w', encoding='utf-8') as f:
        for i, segment in enumerate(text_segments):
            segment = segment.replace("#","")
            title = segment.split('\n')[0].strip()
            f.write(f"# Segment {i+1} {title}\n{segment}\n\n")
    print(f"Text segments saved to {out_path}")
