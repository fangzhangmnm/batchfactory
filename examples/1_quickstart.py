# START_EXAMPLE_EXPORT
import batchfactory as bf
from batchfactory.op import *

PROMPT = """
Write a poem about {keyword}.
"""

with bf.ProjectFolder("quickstart", 1, 0, 6) as project:
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
# END_EXAMPLE_EXPORT