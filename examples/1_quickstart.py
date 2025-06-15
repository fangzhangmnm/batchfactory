# START_EXAMPLE_EXPORT
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
# END_EXAMPLE_EXPORT