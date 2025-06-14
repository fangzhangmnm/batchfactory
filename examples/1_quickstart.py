# START_EXAMPLE_EXPORT
import batchfactory as bf
from batchfactory.op import *

project = bf.ProjectFolder("quickstart", 1, 0, 2)
broker  = bf.brokers.ConcurrentLLMCallBroker(project["cache/llm_broker.jsonl"])

PROMPT = """
Write a poem about {keyword}.
"""

g = bf.Graph()
g |= ReadMarkdownLines("./demo_data/greek_mythology_stories.md").to_graph()
g |= Shuffle(42) | TakeFirstN(5)
g |= GenerateLLMRequest(PROMPT, model="gpt-4o-mini@openai")
g |= ConcurrentLLMCall(project["cache/llm_call.jsonl"],broker)
g |= ExtractResponseText()
g |= MapField(lambda headings,keyword: headings+[keyword], ["headings", "keyword"], "headings")
g |= WriteMarkdownEntries(project["out/poems.md"])

g.execute(dispatch_brokers=True)
# END_EXAMPLE_EXPORT