# START_EXAMPLE_EXPORT
import batchfactory as bf
from batchfactory.op import *

project = bf.CacheFolder("quickstart", 1, 0, 0)
broker  = bf.brokers.ConcurrentLLMCallBroker(project["cache/llm_broker.jsonl"])

PROMPT = """
Write a poem about {keyword}.
"""

g = bf.Graph()
g |= ReadMarkdownLines("./demo_data/greek_mythology_stories.md")
g |= Shuffle(42) | TakeFirstN(5)
g |= GenerateLLMRequest(PROMPT, model="gpt-4o-mini@openai")
g |= ConcurrentLLMCall(project["cache/llm_call.jsonl"],broker)
g |= ExtractResponseText()
g |= WriteMarkdownEntries(project["out/poems.md"])

g.execute(dispatch_brokers=True)
# END_EXAMPLE_EXPORT