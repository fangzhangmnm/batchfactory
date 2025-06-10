import batchfactory as bf
from batchfactory.op import *

def test_example1_basic_pipeline():
    project = bf.CacheFolder("example1_basic_pipeline", 1, 0, 0)
    broker = bf.brokers.ConcurrentLLMCallBroker(project["cache/llm_broker.jsonl"])

    # Build a small graph that rewrites passages into short English poems

    g = (
        ReadMarkdownLines("example_data.txt", "keyword", directory_str_field="directory")
        | Shuffle(42)
        | TakeFirstN(3)
        | GenerateLLMRequest(
            'Rewrite the passage from "{directory}" titled "{keyword}" as a four-line English poem.',
            model="gpt-4o-mini@openai",
        )
        | ConcurrentLLMCall(project["cache/llm_call1.jsonl"], broker)
        | ExtractResponseText()
        | WriteJsonl(project["out/poems.jsonl"],output_fields=["keyword","text","directory"])
        | PrintText()
    )

    g = g.compile()
    g.execute(dispatch_brokers=True, mock=False)

if __name__ == "__main__":
    test_example1_basic_pipeline()
    print("Example pipeline executed successfully.")