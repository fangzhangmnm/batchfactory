import batchfactory as bf

project = bf.CacheFolder("example1", 1, 0, 0)
broker = bf.brokers.ConcurrentLLMCallBroker(project("cache/llm_broker.jsonl"))

# Build a small graph that rewrites passages into short English poems

g = (
    bf.ReadMarkdownLinesOp("./data/*.txt", "keyword", directory_str_field="directory")
    | bf.ShuffleOp(42)
    | bf.TakeFirstNOp(3)
    | bf.GenerateLLMRequestOp(
        'Rewrite the passage from "{directory}" titled "{keyword}" as a four-line English poem.',
        model="gpt-4o-mini@openai",
    )
    | bf.ConcurrentLLMCallOp(project("cache/llm_call1.jsonl"), broker)
    | bf.ExtractResponseTextOp()
    | bf.SaveJsonlOp(project("out/poems.jsonl"),output_fields=["keyword","text","directory"])
    | bf.PrintTextOp()
)

g = g.compile()
g.resume()
g.pump(dispatch_broker=True, reset_input=True)