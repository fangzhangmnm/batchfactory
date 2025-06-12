import batchfactory as bf
from batchfactory.op import *

def test_example3_split_and_summarize():
    project = bf.CacheFolder("example3_split_and_summarize", 1, 0, 0)
    # project.delete_all(warning=False)
    broker = bf.brokers.ConcurrentLLMCallBroker(project["cache/llm_broker.jsonl"])
    # model = "gpt-4o-mini@openai"
    model = "deepseek-v3-0324@lambda"

    def AskLLM(prompt, output_key, identifier):
        g = GenerateLLMRequest(prompt,model=model)
        g |= ConcurrentLLMCall(project[f"cache/llm_call_{identifier}.jsonl"], broker, failure_behavior="retry")
        g |= ExtractResponseText(output_key=output_key)
        g |= CleanupLLMData()
        return g
        
    g = ReadMarkdownLines("example_data.txt")
    g |= Shuffle(42)
    g |= TakeFirstN(1)
    split = Replicate(n_out_ports=3)
    g |= split
    g1 = AskLLM(
        """Discuss the general plot structure and main arcs of "{keyword}". """,
        "plot_summary",1)
    g2 = AskLLM(
        """Discuss the characterization of main characters in "{keyword}". """,
        "characterization_summary",2)
    g3 = AskLLM(
        """Discuss the world-building and setting in "{keyword}". """,
        "worldbuilding_summary",3)
    g |= g1
    g.wire(split, g2, 1, 0)
    g.wire(split, g3, 2, 0)
    c1 = Collect("characterization_summary")
    g |= c1
    g.wire(g2, c1, 0, 1)
    c2 = Collect("worldbuilding_summary")
    g |= c2
    g.wire(g3, c2, 0, 1)

    g |= AskLLM(
        """
        Write a Report by syhnthesizing the following information in Simple English:
        - Plot Summary: {plot_summary}
        - Characterization Summary: {characterization_summary}
        - Worldbuilding Summary: {worldbuilding_summary}
        """,
        "text",4)

    g |= PrintField()
    g |= WriteJsonl(project["out/summaries.jsonl"], output_keys=["keyword", "text"])
    g |= WriteMarkdownEntries(project["out/summaries.md"])

    g = g.compile()
    g.execute(dispatch_brokers=True, mock=False)

if __name__ == "__main__":
    test_example3_split_and_summarize()
    print("Example pipeline executed successfully.")