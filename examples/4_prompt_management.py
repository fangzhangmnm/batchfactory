import batchfactory as bf
from batchfactory.op import *

class MyPromptMaker(bf.PromptMaker):
    ENGLISH_PROMPT = """
    Please tell a story about {keyword} in English, with a length of {word_count} words.
    """
    CHINESE_PROMPT = """
    请用中文讲述关于{keyword}的故事，长度为{chinese_character_count}字。
    """
    def __init__(self, word_count):
        self.word_count = word_count

    def make_prompt(self, data: dict) -> str:
        if data["lang"] == "en":
            return self.ENGLISH_PROMPT.format(
                keyword=data["keyword"],
                word_count=self.word_count
            )
        elif data["lang"] == "zh":
            return self.CHINESE_PROMPT.format(
                keyword=data["keyword"],
                chinese_character_count=int(self.word_count / 1.75)
            )

project = bf.ProjectFolder("prompt_management", 1, 0, 3)
broker  = bf.brokers.ConcurrentLLMCallBroker(project["cache/llm_broker.jsonl"])
model = "gpt-4o-mini@openai"


def AskLLM(prompt, output_key, identifier):
    g = GenerateLLMRequest(prompt, model=model)
    g |= ConcurrentLLMCall(project[f"cache/llm_call_{identifier}.jsonl"], broker, failure_behavior="retry")
    g |= ExtractResponseText(output_key=output_key)
    g |= MapField(remove_cot, output_key)
    g |= CleanupLLMData()
    return g

g = bf.Graph()

g |= ReadMarkdownLines("./demo_data/greek_mythology_stories.md").to_graph()
g |= SetField("langs", ["en", "zh"])
g |= ExplodeList("langs","lang")
g |= Shuffle(42) | TakeFirstN(5)
g |= AskLLM(MyPromptMaker(100),"text","tell_story")
g |= MapField(lambda headings,keyword,lang: headings+[keyword+" ("+lang+")"],
              ["headings", "keyword", "lang"], "headings")
g |= WriteMarkdownEntries(project["out/stories_bilingual.md"])

g.execute(dispatch_brokers=True)



        
        
