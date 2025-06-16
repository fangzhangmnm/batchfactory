import batchfactory as bf
from batchfactory.op import *
import batchfactory.op.functional as F

class MyPromptMaker(bf.PromptMaker):
    ENGLISH_PROMPT = """
    Please tell a story about {keyword} in English, with a length of {word_count} words.
    """
    CHINESE_PROMPT = """
    请用中文讲述关于{keyword}的故事，长度为{chinese_character_count}字。
    """
    def __init__(self, *, word_count):
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

model = "gpt-4o-mini@openai"

with bf.ProjectFolder("prompt_management", 1, 0, 5) as project:
    g = ReadMarkdownLines("./demo_data/greek_mythology_stories.md")
    g |= SetField("langs", ["en", "zh"])
    g |= ExplodeList("langs","lang")
    g |= Shuffle(seed=42) | TakeFirstN(5)
    g |= AskLLM(MyPromptMaker(word_count=100),model=model)
    g |= MapField(lambda headings,keyword,lang: headings+[keyword+" ("+lang+")"],
              ["headings", "keyword", "lang"], "headings")
    g |= WriteMarkdownEntries(project["out/stories_bilingual.md"])

g.execute(dispatch_brokers=True)



        
        
