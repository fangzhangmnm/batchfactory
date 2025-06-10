import batchfactory as bf
from batchfactory.op import *

def test_example2_role_playing_loop():
    project = bf.CacheFolder("example2_role_playing_loop", 1, 0, 0)
    broker = bf.brokers.ConcurrentLLMCallBroker(project["cache/llm_broker.jsonl"])

    TEACHER_PROMPT = """你是一名名叫{teacher_name}小学老师，你要讲解一篇出自"{directory}"的名叫"{keyword}"的课文。请只输出对话。"""
    STUDENT_PROMPT = """你是一名名叫{student_name}的小学生。请只输出对话。"""

    def Character(character_field, user_prompt):
        def func(command,identifier):
            seg = GenerateLLMRequest(
                user_prompt = user_prompt,
                model="gpt-4o-mini@openai",
                chat_history_field=True,
                after_prompt=command,
            )
            seg |= TransformCharacterDialogueForLLM(character_field=character_field)
            seg |= ConcurrentLLMCall(project[f"cache/llm_call_{identifier}.jsonl"], broker, failure_behavior="retry")
            seg |= UpdateChatHistory(character_field=character_field)
            seg |= ExtractResponseMeta() | CleanupLLMData()
            return seg
        return func
    
    Teacher = Character("teacher_name", TEACHER_PROMPT)
    Student = Character("student_name", STUDENT_PROMPT)

    g = ReadMarkdownLines("example_data.txt", "keyword", directory_str_field="directory")
    g |= Shuffle(42) | TakeFirstN(3)
    g |= SetField({"teacher_name": "芭芭拉", "student_name": "莉莉"})
    g |= Teacher("请老师先介绍一下课文的内容。", 0)
    g1 = Student("请扮演学生提问或者回答问题。", 1)
    g1 |= Teacher("请回应学生或者继续讲解。", 2)
    g |= Repeat(g1, 3)
    g |= Teacher("请进行总结。", 3)
    g |= ChatHistoryToText() | RemoveField("chat_history")
    g |= WriteJsonl(project["out/role_playing_loop.jsonl"], output_fields=["keyword", "text", "directory","model"])
    g |= PrintText()

    g = g.compile()
    g.execute(dispatch_brokers=True, mock=False)

    print(g)

if __name__ == "__main__":
    test_example2_role_playing_loop()
    print("Example pipeline executed successfully.")