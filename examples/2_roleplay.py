import batchfactory as bf
from batchfactory.op import *

project = bf.CacheFolder("roleplay", 1, 0, 2)
broker  = bf.brokers.ConcurrentLLMCallBroker(project["cache/llm_broker.jsonl"])

def Character(character_key, user_prompt):
    def func(command,identifier):
        seg = GenerateLLMRequest(
            user_prompt = user_prompt,
            model="gpt-4o-mini@openai",
            chat_history_key=True,
            after_prompt=command,
        )
        seg |= TransformCharacterDialogueForLLM(character_key=character_key)
        seg |= ConcurrentLLMCall(project[f"cache/llm_call_{identifier}.jsonl"], broker, failure_behavior="retry")
        seg |= ExtractResponseText()
        seg |= Apply(remove_speaker_tag, "text", "text")
        seg |= UpdateChatHistory(character_key=character_key)
        seg |= ExtractResponseMeta() | CleanupLLMData()
        return seg
    return func

FORMAT_REQ = "Please only output the dialogue."

# START_EXAMPLE_EXPORT
Teacher = Character("teacher_name", "You are a teacher named {teacher_name}. "+FORMAT_REQ)
Student = Character("student_name", "You are a student named {student_name}. "+FORMAT_REQ)

g = bf.Graph()
g |= ReadMarkdownLines("./demo_data/greek_mythology_stories.md") | TakeFirstN(1)
g |= SetField("teacher_name", "Teacher","student_name", "Student")

g |= Teacher("Please introduce the text from {directory} titled {keyword}.", 0)
loop_body = Student("Please ask questions or respond.", 1)
loop_body |= Teacher("Please respond to the student or continue explaining.", 2)
g |= Repeat(loop_body, 3)
g |= Teacher("Please summarize.", 3)
g |= ChatHistoryToText(template="**{role}**: {content}\n\n")
g |= WriteMarkdownEntries(project["out/roleplay.md"])
# END_EXAMPLE_EXPORT

g.execute(dispatch_brokers=True)