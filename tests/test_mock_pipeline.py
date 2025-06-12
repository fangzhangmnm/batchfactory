import batchfactory as bf
from batchfactory.op import *

import nest_asyncio; nest_asyncio.apply()  # For Jupyter and pytest compatibility

project = bf.CacheFolder("./tmp/test_mock_pipeline", 1, 0, 0)
test_data = [
    {"keyword": "test1", "text": "This is a test passage test1.", "directory": "dir1"},
    {"keyword": "test2", "text": "This is a test passage test2.", "directory": "dir2"},
    {"keyword": "test3", "text": "This is a test passage test3.", "directory": "dir3"},
]
test_data = list(sorted(test_data, key=lambda x: x["keyword"]))

def check_against_test_data(results):
    results = list(sorted(results, key=lambda x: x.data["keyword"]))
    assert len(results) == 3
    for entry, reference in zip(results, test_data):
        assert entry.data["keyword"] == reference["keyword"]
        assert entry.data["text"] == reference["text"]
        assert entry.data["directory"] == reference["directory"]

def test_llm_call():
    broker = bf.brokers.ConcurrentLLMCallBroker(project["cache/llm_broker.jsonl"])
    project.delete_all(warning=False)

    g = FromList(test_data)
    g |= GenerateLLMRequest(
            'Rewrite the passage from "{directory}" titled "{keyword}" as a four-line English poem.',
            model="gpt-4o-mini@openai",
        )
    g |= ConcurrentLLMCall(project["cache/llm_call1.jsonl"], broker)
    g |= ExtractResponseText()
    def check_dummy_response_and_restore(data):
        assert data["text"]
        data["text"] = f"This is a test passage {data['keyword']}."
    g |= Apply(check_dummy_response_and_restore)
    g = g.compile()
    results = g.execute(dispatch_brokers=True, mock=True)

    check_against_test_data(results)

    project.delete_all(warning=False)

def test_json():
    project.delete_all(warning=False)

    g = FromList(test_data)
    g |= WriteJsonl(project["cache/test_data.jsonl"])
    g = g.compile()
    g.execute(dispatch_brokers=False, mock=True)

    g = ReadJsonl(project["cache/test_data.jsonl"]).to_segment()
    g = g.compile()
    results = g.execute(dispatch_brokers=False, mock=True)

    check_against_test_data(results)

    project.delete_all(warning=False)

def test_markdown():
    project.delete_all(warning=False)

    g = FromList(test_data)
    g |= WriteMarkdownEntries(project["cache/test_data.md"])
    g = g.compile()
    g.execute(dispatch_brokers=False, mock=True)

    g = ReadMarkdownEntries(project["cache/test_data.md"],directory_mode='str').to_segment()
    g = g.compile()
    results = g.execute(dispatch_brokers=False, mock=True)

    check_against_test_data(results)

    project.delete_all(warning=False)

def test_rpg_loop():
    broker = bf.brokers.ConcurrentLLMCallBroker(project["cache/llm_broker.jsonl"])
    project.delete_all(warning=False)

    g = FromList(test_data)
    g |= SetField("teacher_name", "Teacher","student_name", "Student")
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
            seg |= UpdateChatHistory(character_key=character_key)
            seg |= ExtractResponseMeta() | CleanupLLMData()
            return seg
        return func
    
    Teacher = Character("teacher_name", "You are a teacher named {teacher_name}. Please only output dialogue.")
    Student = Character("student_name", "You are a student named {student_name}. Please only output dialogue.")

    g |= Teacher("Please introduce the text from {directory} titled {keyword}.", 0)
    g1 = Student("Please ask questions or respond.", 1)
    g1 |= Teacher("Please respond to the student or continue explaining.", 2)
    g |= Repeat(g1, 3)
    g |= Teacher("Please summarize.", 3)
    g |= ChatHistoryToText(template="**{role}**: {content}\n\n")

    g = g.compile()

    results = g.execute(dispatch_brokers=True, mock=True)
    assert len(results) == 3, f"Expected 3 entries, got {len(results)}"
    for entry, reference in zip(results, test_data):
        # how many time should Teacher appear?
        n_teacher_speaks = 1 + 3 + 1
        n_student_speaks = 3
        dialogue_text = entry.data["text"]
        assert dialogue_text.count("**Teacher**: ") == n_teacher_speaks, f"Expected Teacher to speak {n_teacher_speaks} times, got {dialogue_text.count('Teacher')}"
        assert dialogue_text.count("**Student**: ") == n_student_speaks, f"Expected Student to speak {n_student_speaks} times, got {dialogue_text.count('Student')}"

    project.delete_all(warning=False)


