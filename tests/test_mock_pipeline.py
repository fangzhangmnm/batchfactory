import batchfactory as bf
from batchfactory.op import *
import numpy as np

import nest_asyncio; nest_asyncio.apply()  # For Jupyter and pytest compatibility

def compare(results, reference, sort_key):
    results = list(sorted(results, key=lambda x: x.data[sort_key]))
    reference = list(sorted(reference, key=lambda x: x[sort_key]))
    assert len(results) == len(reference), f"Expected {len(reference)} entries, got {len(results)}"
    for entry, ref in zip(results, reference):
        for key in ref:
            assert entry.data[key] == ref[key], f"Expected {key} to be {repr(ref[key])}, got {repr(entry.data[key])}"


def test_llm_call(tmp_path):
    project = bf.ProjectFolder("test_llm_call", 1, 0, 0, data_dir=tmp_path)
    broker = bf.brokers.LLMBroker(project["cache/llm_broker.jsonl"])

    test_data = [
        {"keyword": "test1", "text": "This is a test passage test1.", "headings": ["Test 1"]},
        {"keyword": "test2", "text": "This is a test passage test2.", "headings": ["Test 2"]},
        {"keyword": "test3", "text": "This is a test passage test3.", "headings": ["Test 3"]},
    ]

    g = bf.Graph()
    g |= FromList(test_data)
    g |= GenerateLLMRequest(
            'Rewrite the passage from "{headings}" titled "{keyword}" as a four-line English poem.',
            model="gpt-4o-mini@openai",
        )
    g |= CallLLM(project["cache/llm_call1.jsonl"], broker)
    g |= ExtractResponseText()
    def restore_text_from_dummy_response(data):
        assert data["text"]
        data["text"] = f"This is a test passage {data['keyword']}."
    g |= Apply(restore_text_from_dummy_response)
    results = g.execute(dispatch_brokers=True, mock=True)
    print(g)

    compare(results, test_data, "keyword")

def test_embedding_call(tmp_path):
    project = bf.ProjectFolder("test_embedding_call", 1, 0, 0, data_dir=tmp_path)
    embedding_broker = bf.brokers.LLMEmbeddingBroker(project["cache/llm_embedding_broker.jsonl"])

    test_data = [
        {"keyword": "test1", "text": "Peter ate an apple."},
        {"keyword": "test2", "text": "Large language model is a type of AI."},
        {"keyword": "test3", "text": "Hey, how are you doing today?"},
    ]
    g = bf.Graph()
    g |= FromList(test_data)
    g |= GenerateLLMEmbeddingRequest("text", model="text-embedding-3-small@openai")
    g |= CallLLMEmbedding(project["cache/llm_embedding_call1.jsonl"], embedding_broker)
    g |= ExtractResponseEmbedding()
    def check_embedding(data,dim=1536):
        assert data["embedding"]
        embedding_vector:np.ndarray = bf.base64_utils.decode_ndarray(data["embedding"])
        assert isinstance(embedding_vector, np.ndarray)
        assert embedding_vector.shape == (dim,), f"Expected embedding shape {(dim,)}, got {embedding_vector.shape}"
    g |= Apply(check_embedding)
    g |= DecodeBase64Embedding("embedding")
    def check_embedding_2(data,dim=1536):
        assert "embedding" in data, "Expected 'embedding' key in data"
        assert isinstance(data["embedding"], list)
        assert isinstance(data["embedding"][0], float)
        assert len(data["embedding"]) == dim, f"Expected embedding length {dim}, got {len(data['embedding'])}"
    g |= Apply(check_embedding_2)
    results = g.execute(dispatch_brokers=True, mock=True)
    print(g)







def test_json(tmp_path):
    project = bf.ProjectFolder("test_json", 1, 0, 0, data_dir=tmp_path)

    test_data = [
        {"keyword": "test1", "text": "This is a test passage test1.", "list": [1, 2, 3], "bool": True, "float": 3.14, "int": 42},
        {"keyword": "test2", "text": "This is a test passage test2.", "list": [4, 5, 6], "bool": False, "float": 2.71, "int": 24},
        {"keyword": "test3", "text": "This is a test passage test3.", "list": [7, 8, 9], "bool": True, "float": 1.41, "int": 12},
    ]

    g = bf.Graph()
    g |= FromList(test_data)
    g |= WriteJsonl(project["cache/test_data.jsonl"])
    g.execute(dispatch_brokers=False, mock=True)
    print(g)

    g = ReadJsonl(project["cache/test_data.jsonl"]).to_graph()
    results = g.execute(dispatch_brokers=False, mock=True)
    print(g)

    compare(results, test_data, "keyword")

def test_markdown_lines(tmp_path):
    project = bf.ProjectFolder("test_markdown_lines", 1, 0, 0, data_dir=tmp_path)

    test_data = [
        {"keyword": "test1", "headings": ["Test 1"]},
        {"keyword": "test2", "headings": ["Test 2"]},
        {"keyword": "test3", "headings": ["Test 3"]},
    ]

    g = bf.Graph()
    g |= FromList(test_data)
    g |= WriteMarkdownLines(project["cache/test_data.md"])
    g.execute(dispatch_brokers=False, mock=True)

    g = ReadMarkdownLines(project["cache/test_data.md"]).to_graph()
    results = g.execute(dispatch_brokers=False, mock=True)

    compare(results, test_data, "keyword")

def test_markdown_entries(tmp_path):
    project = bf.ProjectFolder("test_markdown_entries", 1, 0, 0, data_dir=tmp_path)

    test_data = [
        {"text": "test1\n", "headings": ["Test 1"]},
        {"text": "test2\n", "headings": ["Test 2"]},
        {"text": "test3\n", "headings": ["Test 3"]}, # WriteMarkdownEntries will add newlines automatically
    ]

    g = bf.Graph()
    g |= FromList(test_data)
    g |= WriteMarkdownEntries(project["cache/test_data.md"])
    g.execute(dispatch_brokers=False, mock=True)

    g = ReadMarkdownEntries(project["cache/test_data.md"]).to_graph()
    results = g.execute(dispatch_brokers=False, mock=True)

    compare(results, test_data, "text")

def test_rpg_loop(tmp_path):
    project = bf.ProjectFolder("test_rpg_loop", 1, 0, 0, data_dir=tmp_path)
    broker = bf.brokers.LLMBroker(project["cache/llm_broker.jsonl"])

    test_data = [
        {"headings": "Greek Mythology", "keyword": "Blah1"},
        {"headings": "Greek Mythology", "keyword": "Blah2"},
        {"headings": "Greek Mythology", "keyword": "Blah3"},
    ]

    g = bf.Graph()
    g |= FromList(test_data)
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
            seg |= CallLLM(project[f"cache/llm_call_{identifier}.jsonl"], broker, failure_behavior="retry")
            seg |= ExtractResponseText()
            seg |= UpdateChatHistory(character_key=character_key)
            # seg |= ExtractResponseMeta() 
            seg |= CleanupLLMData()
            return seg
        return func
    
    Teacher = Character("teacher_name", "You are a teacher named {teacher_name}. Please only output dialogue.")
    Student = Character("student_name", "You are a student named {student_name}. Please only output dialogue.")

    g |= Teacher("Please introduce the text from {headings} titled {keyword}.", 0)
    g1 = Student("Please ask questions or respond.", 1)
    g1 |= Teacher("Please respond to the student or continue explaining.", 2)
    g |= Repeat(g1, 3)
    g |= Teacher("Please summarize.", 3)
    g |= ChatHistoryToText(template="**{role}**: {content}\n\n")
    results = g.execute(dispatch_brokers=True, mock=True)
    print(g)

    assert len(results) == 3, f"Expected 3 entries, got {len(results)}"
    for entry, reference in zip(results, test_data):
        n_teacher_speaks = 1 + 3 + 1
        n_student_speaks = 3
        dialogue_text = entry.data["text"]
        assert dialogue_text.count("**Teacher**: ") == n_teacher_speaks, f"Expected Teacher to speak {n_teacher_speaks} times, got {dialogue_text.count('Teacher')}"
        assert dialogue_text.count("**Student**: ") == n_student_speaks, f"Expected Student to speak {n_student_speaks} times, got {dialogue_text.count('Student')}"


