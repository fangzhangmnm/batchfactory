from ..core.entry import Entry
from ..core.base_op import ApplyOp, OutputOp
from ..lib.llm_backend import LLMRequest, LLMMessage, LLMResponse, llm_client_hub
from ..lib.prompt_maker import PromptMaker
from ..lib.utils import get_format_keys, hash_texts, ReprUtil
from ..brokers.llm_broker import LLMBroker
from ..core.broker import BrokerJobStatus
from .common_op import RemoveField, MapField
from ..lib.utils import _to_list_2, _pick_field_or_value_strict
from .broker_op import BrokerOp, BrokerFailureBehavior
from ..core.project_folder import ProjectFolder
from ._registery import show_in_op_list
from .llm_op import GenerateLLMRequest, CleanupLLMData, ExtractResponseText, CallLLM
from . import functional as F
from typing import List, Dict, NamedTuple, Set, Tuple, Any
from pathlib import Path


class UpdateChatHistory(ApplyOp):
    "Appending the LLM response to the chat history."
    def __init__(self,
                    *,
                    input_key="text",
                    output_key="chat_history",
                    character_name:str=None, # e.g. "Timmy"
                    character_name_key:str=None, # e.g. "character_name"
    ):
        super().__init__()
        self.input_key = input_key
        self.output_key = output_key
        self.character_name = character_name
        self.character_name_key = character_name_key
    def update(self, entry: Entry) -> None:
        response_text = entry.data.get(self.input_key, None)
        chat_history = entry.data.setdefault(self.output_key, [])
        chat_history.append({
            "role": _pick_field_or_value_strict(entry.data, self.character_name_key, self.character_name, default="assistant"),
            "content": response_text,
        })
        entry.data[self.output_key] = chat_history

class ChatHistoryToText(ApplyOp):
    "Format the chat history into a single text."
    def __init__(self, 
                *,
                input_key="chat_history",
                output_key="text",
                template="**{role}**: {content}\n\n",
                exclude_roles:List[str]|None=None, # e.g. ["system"]
    ):
        super().__init__()
        self.input_key = input_key
        self.output_key = output_key
        self.template = template
        self.exclude_roles = _to_list_2(exclude_roles)
    def update(self, entry: Entry) -> None:
        text=""
        chat_history = entry.data[self.input_key]
        for message in chat_history:
            if message["role"] in self.exclude_roles:
                continue
            text += self.template.format(role=message["role"], content=message["content"])
        entry.data[self.output_key] = text

        
class TransformCharacterDialogueForLLM(ApplyOp):
    "Map custom character roles to valid LLM roles (user/assistant/system). Must be called after GenerateLLMRequest."
    def __init__(self, 
                *,
                character_name:str|None=None, # e.g. "Timmy"
                character_name_key:str|None=None, # e.g. "character_name"
                dialogue_template="{name}: {content}\n",
                input_key="llm_request",
    ):
        super().__init__()
        self.character_name = character_name
        self.character_name_key = character_name_key
        self.input_key = input_key
        self.allowed_roles=["user","assistant","system"]
        self.dialogue_template = dialogue_template
    def update(self, entry: Entry) -> None:
        llm_request = entry.data.get(self.input_key, None)
        llm_request:LLMRequest = LLMRequest.model_validate(llm_request)
        input_messages = llm_request.messages
        output_messages = []
        assistant_character_name = _pick_field_or_value_strict(entry.data, self.character_name_key, self.character_name, default="assistant")
        for input_message in input_messages:
            if input_message.role in self.allowed_roles:
                output_messages.append(input_message)
                continue
            if input_message.role == assistant_character_name:
                role = "assistant"
            else:
                role = "user"
            context = self.dialogue_template.format(name=input_message.role, content=input_message.content)
            if len(output_messages)>0 and output_messages[-1].role == role:
                output_messages[-1].content += context
            else:
                output_messages.append(LLMMessage(role=role, content=context))
        llm_request.messages = output_messages
        entry.data[self.input_key] = llm_request.model_dump()


@show_in_op_list(highlight=True)
class AICharacter:
    """
    Create a callable AI-character that yields a dialogue subgraph.
    - Usage:
        - `Timmy = AICharacter("Timmy", "You are a character named Timmy.")`
        - `Timmy("Please introduce yourself.")`
    """
    def __init__(self,
            name:str|None,
            character_setting_prompt:str|PromptMaker,
            *,
            model:str|None,
            broker: LLMBroker=None,
            name_key=None,
            max_completion_tokens=4096,
            system_prompt:str|PromptMaker|None=None,
            remove_cot:bool=True,
            chat_history_key="chat_history",
            failure_behavior:BrokerFailureBehavior=BrokerFailureBehavior.STAY,
            ):
        if name is not None and name_key is not None:
            raise ValueError("Only one of character_name or character_name_key should be provided.")
        if character_setting_prompt is None:
            raise ValueError("character_setting_prompt must be provided.")
        self.name = name
        self.character_setting_prompt = PromptMaker.from_prompt(character_setting_prompt)
        self.name_key = name_key
        self.model = model
        self.broker = broker
        self.max_completion_tokens = max_completion_tokens
        self.system_prompt = PromptMaker.from_prompt(system_prompt) if system_prompt else None
        self.remove_cot = remove_cot
        self.chat_history_key = chat_history_key
        self.failure_behavior = failure_behavior
        self._call_count=0
    def __repr__(self):
        return f"AICharacter({self.name or self.name_key})"
    def __call__(self,command:str|PromptMaker,*,cache_path:str|Path=None):
        if cache_path is None:
            cache_name = (self.name or self.name_key) + f"_{self._call_count}"
            self._call_count += 1
            cache_path = ProjectFolder.get_current()["op_cache/characters"]/cache_name
        g = GenerateLLMRequest(
            user_prompt=self.character_setting_prompt,
            model=self.model,
            max_completion_tokens=self.max_completion_tokens,
            output_key="llm_request",
            system_prompt=self.system_prompt,
            chat_history_key=self.chat_history_key, # if True, will use "chat_history" as the key
            after_prompt=command,
        )
        g |= TransformCharacterDialogueForLLM(
            character_name=self.name, 
            character_name_key=self.name_key, 
            input_key="llm_request"
            )
        g |= CallLLM(
            cache_path=cache_path,
            broker=self.broker,
            input_key="llm_request",
            output_key="llm_response",
            status_key="llm_status",
            job_idx_key="llm_job_idx",
            failure_behavior=self.failure_behavior,
        )
        g |= ExtractResponseText(
            input_key="llm_response",
            output_key="dialogue_text",
        )
        if self.remove_cot:
            g |= MapField(F.remove_cot, "dialogue_text")
        g |= MapField(F.remove_speaker_tag, "dialogue_text")
        g |= UpdateChatHistory(
            input_key="dialogue_text",
            output_key=self.chat_history_key,
            character_name=self.name,
            character_name_key=self.name_key,
        )
        g |= CleanupLLMData()
        return g

__all__ = [
    "UpdateChatHistory",
    "ChatHistoryToText",
    "TransformCharacterDialogueForLLM",
    "AICharacter",
]