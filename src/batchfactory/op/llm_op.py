from ..core import *
from ..lib.llm_backend import LLMRequest, LLMMessage, LLMResponse, compute_llm_cost, get_provider_name
from ..lib.utils import get_format_keys, hash_texts, ReprUtil
from ..brokers.concurrent_llm_call_broker import ConcurrentLLMCallBroker
from ..core.broker import BrokerJobRequest, BrokerJobResponse, BrokerJobStatus
from .common_op import RemoveField
from ..lib.utils import  _to_record, _to_BaseModel, _dict_to_dataclass, _to_list_2, _pick_field_or_value_strict
from .broker_op import BrokerOp, BrokerFailureBehavior
import copy
from typing import List, Dict, NamedTuple, Set, Tuple
from dataclasses import asdict
import re



class GenerateLLMRequest(ApplyOp):
    "Generate a LLM query from a given prompt, formatting it with the entry data."
    def __init__(self,user_prompt,model,
                 max_completion_tokens=4096,
                 role="user",
                 output_key="llm_request",
                 system_prompt=None,
                 chat_history_key:str|bool|None=None, # if provided, will append the history to the prompt, if True, default to "chat_history"
                 after_prompt=None, # if provided, will append the after_prompt after the history
                 ):
        super().__init__()
        self.role = role
        self.user_prompt = user_prompt
        self.system_prompt = system_prompt
        if chat_history_key is True: 
            chat_history_key = "chat_history"
        self.chat_history_key = chat_history_key
        self.after_prompt = after_prompt
        self.model = model
        self.max_completion_tokens = max_completion_tokens
        self.output_key = output_key
    def _args_repr(self): return ReprUtil.repr_str(self.user_prompt)
    def update(self, entry: Entry) -> None:
        messages = self._build_messages(entry)
        request_obj = LLMRequest(
            custom_id=self._generate_custom_id(messages, self.model, self.max_completion_tokens),
            messages=messages,
            model=self.model,
            max_completion_tokens=self.max_completion_tokens
        )
        entry.data[self.output_key] = request_obj.model_dump()
    
    def _build_messages(self,entry:Entry)->List[LLMMessage]:
        messages = []
        if self.system_prompt:
            system_str = self.system_prompt.format(**{k: entry.data[k] for k in get_format_keys(self.system_prompt)})
            messages.append(LLMMessage(role="system", content=system_str))
        if self.user_prompt:
            prompt_str = self.user_prompt.format(**{k: entry.data[k] for k in get_format_keys(self.user_prompt)})
            messages.append(LLMMessage(role=self.role, content=prompt_str))
        if self.chat_history_key:
            history = entry.data.get(self.chat_history_key, [])
            for msg in history:
                messages.append(LLMMessage(role=msg["role"], content=msg["content"]))
        if self.after_prompt:
            after_prompt_str = self.after_prompt.format(**{k: entry.data[k] for k in get_format_keys(self.after_prompt)})
            messages.append(LLMMessage(role=self.role, content=after_prompt_str))
        return messages

    @staticmethod
    def _generate_custom_id(messages,model,max_completion_tokens):
        texts=[model,str(max_completion_tokens)]
        for message in messages:
            texts.extend([message.role, message.content])
        return hash_texts(*texts)
    
class ExtractResponseMeta(ApplyOp):
    "Extract metadata from the LLM response like model name and accumulated cost."
    def __init__(self, 
                 input_response_key="llm_response", 
                 input_request_key="llm_request",
                 output_model_key="model",
                 accumulated_cost_key="api_cost",
                 ):
        super().__init__()
        self.input_response_key = input_response_key
        self.input_request_key = input_request_key
        self.output_model_key = output_model_key
        self.accumulated_cost_key = accumulated_cost_key
    def update(self, entry: Entry) -> None:
        llm_response = entry.data.get(self.input_response_key, None)
        llm_response:LLMResponse = LLMResponse.model_validate(llm_response)
        llm_request = entry.data.get(self.input_request_key, None)
        llm_request:LLMRequest = LLMRequest.model_validate(llm_request)
        if self.output_model_key:
            entry.data[self.output_model_key] = llm_request.model
        if self.accumulated_cost_key:
            cost = compute_llm_cost(llm_response,get_provider_name(llm_request.model))
            entry.data[self.accumulated_cost_key] = cost + entry.data.get(self.accumulated_cost_key, 0.0)

class ExtractResponseText(ApplyOp):
    "Extract the text content from the LLM response and store it to entry data."
    def __init__(self, 
                 input_key="llm_response", 
                 output_key="text",
                 ):
        super().__init__()
        self.input_key = input_key
        self.output_key = output_key
    def update(self, entry: Entry) -> None:
        llm_response = entry.data.get(self.input_key, None)
        llm_response:LLMResponse = LLMResponse.model_validate(llm_response)
        entry.data[self.output_key] = llm_response.message.content
    
class UpdateChatHistory(ApplyOp):
    "Appending the LLM response to the chat history."
    def __init__(self,
                    input_key="text",
                    output_key="chat_history",
                    character_name:str=None, # e.g. "Timmy"
                    character_key:str=None, # e.g. "character_name"
    ):
        super().__init__()
        self.input_key = input_key
        self.output_key = output_key
        self.character_name = character_name
        self.character_key = character_key
    def update(self, entry: Entry) -> None:
        response_text = entry.data.get(self.input_key, None)
        chat_history = entry.data.setdefault(self.output_key, [])
        chat_history.append({
            "role": _pick_field_or_value_strict(entry.data, self.character_key, self.character_name, default="assistant"),
            "content": response_text,
        })
        entry.data[self.output_key] = chat_history

class ChatHistoryToText(ApplyOp):
    "Format the chat history into a single text."
    def __init__(self, 
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
                 character_name:str|None=None, # e.g. "Timmy"
                 character_key:str|None=None, # e.g. "character_name"
                 prompt_template="{name}: {content}\n",
                 input_key="llm_request",
    ):
        super().__init__()
        self.character_name = character_name
        self.character_key = character_key
        self.input_key = input_key
        self.allowed_roles=["user","assistant","system"]
        self.prompt_template = prompt_template
    def update(self, entry: Entry) -> None:
        llm_request = entry.data.get(self.input_key, None)
        llm_request:LLMRequest = LLMRequest.model_validate(llm_request)
        input_messages = llm_request.messages
        output_messages = []
        assistant_character_name = _pick_field_or_value_strict(entry.data, self.character_key, self.character_name, default="assistant")
        for input_message in input_messages:
            if input_message.role in self.allowed_roles:
                output_messages.append(input_message)
                continue
            if input_message.role == assistant_character_name:
                role = "assistant"
            else:
                role = "user"
            context = self.prompt_template.format(name=input_message.role, content=input_message.content)
            if len(output_messages)>0 and output_messages[-1].role == role:
                output_messages[-1].content += context
            else:
                output_messages.append(LLMMessage(role=role, content=context))
        llm_request.messages = output_messages
        entry.data[self.input_key] = llm_request.model_dump()

    
class PrintTotalCost(OutputOp):
    "Print the total accumulated API cost for the output batch."
    def __init__(self, accumulated_cost_key="api_cost"):
        super().__init__()
        self.accumulated_cost_key = accumulated_cost_key
    def output_batch(self,batch:Dict[str,Entry])->None:
        total_cost = sum(entry.data.get(self.accumulated_cost_key, 0.0) for entry in batch.values())
        if total_cost<0.05:
            print(f"Total API cost for the output: {total_cost: .6f} USD")
        else:
            print(f"Total API cost for the output: ${total_cost:.2f} USD")
    

class ConcurrentLLMCall(BrokerOp):
    "Dispatch concurrent LLM API calls — may induce API billing from external providers."
    def __init__(self,
                    cache_path: str,
                    broker: ConcurrentLLMCallBroker,
                    input_key="llm_request",
                    output_key="llm_response",
                    status_key="status",
                    job_idx_key="job_idx",
                    keep_all_rev: bool = True,
                    failure_behavior:BrokerFailureBehavior = BrokerFailureBehavior.STAY
    ):
        super().__init__(
            cache_path=cache_path,
            broker=broker,
            input_key=input_key,
            output_key=output_key,
            keep_all_rev=keep_all_rev,
            status_key=status_key,
            job_idx_key=job_idx_key,
            failure_behavior=failure_behavior
        )

    def generate_job_idx(self, entry):
        return entry.data[self.input_key]["custom_id"]

    def get_request_object(self, entry: Entry)->Dict:
        return LLMRequest.model_validate(entry.data[self.input_key])
        
    def dispatch_broker(self, mock:bool=False)->None:
        if self.failure_behavior == BrokerFailureBehavior.RETRY:
            allowed_status = [BrokerJobStatus.FAILED, BrokerJobStatus.QUEUED]
        else:
            allowed_status = [BrokerJobStatus.QUEUED]
        requests = self.broker.get_job_requests(allowed_status)
        if not requests:
            return
        self.broker.process_jobs(requests, mock=mock)

class CleanupLLMData(RemoveField):
    "Clean up internal fields used for LLM processing, such as llm_request, llm_response, and status."
    def __init__(self,fields=["llm_request","llm_response","status"]):
        super().__init__(*fields)

def remove_speaker_tag(line):
    "Remove speaker tags. Use ApplyField to wrap it."
    pattern = r'^\s*[*_~`]*\w+[*_~`]*[:：][*_~`]*\s*'
    return re.sub(pattern, '', line)
remove_speaker_tag._show_in_op_list = True

def split_cot(text)->Tuple[str,str]:
    "Split the LLM response into text and chain of thought (CoT). Use ApplyField to wrap it."
    cot = ""
    if "</think>" in text:
        cot, text = text.split("</think>", 1)
        if cot.strip().startswith("<think>"):
            cot = cot.strip()[len("<think>"):]
    return text, cot.strip()
split_cot._show_in_op_list = True

def remove_cot(text):
    "Remove the chain of thought (CoT) from the LLM response. Use ApplyField to wrap it."
    return split_cot(text)[0]
remove_cot._show_in_op_list = True


__all__ = [
    "GenerateLLMRequest",
    "ExtractResponseText",
    "ExtractResponseMeta",
    "UpdateChatHistory",
    "TransformCharacterDialogueForLLM",
    "ConcurrentLLMCall",
    "PrintTotalCost",
    "CleanupLLMData",
    "ChatHistoryToText",
    "remove_speaker_tag",
    "remove_cot",
    "split_cot",
]





