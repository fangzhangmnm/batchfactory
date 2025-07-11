from ..core.entry import Entry
from ..core.base_op import ApplyOp, OutputOp
from ..lib.llm_backend import LLMRequest, LLMMessage, LLMResponse, llm_client_hub
from ..lib.prompt_maker import PromptMaker
from ..lib.utils import get_format_keys, hash_texts, ReprUtil, KeysUtil
from ..brokers.llm_broker import LLMBroker
from ..core.broker import BrokerJobStatus
from .common_op import RemoveField, MapField
from ..lib.utils import _to_list_2, _pick_field_or_value_strict
from .broker_op import BrokerOp, BrokerFailureBehavior
from ..core.project_folder import ProjectFolder
from ._registery import show_in_op_list
from . import functional as F
from typing import List, Dict, NamedTuple, Set, Tuple, Any

class GenerateLLMRequest(ApplyOp):
    "Generate LLM requests from a given prompt, formatting it with the entry data."
    def __init__(self,user_prompt:str|PromptMaker|None,
                *,
                model,
                max_completion_tokens=4096,
                role="user",
                output_key="llm_request",
                system_prompt:str|PromptMaker|None=None,
                chat_history_key:str|bool|None=None, # if provided, will append the history to the prompt, if True, default to "chat_history"
                after_prompt:str|PromptMaker|None=None, # if provided, will append the after_prompt after the history
                ):
        if not llm_client_hub.is_chat_completion_model(model):
            raise ValueError(f"{model} is not a chat completion model.")
        super().__init__()
        self.model = model
        self.role = role
        self.user_prompt:PromptMaker = PromptMaker.from_prompt(user_prompt)
        self.system_prompt:PromptMaker = PromptMaker.from_prompt(system_prompt) if system_prompt else None
        self.after_prompt:PromptMaker = PromptMaker.from_prompt(after_prompt) if after_prompt else None
        if chat_history_key is True: 
            chat_history_key = "chat_history"
        self.chat_history_key = chat_history_key
        self.max_completion_tokens = max_completion_tokens
        self.output_key = output_key
    def _args_repr(self): return repr(self.user_prompt)
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
        if self.system_prompt is not None:
            system_str = self.system_prompt.make_prompt(entry.data)
            messages.append(LLMMessage(role="system", content=system_str))
        if self.user_prompt is not None:
            prompt_str = self.user_prompt.make_prompt(entry.data)
            messages.append(LLMMessage(role=self.role, content=prompt_str))
        if self.chat_history_key is not None:
            history = entry.data.get(self.chat_history_key, [])
            for msg in history:
                messages.append(LLMMessage(role=msg["role"], content=msg["content"]))
        if self.after_prompt is not None:
            after_prompt_str = self.after_prompt.make_prompt(entry.data)
            messages.append(LLMMessage(role=self.role, content=after_prompt_str))
        return messages

    @staticmethod
    def _generate_custom_id(messages,model,max_completion_tokens):
        texts=[model,str(max_completion_tokens)]
        for message in messages:
            texts.extend([message.role, message.content])
        return hash_texts(*texts)
    
# class ExtractResponseMeta(ApplyOp):
#     "Extract metadata from the LLM (or embedding) response like model name and accumulated cost."
#     def __init__(self, 
#                  input_response_key="llm_response", 
#                  input_request_key="llm_request",
#                  output_model_key="model",
#                  accumulated_cost_key="api_cost",
#                  ):
#         super().__init__()
#         self.input_response_key = input_response_key
#         self.input_request_key = input_request_key
#         self.output_model_key = output_model_key
#         self.accumulated_cost_key = accumulated_cost_key
#     def update(self, entry: Entry) -> None:
#         llm_response = entry.data.get(self.input_response_key, None)
#         llm_response:LLMResponse = LLMResponse.model_validate(llm_response)
#         llm_request = entry.data.get(self.input_request_key, None)
#         llm_request:LLMRequest = LLMRequest.model_validate(llm_request)
#         if self.output_model_key:
#             entry.data[self.output_model_key] = llm_request.model
#         if self.accumulated_cost_key:
#             entry.data[self.accumulated_cost_key] = llm_response.cost + entry.data.get(self.accumulated_cost_key, 0.0)

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
    
# class PrintTotalCost(OutputOp):
#     "Print the total accumulated API cost for the output batch."
#     def __init__(self, accumulated_cost_key="api_cost"):
#         super().__init__()
#         self.accumulated_cost_key = accumulated_cost_key
#     def output_batch(self,batch:Dict[str,Entry])->None:
#         total_cost = sum(entry.data.get(self.accumulated_cost_key, 0.0) for entry in batch.values())
#         if total_cost<0.05:
#             print(f"Total API cost for the output: {total_cost: .6f} USD")
#         else:
#             print(f"Total API cost for the output: ${total_cost:.2f} USD")
    
class CallLLM(BrokerOp):
    "Dispatch concurrent API calls for LLM — may induce API billing from external providers."
    def __init__(self,
                 *,
                cache_path: str=None,
                broker: LLMBroker=None,
                input_key="llm_request",
                output_key="llm_response",
                status_key="status",
                job_idx_key="job_idx",
                keep_all_rev: bool = True,
                failure_behavior:BrokerFailureBehavior = BrokerFailureBehavior.STAY,
                barrier_level: int = 1,
    ):
        if broker is None: broker = ProjectFolder.get_current().get_default_broker(LLMBroker)
        if not isinstance(broker, LLMBroker): raise ValueError(f"Expected broker to be of type LLMBroker, got {type(broker)}")
        super().__init__(
            cache_path=cache_path,
            broker=broker,
            input_key=input_key,
            output_key=output_key,
            keep_all_rev=keep_all_rev,
            status_key=status_key,
            job_idx_key=job_idx_key,
            failure_behavior=failure_behavior,
            barrier_level=barrier_level
        )

    def generate_job_idx(self, entry):
        return entry.data[self.input_key]["custom_id"]

    def get_request_object(self, entry: Entry)->Dict:
        return LLMRequest.model_validate(entry.data[self.input_key]).model_dump()
        
    def dispatch_broker(self, mock:bool=False)->None:
        if self.failure_behavior == BrokerFailureBehavior.RETRY:
            allowed_status = [BrokerJobStatus.FAILED, BrokerJobStatus.QUEUED]
        else:
            allowed_status = [BrokerJobStatus.QUEUED]
        requests = self.broker.get_job_requests(allowed_status)
        if len(requests) == 0:
            return
        self.broker.process_jobs(requests, mock=mock)

class CleanupLLMData(RemoveField):
    "Clean up internal fields for LLM processing, such as `llm_request`, `llm_response`, `status`, and `job_idx`."
    def __init__(self,keys=["llm_request","llm_response","status","job_idx"]):
        super().__init__(*keys)

class CountTotalCharacters(OutputOp):
    "Count the total number of characters in the output text field of the batch."
    def __init__(self, keys:List[str]=["text"]):
        super().__init__()
        self.keys = KeysUtil.make_keys(keys)
    def output_batch(self, batch:Dict[str,Entry]) -> None:
        total_chars = 0
        for entry in batch.values():
            for key in self.keys:
                text = entry.data.get(key, "")
                total_chars += len(text)
        print(f"[CountTotalCharacters] Total characters in output: {total_chars:,} characters.")

@show_in_op_list(highlight=True)
def AskLLM(prompt:str|PromptMaker,
            *,
            model:str,
            output_key: str = "text",
            cache_path: str=None,
            broker: LLMBroker=None,
            max_completion_tokens=4096,
            system_prompt:str|PromptMaker|None=None,
            remove_cot:bool=True,
            failure_behavior:BrokerFailureBehavior = BrokerFailureBehavior.STAY,
            ):
    "Ask the LLM with a given prompt and model, returning the response text."
    g = GenerateLLMRequest(
        user_prompt=prompt,
        model=model,
        max_completion_tokens=max_completion_tokens,
        output_key="llm_request",
        system_prompt=system_prompt)
    g |= CallLLM(
        cache_path=cache_path,
        broker=broker,
        input_key="llm_request",
        output_key="llm_response",
        status_key="status",
        job_idx_key="job_idx",
        failure_behavior=failure_behavior,
    )
    g |= ExtractResponseText(
        input_key="llm_response",
        output_key=output_key,
    )
    g |= CleanupLLMData()
    if remove_cot:
        g |= MapField(F.remove_cot, output_key)
    return g

__all__ = [
    "GenerateLLMRequest",
    "ExtractResponseText",
    # "ExtractResponseMeta",
    # "PrintTotalCost",
    "CleanupLLMData",
    "CallLLM",
    "AskLLM",
    "CountTotalCharacters",
]





