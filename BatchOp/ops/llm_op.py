from ..core import *
from ..lib.llm_backend import LLMRequest, LLMMessage, LLMResponse
from ..lib.utils import get_format_keys, hash_text
from ..brokers.concurrent_llm_call import ConcurrentLLMCallBroker
from ..core.broker import BrokerJobRequest, BrokerJobResponse, BrokerJobStatus
from ..lib.utils import  _to_record, _to_BaseModel, _dict_to_dataclass
import copy
from typing import List, Dict, NamedTuple
from dataclasses import asdict



class PromptOp(AtomicOp):
    """
    Generate a LLM query from a given prompt and save to entry.data["llm_request"]
    if the prompt is a string template, it will format according to entry.data 
    """
    def __init__(self,prompt,model,
                 max_completion_tokens=4096,
                 role="user",
                 output_field="llm_request",
                 system=""):
        super().__init__()
        self.role = role
        self.prompt = prompt
        self.system_prompt = system
        self.model = model
        self.max_completion_tokens = max_completion_tokens
        self.format_keys = get_format_keys(prompt)
        self.output_field = output_field
    def update(self, entry: Entry) -> Entry:
        query_str = self.prompt.format(**{k: entry.data[k] for k in self.format_keys})
        messages = [LLMMessage(role=self.role, content=query_str)]
        if self.system_prompt:
            messages.insert(0, LLMMessage(role="system", content=self.system_prompt))
        request_obj = LLMRequest(
            custom_id=self._generate_custom_id(messages, self.model, self.max_completion_tokens),
            messages=messages,
            model=self.model,
            max_completion_tokens=self.max_completion_tokens
        )
        entry.data[self.output_field] = request_obj.model_dump()
        return entry

    @staticmethod
    def _generate_custom_id(messages,model,max_completion_tokens):
        texts=[model,str(max_completion_tokens)]
        for message in messages:
            texts.extend([message.role, message.content])
        return hash_text(*texts)

class ExtractResponseOp(AtomicOp):
    def __init__(self, input_field="llm_response", output_field="response_text"):
        super().__init__()
        self.input_field = input_field
        self.output_field = output_field
    def update(self, entry: Entry) -> Entry:
        llm_response = entry.data.get(self.input_field, None)
        if llm_response is None:
            raise ValueError(f"Entry {entry.idx} does not have field '{self.input_field}'")
        llm_response = LLMResponse.model_validate(llm_response)
        entry.data[self.output_field] = llm_response.message.content
        return entry
    

class ConcurrentLLMCallOp(BrokerOp):
    def __init__(self, 
                    cache_path: str,
                    broker: ConcurrentLLMCallBroker,
                    input_field="llm_request",
                    output_field="llm_response",
                    status_field="status",
                    retry_failed:bool=False
    ):
        super().__init__(
            cache_path=cache_path,
            broker=broker,
            status_field=status_field
        )
        self.input_field = input_field
        self.output_field = output_field
        self.status_field = status_field
        self.retry_failed = retry_failed

    def dispatch_broker(self):
        entries:List[Entry] = self._ledger.filter(
            lambda x: BrokerJobStatus(x.data[self.status_field]) == BrokerJobStatus.QUEUED,
            builder=lambda record: _dict_to_dataclass(record, Entry)
        )
        jobs=[]
        for entry in entries:
            llm_request = _to_BaseModel(entry.data[self.input_field], LLMRequest)
            jobs.append(
                BrokerJobRequest(
                    job_idx=llm_request.custom_id,
                    status=BrokerJobStatus.QUEUED,
                    request_object=copy.deepcopy(llm_request),
                    meta={"entry_idx": entry.idx},
                )
            )
        self.broker.enqueue(jobs)

        # only update after broker safely cached the results
        for entry in entries:
            entry.data[self.status_field] = BrokerJobStatus.QUEUED.value
        self._ledger.update(entries,compact=True,serializer=asdict)

        # now call the broker to process the jobs
        broker:ConcurrentLLMCallBroker = self.broker
        allowed_status = [BrokerJobStatus.FAILED, BrokerJobStatus.QUEUED] if self.retry_failed else [BrokerJobStatus.QUEUED]
        broker.process_jobs(broker.get_job_requests(allowed_status))

    def _check_broker(self):
        entries = []
        job_idxs = []
        for response in self.broker.get_job_responses():
            entry_idx = response.meta.get("entry_idx",None)
            if entry_idx is None:
                print(f"Response {response.job_idx} has no entry index in meta, skipping.")
                continue
            if not self._ledger.contains(entry_idx):
                continue
            entry:Entry = self._ledger.get(entry_idx,builder=lambda record: _dict_to_dataclass(record, Entry))
            entry.data[self.status_field] = response.status.value
            if response.status.is_terminal():
                entry.data[self.output_field] = _to_record(response.response_object)
            else:
                entry.data[self.output_field] = None
            job_idxs.append(response.job_idx)
            entries.append(entry)
        if entries:
            self._ledger.update(entries,compact=True,serializer=asdict)
            self.broker.dequeue(job_idxs)
        


__all__ = [
    "PromptOp",
    "ExtractResponseOp",
    "ConcurrentLLMCallOp",
]





