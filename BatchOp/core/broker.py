from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from typing import Union, List, Any, Tuple, Iterator, Literal, Dict, NamedTuple, Optional, Callable, Mapping, Iterable
import os
import jsonlines,json
from collections import Counter
import aiofiles,asyncio
from pydantic import BaseModel
from enum import Enum

from ..lib.utils import _to_list, _to_record, _to_BaseModel
from .ledger import Ledger

class BrokerJobStatus(str,Enum):
    QUEUED = "queued"
    DONE = "done"
    FAILED = "failed"
    WAITING = "waiting"
    def is_terminal(self) -> bool:
        return self in {self.DONE, self.FAILED}
    
class BrokerJobRequest(NamedTuple):
    job_idx: str
    status: BrokerJobStatus
    request_object: BaseModel
    meta: Dict|None = None

class BrokerJobResponse(NamedTuple):
    job_idx: str
    status: BrokerJobStatus
    response_object: BaseModel|None = None
    meta: Dict|None = None


class Broker(ABC):
    def __init__(self, cache_path: str, request_cls:type[BaseModel]=None, response_cls:type[BaseModel]=None):
        self.request_cls = request_cls
        self.response_cls = response_cls
        self._ledger = Ledger(cache_path)
    def resume(self):
        self._ledger.resume()
    def enqueue(self, requests: List[BrokerJobRequest]|BrokerJobRequest):
        requests = list({r.job_idx:r for r in _to_list(requests) if not self._ledger.contains(r.job_idx)}.values())
        self._ledger.append(requests,
                            serializer = lambda r: {
                                "idx": r.job_idx,
                                "status": BrokerJobStatus.QUEUED.value,
                                "request": _to_record(r.request_object),
                                "meta": r.meta or {},
                            })
    def dequeue(self, job_idx:str|List):
        self._ledger.remove(_to_list(job_idx))

    def get_job_responses(self)->List[BrokerJobResponse]:
        return self._ledger.filter(
            lambda x: x.status.is_terminal(),
            builder=lambda record: BrokerJobResponse(
                    job_idx=record["idx"],
                    status=BrokerJobStatus(record["status"]),
                    response_object=_to_BaseModel(record.get("response"), self.response_cls, allow_None=True),
                    meta=record.get("meta", {}),
                )
        )
    def get_job_requests(self, status:List|BrokerJobStatus)->List[BrokerJobRequest]:
        status=_to_list(status)
        return self._ledger.filter(
            lambda x: x.status in status,
            builder=lambda record: BrokerJobRequest(
                job_idx=record["idx"],
                status=BrokerJobStatus(record["status"]),
                request_object=_to_BaseModel(record["request"], self.request_cls, allow_None=False),
                meta=record.get("meta", {}),
            )
        )

class DeferredBroker(Broker, ABC):
    @abstractmethod
    def dispatch_jobs(self, jobs:List[BrokerJobRequest]):
        """
            Asynchronously dispatch requests.
            Examples include 
                - sending requests to a batch api
                - emailing requests to a human annotator
                - sending the requests to printer, and ask the user to send these questionaires to their friend,
                    and collect the response as a specifically formatted file on a specified path.
            Note that this method should casually cause an external entity to complete the request.
                but with no guarantee of success or deadline.
        """
        pass
    @abstractmethod
    def collect_job_results(self):
        """
            Collect the responses of the requests dispatched earlier.
            Examples include
                - polling the batch api to see if the request has been completed
                - checking the email inbox for responses from human annotators
                - checking the specified path for the response file from the user
            Note that it might expect the response is not completed
            The result is written to ledger, and can be retrieved by `get_terminated_jobs` method.
        """
        pass

class ImmediateBroker(Broker, ABC):
    @abstractmethod
    def process_jobs(self, jobs:List[BrokerJobRequest]):
        """
            Process the requests.
            Example include
                - spawning a thread pool for concurrency api calls
            This will block the main thread until all requests are processed.
            Note if the process is shut down during processing, completed requests should be cached to disk and resumed later.
            The result is written to ledger, and can be retrieved by `get_terminated_jobs` method.
        """
        pass







__all__ = [
    "BrokerJobStatus",
    "BrokerJobRequest",
    "BrokerJobResponse",
    "Broker",
    "DeferredBroker",
    "ImmediateBroker",
]






