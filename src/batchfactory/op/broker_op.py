from abc import ABC, abstractmethod
from typing import Dict, Tuple, Set
from enum import Enum
from copy import deepcopy
from ..core.entry import Entry
from ..core.op_base import PumpOptions
from .checkpoint_op import CheckpointOp
from ..core.broker import Broker, BrokerJobStatus, BrokerJobRequest, BrokerJobResponse

class BrokerFailureBehavior(str,Enum):
    "Defines how to handle broker job failures."
    STAY = 'stay'  # Keep the job in the queue with 'failed' status
    RETRY = 'retry'  # Retry the job
    EMIT = 'emit'  # Emit the job for further processing
    
class BrokerOp(CheckpointOp,ABC):
    """
    - Used for expensive operations that can be cached.
        - e.g. LLM call, search engine call, human data labeling.
    - Should only do the call, separate preparation and post processing to atomic Ops.
    - The Broker class should handle the api call and caching logic
    """
    def __init__(self,
                    cache_path: str,
                    broker: Broker,
                    input_key: str,
                    output_key: str,
                    status_key: str = "status",
                    job_idx_key: str = "job_idx",
                    keep_all_rev: bool = True,
                    failure_behavior:BrokerFailureBehavior = BrokerFailureBehavior.STAY,
                    barrier_level: int = 1
                    ):
            super().__init__(cache_path, keep_all_rev, barrier_level)
            self.broker = broker
            self.input_key = input_key
            self.output_key = output_key
            self.status_key = status_key
            self.failure_behavior = failure_behavior
            self.job_idx_key = job_idx_key
    def resume(self):
        super().resume()
        self.broker.resume()
    def prepare_input(self, entry: Entry) -> None:
        entry.data[self.status_key] = BrokerJobStatus.QUEUED.value
        entry.data[self.job_idx_key] = self.generate_job_idx(entry)
    def is_ready_for_output(self, entry: Entry) -> bool:
        state = BrokerJobStatus(entry.data[self.status_key])
        if state == BrokerJobStatus.DONE:
            return True
        if state == BrokerJobStatus.FAILED and self.failure_behavior == BrokerFailureBehavior.EMIT:
            return True
        return False

    def enqueue_requests(self, queued_entries: Dict[str,Entry]):
        """
        - Enqueue newest unprocessed requests to the broker
        - Might also update the status of the entries in the cache.
        """
        requests:Dict[str,BrokerJobRequest] = {}
        for entry in queued_entries.values():
            job_idx = entry.data[self.job_idx_key]
            request_object = self.get_request_object(entry)
            requests[job_idx] = BrokerJobRequest(
                job_idx=job_idx,
                status=BrokerJobStatus.QUEUED,
                request_object=deepcopy(request_object),
                meta={"entry_idx": entry.idx, "entry_rev": entry.rev},
            )
        self.broker.enqueue(requests)

        # now update the status of the entries in the cache
        for entry in queued_entries.values():
            entry.data[self.status_key] = BrokerJobStatus.QUEUED.value
        self.update_batch(queued_entries)

    # @abstractmethod
    # def get_job_idx_and_request_object(self, entry: Entry)->Tuple[str, Dict]:
    #     "get job_idx and request_object from the entry"
    #     pass

    @abstractmethod
    def generate_job_idx(self, entry: Entry) -> str:
        pass

    @abstractmethod
    def get_request_object(self, entry: Entry) -> Dict:
        pass

    @abstractmethod
    def dispatch_broker(self, mock: bool = False) -> None:
        """
        - Asynchronously dispatch requests.
        - Examples include sending requests to a batch API or emailing requests to a human annotator.
        """
        pass

    def check_broker(self)->Tuple[Dict[str,Entry],Set[str]]:
        """
        - Retrieve new responses from the broker
        - Returns batch, consumed_job_idxs
            - batch will be updated to the cache
            - consumed_job_idxs will be dequeued from the broker
        """
        batch = {}
        consumed_job_idxs = set()
        for response in self.broker.get_job_responses().values():
            # note that entry_idx is not job_idx
            entry_idx = response.meta.get("entry_idx", None)
            rev = response.meta.get("entry_rev", 0)
            if entry_idx is None:
                print(f"Response {response.job_idx} has no entry index in meta, skipping.")
                continue
            if not self._contains(entry_idx, rev=rev):
                print(f"Response {response.job_idx} has no matching entry in the ledger, skipping.")
                continue
            entry:Entry = self._get_entry(entry_idx, rev=rev)
            if entry.data[self.job_idx_key] != response.job_idx:
                # this is a common situation when the same entry_idx on different parallel routes enters the same broker
                continue
            entry.data[self.status_key] = response.status.value
            if response.status.is_terminal() and response.response_object is not None:
                entry.data[self.output_key] = response.response_object.model_dump()
            else:
                entry.data[self.output_key] = None
            consumed_job_idxs.add(response.job_idx)
            batch[entry.idx] = entry
        return batch, consumed_job_idxs


    def process_cached_batch(self, cached_newest_batch: Dict[str, Entry], options: PumpOptions) -> None:
        # a broker might be shared by other parts of the graph. check broker for new results before dispatching
        # might not needed
        # dequeued_entries, consumed_job_idxs = self.check_broker()
        # if dequeued_entries:
        #     self.update_batch(dequeued_entries)
        # if consumed_job_idxs:
        #     self.broker.dequeue(consumed_job_idxs)
        # del dequeued_entries, consumed_job_idxs

        queued_entries = {}
        for entry in cached_newest_batch.values():
            state = BrokerJobStatus(entry.data[self.status_key])
            if state == BrokerJobStatus.QUEUED:
                queued_entries[entry.idx] = entry
            elif state == BrokerJobStatus.FAILED and self.failure_behavior == BrokerFailureBehavior.RETRY:
                queued_entries[entry.idx] = entry
        if queued_entries:
            self.enqueue_requests(queued_entries)
        del queued_entries

        if options.dispatch_brokers:
            self.dispatch_broker(mock=options.mock)

        dequeued_entries, consumed_job_idxs = self.check_broker()
        if dequeued_entries:
            self.update_batch(dequeued_entries)
        if consumed_job_idxs:
            self.broker.dequeue(consumed_job_idxs)
        del dequeued_entries, consumed_job_idxs
    

__all__ = [
    'BrokerOp',
    'BrokerFailureBehavior',
]