from abc import ABC, abstractmethod
from typing import Dict
from enum import Enum
from ..core.entry import Entry
from ..core.op_base import PumpOptions
from .checkpoint_op import CheckpointOp
from ..core.broker import Broker, BrokerJobStatus

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
                    status_field: str = "status",
                    keep_all_rev: bool = True,
                    failure_behavior:BrokerFailureBehavior = BrokerFailureBehavior.STAY
                    ):
            super().__init__(cache_path, keep_all_rev)
            self.broker = broker
            self.status_field = status_field
            self.failure_behavior = failure_behavior
    def resume(self):
        super().resume()
        self.broker.resume()
    def prepare_input(self, entry: Entry) -> None:
        entry.data[self.status_field] = BrokerJobStatus.QUEUED.value
    def is_ready_for_output(self, entry: Entry) -> bool:
        state = BrokerJobStatus(entry.data[self.status_field])
        if state == BrokerJobStatus.DONE:
            return True
        if state == BrokerJobStatus.FAILED and self.failure_behavior == BrokerFailureBehavior.EMIT:
            return True
        return False

    @abstractmethod
    def enqueue_requests(self, queued_entries: Dict[str,Entry]):
        """
        - Enqueue newest unprocessed requests to the broker
        - Might also update the status of the entries in the cache.
        """
        pass
    @abstractmethod
    def dispatch_broker(self, mock: bool = False) -> None:
        """
        - Asynchronously dispatch requests.
        - Examples include sending requests to a batch API or emailing requests to a human annotator.
        """
        pass
    @abstractmethod
    def check_broker(self) -> Dict[str, Entry]:
        """
        - Retrieve new responses from the broker
        - Returns batch, consumed_job_idxs
            - batch will be updated to the cache
            - consumed_job_idxs will be dequeued from the broker
        """
        pass
    def process_cached_batch(self, cached_newest_batch: Dict[str, Entry], options: PumpOptions) -> None:
        queued_entries = {}
        for entry in cached_newest_batch.values():
            state = BrokerJobStatus(entry.data[self.status_field])
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

__all__ = [
    'BrokerOp',
    'BrokerFailureBehavior',
]