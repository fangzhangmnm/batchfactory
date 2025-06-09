from dataclasses import dataclass, field, asdict
from abc import ABC, abstractmethod
from typing import Union, List, Any, Tuple, Iterator, Dict

from .entry import Entry
from .ledger import _Ledger
from .broker import Broker, BrokerJobStatus
from ..lib.utils import _make_list_of_list, _dict_to_dataclass
from .op_node import BaseOp

class BrokerOp(BaseOp, ABC):
    """
    Used for expensive operations that can be cached.
        like LLM call, search engine call, etc.
    Should only do the call, separate preparation and post processing to atomic Ops.
    The Broker class should handle the api call and caching logic
    Do not guarantee entry immutability
    """
    def __init__(self,
                 cache_path: str,
                 broker: Broker,
                 status_field: str = "status",
    ):
        super().__init__()
        self._ledger = _Ledger(cache_path)
        self.broker = broker
        self.status_field = status_field
    def resume(self):
        self._ledger.resume()
        self.broker.resume()
    def enqueue(self, entries: Dict[str,Entry]):
        """Queue entries and save to the cache
        override is allowed only if entry have greater rev than the old one
        """
        entries_list = [] 
        for idx,input_entry in entries.items():
            current_entry = self._ledger.get(idx,builder=lambda record: _dict_to_dataclass(record, Entry)) \
                if self._ledger.contains(idx) else None
            if current_entry is None or input_entry.rev > current_entry.rev:
                entries_list.append(input_entry)
        for entry in entries_list:
            entry.data[self.status_field] = BrokerJobStatus.QUEUED.value
        self._ledger.remove([entry.idx for entry in entries_list])
        self._ledger.append(entries_list,serializer=asdict)

    @abstractmethod
    def dispatch_broker(self):
        """Send the queued entries to broker and dispatch the broker to process them
        It might not immediately return all the processed entries
        Please make sure only update or remove the entries after broker has safely cahed the jobs by broker.enqueue(jobs)
        """
        pass
    @abstractmethod
    def _check_broker(self):
        """Update the cached entries with the broker results, including output fields and status."""
        pass
    def get_results(self) -> Dict[str,Entry]:
        """Check new brock result, and returns all the entries processed (include earlier ones), whenever done or failed"""
        self._check_broker()
        entry_list = self._ledger.filter(
            lambda x:BrokerJobStatus(x.data[self.status_field]).is_terminal(),
            builder=lambda record: _dict_to_dataclass(record, Entry)
        )
        return {e.idx: e for e in entry_list}

__all__=[
    "BrokerOp",
]