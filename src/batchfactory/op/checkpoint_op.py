from dataclasses import dataclass, field, asdict
from abc import ABC, abstractmethod
from typing import Union, List, Any, Tuple, Iterator, Dict, Set, Literal

from ..core.entry import Entry
from ..core.ledger import _Ledger
from ..core.op_base import BaseOp, PumpOptions, PumpOutput


class CheckpointOp(BaseOp, ABC):
    """
    - constantly save its output to a cache
    - consume all inputs, including the one being invalidated by the newer version in the cache
    """
    def __init__(self,cache_path: str,keep_all_rev:bool):
        super().__init__(n_in_ports=1, n_out_ports=1)
        self._ledger = _Ledger(cache_path)
        self.keep_all_rev = keep_all_rev
        self.emitted_revs = {} # prevent the same entry being emitted twice

    def resume(self):
        super().resume()
        self._ledger.resume()

    @abstractmethod
    def prepare_input(self, entry:Entry) -> None:
        "preprocess the entry before depositing to the cache"
        pass

    @abstractmethod
    def process_cached_batch(self, cached_newest_batch:Dict[str,Entry], options: PumpOptions)->None:
        "process the cached newest batch, and update the cache with the results, by calling self.update_batch()"
        pass

    @abstractmethod
    def is_ready_for_output(self, entry:Entry) -> bool:
        "check if the entry is being processed and ready for output"
        pass

    def _deposit_batch(self, batch:Dict[str,Entry]):
        """
        - Queue entries and save to the cache
        - rejects entries with same idx, unless rev is larger
        """
        submit_records = {}
        for entry in batch.values():
            record = self._serialize_entry(entry)
            record_idx = record['idx']
            if self._ledger.contains(record_idx) and record['rev'] <= self._ledger.get_one(record_idx)['rev']:
                continue
            submit_records[record_idx] = record
        self._ledger.update_many(submit_records)

    def update_batch(self, batch:Dict[str,Entry]):
        """
        - Update the cache with the batch
        - accepts entries with same idx, unless rev is smaller
        """
        submit_records = {}
        for entry in batch.values():
            record = self._serialize_entry(entry)
            record_idx = record['idx']
            if self._ledger.contains(record_idx) and record['rev'] < self._ledger.get_one(record_idx)['rev']:
                continue
            submit_records[record_idx] = record
        self._ledger.update_many(submit_records,compact=True)
    
    def _get_up_to_date_batch(self)->Dict[str, Entry]:
        "get entries of the newest rev"
        records = self._ledger.get_all()
        newest_entries = {}
        for record in records.values():
            entry = self._build_entry(record)
            if entry.idx in newest_entries and entry.rev < newest_entries[entry.idx].rev:
                continue
            newest_entries[entry.idx] = entry
        return newest_entries
    
    def pump(self, inputs: Dict[int, Dict[str, Entry]], options: PumpOptions) -> PumpOutput:
        input_batch = inputs.get(0, {})
        if input_batch:
            for entry in input_batch.values():
                self.prepare_input(entry)
            self._deposit_batch(input_batch)
            del input_batch
        self.process_cached_batch(self._get_up_to_date_batch(), options)
        outputs,consumed,did_emit={0:{}}, {0:set()}, False
        for idx,entry in self._get_up_to_date_batch().items():
            if not self.is_ready_for_output(entry):
                continue
            if idx in self.emitted_revs and entry.rev <= self.emitted_revs[idx]:
                continue
            outputs[0][idx] = entry
            consumed[0].add(idx)
            did_emit = True
            self.emitted_revs[idx] = entry.rev
        return PumpOutput(outputs, consumed, did_emit)
    
    def _get_entry(self, idx: str, rev: int)->Entry:
        if self.keep_all_rev:
            return self._ledger.get_one(f"{idx}_{rev}", builder=self._build_entry)
        else:
            return self._ledger.get_one(idx, builder=self._build_entry)
    
    def _contains(self, idx: str, rev: int)->bool:
        if self.keep_all_rev:
            return self._ledger.contains(f"{idx}_{rev}")
        else:
            return self._ledger.contains(idx)

    def _serialize_entry(self,entry:Entry)->Dict[str, Any]:
        record = asdict(entry)
        if self.keep_all_rev:
            record['idx'] = f"{entry.idx}_{entry.rev}"
        return record
    def _build_entry(self, record: Dict[str, Any]) -> Entry:
        if self.keep_all_rev and '_' in record['idx']:
            record['idx'],_ = record['idx'].rsplit('_',1)
        return Entry(**record)

class CheckPoint(CheckpointOp):
    "A no-op checkpoint that saves inputs to the cache, and resumes from the cache."
    def __init__(self, cache_path: str, keep_all_rev: bool = True):
        super().__init__(cache_path, keep_all_rev)
    def prepare_input(self, entry: Entry) -> None:
        pass
    def process_cached_batch(self, cached_newest_batch: Dict[str, Entry], options: PumpOptions) -> None:
        pass
    def is_ready_for_output(self, entry: Entry) -> bool:
        return True
    
        
__all__ = [
    "CheckpointOp",
    "CheckPoint",
]
        