from dataclasses import dataclass, field, asdict
from abc import ABC, abstractmethod
from typing import Union, List, Any, Tuple, Iterator, Dict, Set, Literal
import os

from ..core.entry import Entry
from ..core.project_folder import ProjectFolder
from ..core.ledger import Ledger
from ..core.ledger2 import Ledger2
from ..core.base_op import BaseOp, PumpOptions, PumpOutput
from ..lib.utils import ReprUtil
from ._registery import show_in_op_list


class CheckpointOp(BaseOp, ABC):
    """
    - constantly save its output to a cache
    - consume all inputs, including the one being invalidated by the newer version in the cache
    """
    def __init__(self,cache_path:str|None,*,keep_all_rev:bool,barrier_level:int):
        if cache_path is None:
            cache_path = ProjectFolder.get_current().generate_op_path(self)
        if barrier_level < 1: raise ValueError("barrier_level of CheckpointOp must be at least 1")
        super().__init__(n_in_ports=1, n_out_ports=1, barrier_level=barrier_level)
        self._ledger:Ledger = None
        self._ledger2 = Ledger2(cache_path)
        self.keep_all_rev = keep_all_rev
        self.emitted_revs = {} # prevent the same entry being emitted twice

    def _args_repr(self): return ReprUtil.repr_path(self._ledger2.path)

    def reset(self):
        super().reset()
        # self._ledger.reset()
        self.emitted_revs.clear()

    # def resume(self):
    #     super().resume()
        # self._ledger.resume()

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
            old_record = self._ledger2.get_one(record_idx, default=None)
            if old_record is not None and record['rev'] <= old_record['rev']:
                continue
            submit_records[record_idx] = record
        if len(submit_records) > 0:
            self._ledger2.update_many_sync(submit_records)

    def update_batch(self, batch:Dict[str,Entry]):
        """
        - Update the cache with the batch
        - accepts entries with same idx, unless rev is smaller
        """
        submit_records = {}
        for entry in batch.values():
            record = self._serialize_entry(entry)
            record_idx = record['idx']
            old_record = self._ledger2.get_one(record_idx, default=None)
            if old_record is not None and record['rev'] < old_record['rev']:
                continue
            submit_records[record_idx] = record
        if len(submit_records) > 0:
            self._ledger2.update_many_sync(submit_records)
    
    def _get_up_to_date_batch(self,input_batch:Dict[str,Entry])->Dict[str, Entry]:
        """
        Get entries in the cache that
        1. only output entries have the same idx in input_batch
        2. have the latest revision in the cache, and its rev >= input_batch[idx].rev
        3. sorted in the order given by input_batch
        """
        newest_entries = {}
        for idx,input_entry in input_batch.items():
            if not self._contains(idx, input_batch[idx].rev):
                continue
            cached_entry = self._get_entry(idx, input_batch[idx].rev)
            if cached_entry.rev < input_entry.rev:
                continue
            if idx in newest_entries and cached_entry.rev < newest_entries[idx].rev:
                continue
            newest_entries[idx] = cached_entry
        return newest_entries
    
    def pump(self, inputs: Dict[int, Dict[str, Entry]], options: PumpOptions) -> PumpOutput:
        input_batch = inputs.get(0, {})
        if input_batch:
            for entry in input_batch.values():
                self.prepare_input(entry)
            self._deposit_batch(input_batch)

        cached_batch = self._get_up_to_date_batch(input_batch)
        self.process_cached_batch(cached_batch, options)
        cached_batch = self._get_up_to_date_batch(input_batch)

        outputs,consumed,did_emit={0:{}}, {0:set()}, False
        for idx,entry in cached_batch.items():
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
            return self._ledger2.get_one(f"{idx}_{rev}", builder=self._build_entry)
        else:
            return self._ledger2.get_one(idx, builder=self._build_entry)
    
    def _contains(self, idx: str, rev: int)->bool:
        if self.keep_all_rev:
            return self._ledger2.contains(f"{idx}_{rev}")
        else:
            return self._ledger2.contains(idx)

    def _serialize_entry(self,entry:Entry)->Dict[str, Any]:
        record = asdict(entry)
        if self.keep_all_rev:
            record['idx'] = f"{entry.idx}_{entry.rev}"
        return record
    def _build_entry(self, record: Dict[str, Any]) -> Entry:
        if self.keep_all_rev and '_' in record['idx']:
            record['idx'],_ = record['idx'].rsplit('_',1)
        return Entry(**record)

@show_in_op_list
class CheckPoint(CheckpointOp):
    """
    A no-op checkpoint that saves inputs to the cache, and resumes from the cache.
    """
    def __init__(self, cache_path: str = None,*, keep_all_rev: bool = True, barrier_level: int = 1):
        super().__init__(cache_path, keep_all_rev=keep_all_rev, barrier_level=barrier_level)
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
        