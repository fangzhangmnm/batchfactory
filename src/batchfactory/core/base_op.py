from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from typing import Union, List, Any, Tuple, Iterator, TYPE_CHECKING, Dict, NamedTuple, Set
from .entry import Entry
from ..lib.utils import _make_list_of_list, ReprUtil
if TYPE_CHECKING:
    from .op_graph import Graph

class PumpOutput(NamedTuple):
    outputs:Dict[int,Dict[str,Entry]] # {out_port: {entry_idx: Entry}}
    consumed:Dict[int,Set[str]] # {in_port: set of consumed entry idxs}
    did_emit:bool # signaling if need to update the downstream nodes
    
class PumpOptions(NamedTuple):
    reload_inputs:bool=False
    dispatch_brokers:bool=False
    mock:bool=False
    max_barrier_level:int|None=None

class BaseOp(ABC):
    def __init__(self,*,n_in_ports:int,n_out_ports:int,barrier_level:int):
        self.n_in_ports= n_in_ports
        self.n_out_ports = n_out_ports
        self.barrier_level = barrier_level # if True, wait for all other ops of lower barrier level finish before pumping
        self._tag = None
    def reset(self):
        "Reset for stateful nodes"
        pass
    def resume(self):
        """Resume the state from cache"""
        pass
    @abstractmethod
    def pump(self,inputs:Dict[int,Dict[str,Entry]],options:PumpOptions) -> PumpOutput:
        """
        Take some entries from in_ports, send some entries to out_ports.
        Checking barrier_level is upper level executer's responsibility
        """
        pass
    def to_graph(self) -> 'Graph':
        from .op_graph import OpGraphConnector
        return OpGraphConnector.make_graph(self)
    def __or__(self,other)->'Graph':
        return self.to_graph() | other
    def __repr__(self):
        if self._tag: return self._tag
        else: return f"{self.__class__.__name__}({self._args_repr()})"
    def _args_repr(self):
        return ""
    def tag(self,tag:str)->'BaseOp':
        if not isinstance(tag,str) or not tag: raise ValueError("tag must be a non-empty string")
        self._tag = tag
        return self

class ApplyOp(BaseOp, ABC):
    "Modifies entries in-place; maps each idx → same idx."
    def __init__(self):
        super().__init__(n_in_ports=1, n_out_ports=1, barrier_level=0)
    @abstractmethod
    def update(self, entry: Entry)->None:
        "Update every entry in-place"
        pass
    def pump(self,inputs,options:PumpOptions) -> PumpOutput:
        outputs,consumed,did_emit={0:{}}, {0:set()}, False
        for entry in inputs.get(0,{}).values():
            self.update(entry)
            outputs[0][entry.idx] = entry
            consumed[0].add(entry.idx)
            did_emit = True
        return PumpOutput(outputs=outputs, consumed=consumed, did_emit=did_emit)

class FilterOp(BaseOp, ABC):
    "Drops some entries, keeps others unchanged."
    def __init__(self,*,consume_rejected:bool):
        super().__init__(n_in_ports=1, n_out_ports=1, barrier_level=0)
        self.consume_rejected = consume_rejected
    @abstractmethod
    def criteria(self, entry: Entry) -> bool:
        pass
    def pump(self,inputs,options:PumpOptions) -> PumpOutput:
        outputs,consumed,did_emit={0:{}}, {0:set()}, False
        for entry in inputs.get(0,{}).values():
            if self.criteria(entry):
                outputs[0][entry.idx] = entry
                consumed[0].add(entry.idx)
                did_emit = True
            elif self.consume_rejected:
                consumed[0].add(entry.idx)
        return PumpOutput(outputs=outputs, consumed=consumed, did_emit=did_emit)
    
class RouterOp(BaseOp, ABC):
    "Merges entries of same idx from N ports, then duplicate or route them to M out ports."
    def __init__(self,*, n_in_ports: int, n_out_ports: int, wait_all:bool):
        super().__init__(n_in_ports=n_in_ports, n_out_ports=n_out_ports, barrier_level=0)
        self.wait_all = wait_all
    @abstractmethod
    def route(self, bundle: Dict[int, Entry]) -> Dict[int, Entry]:
        "{in_port_idx:Entry} -> {out_port_idx:Entry}"
        pass
    def pump(self, inputs, options: PumpOptions) -> PumpOutput:
        outputs = {i: {} for i in range(self.n_out_ports)}
        consumed = {i: set() for i in range(self.n_in_ports)}
        did_emit = False
        bundles = {}
        for in_port, batch in inputs.items():
            for entry in batch.values():
                bundles.setdefault(entry.idx, {})[in_port] = entry
        for idx, in_bundle in bundles.items():
            if self.wait_all and len(in_bundle) < self.n_in_ports:
                continue
            out_bundle = self.route(in_bundle)
            for out_port, entry in out_bundle.items():
                outputs[out_port][entry.idx] = entry
            for in_port in in_bundle.keys():
                consumed[in_port].add(idx)
            did_emit = True
        return PumpOutput(outputs=outputs, consumed=consumed, did_emit=did_emit)

class MergeOp(RouterOp, ABC):
    "Merges entries of same idx from N ports to 1"
    def __init__(self,*,n_in_ports:int,wait_all:bool):
        super().__init__(n_in_ports=n_in_ports, n_out_ports=1, wait_all=wait_all)
    @abstractmethod
    def merge(self, bundle: Dict[int,Entry]) -> Entry:
        "input:{in_port_idx:Entry}"
        pass
    def route(self, bundle):
        return {0: self.merge(bundle)}
    
class SplitOp(RouterOp, ABC):
    "Splits an entry to multiple out_ports"
    def __init__(self,*, n_out_ports: int):
        super().__init__(n_in_ports=1, n_out_ports=n_out_ports, wait_all=True)
    @abstractmethod
    def split(self, entry: Entry) -> Dict[int, Entry]:
        "output:{out_port_idx:Entry}"
        pass
    def route(self, bundle):
        return self.split(bundle[0])

class SourceOp(BaseOp, ABC):
    """	Generates entries (reads datasets, dice rolls, etc)."""
    def __init__(self,*,fire_once:bool):
        super().__init__(n_in_ports=0, n_out_ports=1, barrier_level=0)
        self.fire_once = fire_once
    @abstractmethod
    def generate_batch(self)-> Dict[str, Entry]:
        "output:{idx:Entry}"
        pass
    def pump(self, inputs, options: PumpOptions) -> PumpOutput:
        outputs, consumed, did_emit = {0:{}}, {0:set()}, False
        if self.fire_once and not options.reload_inputs:
            return PumpOutput(outputs=outputs, consumed=consumed, did_emit=did_emit)
        batch = self.generate_batch()
        for entry in batch.values():
            outputs[0][entry.idx] = entry
            consumed[0].add(entry.idx)
            did_emit = True
        return PumpOutput(outputs=outputs, consumed=consumed, did_emit=did_emit)
    
class BatchOp(BaseOp, ABC):
    "Batch-level shuffle/crosstalk, might drop or insert entries of different idxs."
    def __init__(self,*,consume_all_batch:bool, barrier_level:int):
        "consume_all_batch: consume all entries or only ones output by update_batch"
        if barrier_level<1: raise ValueError("barrier_level of BatchOp must be at least 1")
        super().__init__(n_in_ports=1, n_out_ports=1, barrier_level=barrier_level)
        self.consume_all_batch = consume_all_batch
    @abstractmethod
    def update_batch(self, batch: Dict[str, Entry]) -> Dict[str, Entry]:
        "{idx:Entry} -> {idx:Entry}"
        pass
    def pump(self, inputs, options: PumpOptions) -> PumpOutput:
        outputs, consumed, did_emit = {0:{}}, {0:set()}, False
        input_batch = inputs.get(0, {})
        if self.consume_all_batch:
            consumed[0].update(input_batch.keys())
        for entry in self.update_batch(input_batch).values():
            outputs[0][entry.idx] = entry
            consumed[0].add(entry.idx)
            did_emit = True
        return PumpOutput(outputs=outputs, consumed=consumed, did_emit=did_emit)
    
class OutputOp(BatchOp, ABC):
    "Outputs a batch to disk/console/etc, then sends it to the next node unmodified."
    def __init__(self,*,barrier_level=1):
        super().__init__(consume_all_batch=True, barrier_level=barrier_level)
    @abstractmethod
    def output_batch(self, batch: Dict[str, Entry]) -> None:
        pass
    def update_batch(self, batch: Dict[str, Entry]) -> Dict[str, Entry]:
        if not batch: return {}
        self.output_batch(batch)
        return {**batch}

class SpawnOp(BaseOp, ABC):
    "Create spawn entries on out_port 1, keep master entry unchanged."
    def __init__(self):
        super().__init__(n_in_ports=1, n_out_ports=2, barrier_level=0)
    @abstractmethod
    def spawn_entries(self, entry: Entry) -> Dict[str, Entry]:
        """Entry->{new_idx:new_Entry}"""
        pass
    def pump(self, inputs, options: PumpOptions) -> PumpOutput:
        outputs, consumed, did_emit = {0:{}, 1:{}}, {0:set()}, False
        for entry in inputs.get(0,{}).values():
            spawn_batch = self.spawn_entries(entry)
            for spawn_idx, spawn_entry in spawn_batch.items():
                outputs[1][spawn_idx] = spawn_entry
            outputs[0][entry.idx] = entry
            consumed[0].add(entry.idx)
            did_emit = True
        return PumpOutput(outputs=outputs, consumed=consumed, did_emit=did_emit)


class CollectAllOp(BaseOp, ABC):
    "Update master entry from spawn entries collected from in_port 1. Wait if spawn entries are not ready."
    def __init__(self,*,consume_spawns:bool):
        n_out_ports = 1 if consume_spawns else 2
        super().__init__(n_in_ports=2, n_out_ports=n_out_ports, barrier_level=0)
        self.consume_spawns = consume_spawns

    @abstractmethod
    def get_master_idx(self, spawn_entry: Entry)->str|None:
        "get the idx of the master entry"
        pass
    @abstractmethod
    def is_ready(self,master_entry: Entry, spawn_bundle:Dict[str,Entry]) -> bool:
        "Check if all spawn entries had arrived."
        pass
    @abstractmethod
    def update_master(self, master_entry: Entry, spawn_bundle: Dict[str, Entry])->None:
        "Update master entry in-place from spawn entries."
        pass
    def pump(self, inputs, options: PumpOptions) -> PumpOutput:
        outputs, consumed, did_emit = {0:{}}, {0:set(), 1:set()}, False
        if not self.consume_spawns:
            outputs[1] = {}
        master_batch = inputs.get(0, {})
        candidate_batch = inputs.get(1, {})
        spawn_batches = {}
        for spawn_entry in candidate_batch.values():
            master_idx = self.get_master_idx(spawn_entry)
            if master_idx is None:
                continue
            spawn_batches.setdefault(master_idx, {})[spawn_entry.idx] = spawn_entry
        for master_idx, master_entry in master_batch.items():
            spawn_entries = spawn_batches.get(master_idx, {})
            if not self.is_ready(master_entry, spawn_entries):
                continue
            self.update_master(master_entry, spawn_entries)
            outputs[0][master_idx] = master_entry
            consumed[0].add(master_idx)
            consumed[1].update(spawn_entries.keys())
            if not self.consume_spawns:
                for spawn_entry in spawn_entries.values():
                    outputs[1][spawn_entry.idx] = spawn_entry
            did_emit = True
        return PumpOutput(outputs=outputs, consumed=consumed, did_emit=did_emit)


__all__ = [
    "PumpOutput",
    "PumpOptions",
    "BaseOp",
    "ApplyOp",
    "FilterOp",
    "RouterOp",
    "MergeOp",
    "SplitOp",
    "SourceOp",
    "BatchOp",
    "OutputOp",
    "SpawnOp",
    "CollectAllOp",
]