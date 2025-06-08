from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from typing import Union, List, Any, Tuple, Iterator, TYPE_CHECKING
from .entry import Entry
from ..lib.utils import _make_list_of_list
if TYPE_CHECKING:
    from .op_graph_segment import OpGraphSegment



class BaseOp(ABC):
    def __init__(self):
        pass
    def resume(self):
        pass
    def __repr__(self):
        return f"{self.__class__.__name__}()"
    def __or__(self,other)->'OpGraphSegment':
        from .op_graph_segment import OpGraphSegment
        return OpGraphSegment.make_seg(self)|other


class AtomicOp(BaseOp, ABC):
    """
    Used for cheap, reproducible operations 
        like prompt preparation, post processing, etc. 
    Suggests break a complex operation into atomic Ops for better reusability.
    Do not guarantee entry immutability
    """
    @abstractmethod
    def update(self, entry: Entry) -> Entry|None:
        "Takes an entry and updates it with the current operation's logic."
        pass
    def update_batch(self, entries: List[Entry]) -> List[Entry]:
        entries = list({e.idx: e for e in entries if e is not None}.values())
        entries = [self.update(e) for e in entries]
        entries = [e for e in entries if e is not None]
        return entries

    
class MergeOp(BaseOp, ABC):
    """
    Used for merging multiple versions of the entry with same idx into one.
    Used for voting, looping, etc
    Do not guarantee entry immutability
        need_all_inputs: if True, an entry with the same idx will be created only if all inputs are present.
    """
    def __init__(self,n_inputs:int):
        super().__init__()
        self.n_inputs = n_inputs
    @abstractmethod
    def merge(self, entries: List[Entry]) -> Entry|None:
        "Merge entries taken from different inputs with the same idx into one entry."
        pass
    def merge_batch(self, entries:List[List[Entry]]) -> List[Entry]:
        merged_inputs={}
        for port, port_entries in enumerate(entries):
            for entry in port_entries:
                if entry.idx not in merged_inputs:
                    merged_inputs[entry.idx] = {}
                merged_inputs[entry.idx][port] = entry
        output_entries = []
        for idx, port_entries in merged_inputs.items():
            if all(port in port_entries for port in range(self.n_inputs)):
                merged_entry = [port_entries[port] for port in range(self.n_inputs)]
                merged_entry = self.merge(merged_entry)
                if merged_entry is not None:
                    output_entries.append(merged_entry)
        return output_entries

    
class SplitOp(BaseOp, ABC):
    """
    Used for routing an entry to different outputs legs based on some condition.
    Used for conditional processing, looping, etc.
    Do not guarantee entry immutability
    """
    def __init__(self, n_outputs: int):
        super().__init__()
        self.n_outputs = n_outputs
    @abstractmethod
    def route(self, entry: Entry) -> List[Tuple[int, Entry]]:
        """Route an entry to different outputs based on some condition.
        returns (output_leg_index, entry) tuples."""
        pass
    def route_batch(self,entries: List[Entry]) -> List[List[Entry]]:
        output_entries = [[] for _ in range(self.n_outputs)]
        for entry in entries:
            routes = self.route(entry)
            for output_leg, routed_entry in routes:
                if routed_entry is not None:
                    output_entries[output_leg].append(routed_entry)
        return output_entries

class InputOp(BaseOp, ABC):
    """
    Used for generating new entries based on some input.
    Used for loading dataset, generating rng, etc
    """
    def __init__(self,fire_once=True):
        self.fire_once = fire_once
    @abstractmethod
    def generate_batch(self)-> List[Entry]:
        "Generate a list of entries based on some input."
        pass

class OutputOp(BaseOp, ABC):
    """
    Used for outputting the entries to some output.
    Used for saving dataset, printing, etc
    Need to resolve duplication in output dataset
    The data will be passed transprently to the next operation in the pipeline.
    """
    @abstractmethod
    def output_batch(self,entries:List[Entry])->None:
        pass


    

        

    







__all__ = [
    'BaseOp',
    'AtomicOp',
    'MergeOp',
    'SplitOp',
    'InputOp',
    'OutputOp',
]