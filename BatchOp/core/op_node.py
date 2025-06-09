from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from typing import Union, List, Any, Tuple, Iterator, TYPE_CHECKING, Dict
from .entry import Entry
from ..lib.utils import _make_list_of_list
if TYPE_CHECKING:
    from .op_graph_segment import OpGraphSegment



class BaseOp(ABC):
    def __init__(self,n_inputs:int=1,n_outputs:int=1,consume_all_batch:bool=False):
        self.n_inputs= n_inputs
        self.n_outputs = n_outputs
        self.consume_all_batch = consume_all_batch
    def resume(self):
        pass
    def __repr__(self):
        return f"{self.__class__.__name__}()"
    def to_segment(self) -> 'OpGraphSegment':
        from .op_graph_segment import OpGraphSegment
        return OpGraphSegment.make_seg(self)
    def __or__(self,other)->'OpGraphSegment':
        return self.to_segment() | other


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

    
class MergeOp(BaseOp, ABC):
    """
    Used for merging multiple versions of the entry with same idx into one.
    Used for voting, looping, etc
    Do not guarantee entry immutability
        allow_missing: if False, an entry with the same idx will be created only if all inputs are present.
    """
    def __init__(self,n_inputs:int,allow_missing:bool):
        super().__init__(n_inputs=n_inputs)
        self.allow_missing = allow_missing
    @abstractmethod
    def merge(self, entries: Dict[int,Entry]) -> Entry|None:
        "Merge entries taken from different inputs with the same idx into one entry."
        pass
    
class SplitOp(BaseOp, ABC):
    """
    Used for routing an entry to different outputs legs based on some condition.
    Used for conditional processing, looping, etc.
    Do not guarantee entry immutability
    """
    def __init__(self, n_outputs: int):
        super().__init__(n_outputs=n_outputs)
    @abstractmethod
    def route(self, entry: Entry) -> Dict[int, Entry]:
        """Route an entry to different outputs based on some condition.
        returns (output_leg_index, entry) tuples."""
        pass

class InputOp(BaseOp, ABC):
    """
    Used for generating new entries based on some input.
    Used for loading dataset, generating rng, etc
    """
    def __init__(self,fire_once=True):
        super().__init__(n_inputs=0)
        self.fire_once = fire_once
    @abstractmethod
    def generate_batch(self)-> Dict[str,Entry]:
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
    def output_batch(self,entries:Dict[str,Entry])->None:
        pass

class BatchOp(BaseOp):
    """
    Doing operation on batch level
    e.g. sampling, shuffling, cross-talk, etc
    """
    def __init__(self,consume_all_batch:bool):
        """
        consume_all_batch: if True, the operation consumes all entries in the batch, no matter how many entries it processes.
        """
        super().__init__(consume_all_batch=consume_all_batch)
    @abstractmethod
    def update_batch(self, entries: Dict[str,Entry])->Dict[str,Entry]:
        """Process a batch of entries and return a processed batch."""
        pass

    

        

    







__all__ = [
    'BaseOp',
    'AtomicOp',
    'MergeOp',
    'SplitOp',
    'InputOp',
    'OutputOp',
    'BatchOp'
]