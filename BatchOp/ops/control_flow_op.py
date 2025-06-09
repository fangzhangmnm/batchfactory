from ..core import AtomicOp, Entry, MergeOp, SplitOp, BaseOp
from ..lib.utils import _to_list_2, FieldsRouter, _pick_field_or_value_strict

from typing import List, Tuple, Dict, Callable, TYPE_CHECKING
from copy import deepcopy
from abc import ABC, abstractmethod

if TYPE_CHECKING:
    from ..core.op_graph_segment import OpGraphSegment


class DuplicateOp(SplitOp):
    def __init__(self, n_outputs:int = 2, replica_idx_field:str|None="replica_idx"):
        super().__init__(n_outputs=n_outputs)
        self.replica_idx_field = replica_idx_field
    def route(self, entry: Entry) -> Dict[int, Entry]:
        output_entries = {}
        for i in range(self.n_outputs):
            new_entry = deepcopy(entry)
            if self.replica_idx_field:
                new_entry.data[self.replica_idx_field] = entry.idx
            output_entries[i] = new_entry
        return output_entries

class JoinOp(MergeOp):
    """
    if an entry passed through port 1, its rev increases by 1
    if its increased rev is greater than the one in port 0, it overrides the old one
    entry0's rev dont increase
    """
    def __init__(self):
        super().__init__(n_inputs=2,allow_missing=True)
    def merge(self, entries: Dict[int,Entry]) -> Entry|None:
        entry0,entry1=entries.get(0),entries.get(1)
        if entry1 is not None:
            entry1.rev +=1
            if entry0 is None or entry1.rev > entry0.rev:
                return entry1
        elif entry0 is not None:
            return entry0
        return None

class BaseBranchOp(SplitOp,ABC):
    """
    if criteria is satisfied. route to port 1, otherwise to port 0
    """
    def __init__(self):
        super().__init__(n_outputs=2)
    def route(self, entry: Entry) -> Dict[int, Entry]:
        if self.criteria(entry):
            return {1: entry}
        else:
            return {0: entry}
    @abstractmethod
    def criteria(self, entry:Entry)->bool:
        """Override this method to define the criteria for routing to loop branch.
        You might also modify the entry in this method like entry.data['loop_count']+=1
        Please avoid using entry.rev for loop counting. rev is for internal tracking of entry revisions"""
        pass

class BranchOp(BaseBranchOp):
    """
    if criteria is satisfied. route to port 1, otherwise to port 0
    """
    def __init__(self, criteria: Callable[[Dict], bool]):
        super().__init__()
        self.criteria_fn = criteria
    def criteria(self, entry: Entry) -> bool:
        return self.criteria_fn(entry.data)
    
class CounterOp(BaseBranchOp):
    """if count_field < max_count increment count_field by 1 and route to port 1, otherwise route to port 0"""
    def __init__(self, count_field="rounds",max_count=None,max_count_field=None,default=0):
        super().__init__()
        self.count_field = count_field
        self.default = default
        self.max_count = max_count
        self.max_count_field = max_count_field
        if max_count_field is None and max_count is None:
            raise ValueError("Either max_count or max_count_field must be provided")
    def criteria(self, entry: Entry) -> bool:
        count = entry.data.get(self.count_field, self.default)
        if count < _pick_field_or_value_strict(entry.data, self.max_count_field, self.max_count):
            entry.data[self.count_field] = count + 1
            return True
        else:
            return False
        
# Let's do some ASM coding!

def If(criteria:Callable[[Dict],bool],true_chain:'OpGraphSegment|BaseOp|None',false_chain=None)->'OpGraphSegment':
    branch,join=BranchOp(criteria),JoinOp()
    if false_chain is not None: 
        main_chain = branch | false_chain | join
    else:
        main_chain = branch | join
    if true_chain is not None:
        main_chain.wire(branch, true_chain, 1, 0)
        main_chain.wire(true_chain, join, 0, 1)
    else:
        main_chain.wire(branch, join, 1, 1)
    return main_chain
        
def While(criteria:Callable[[Dict],bool],body_chain:'OpGraphSegment|BaseOp')->'OpGraphSegment':
    branch,join=BranchOp(criteria),JoinOp()
    main_chain = join | branch
    main_chain.wire(branch, body_chain, 1, 0)
    main_chain.wire(body_chain, join, 0, 1)
    return main_chain

def For(count_field:str, max_count:int|None,body_chain:'OpGraphSegment|BaseOp',max_count_field=None,default=0)->'OpGraphSegment':
    branch = CounterOp(count_field=count_field, 
                       max_count=max_count, 
                       max_count_field=max_count_field,
                       default=default)
    join = JoinOp()
    main_chain = join | branch
    main_chain.wire(branch, body_chain, 1, 0)
    main_chain.wire(body_chain, join, 0, 1)
    return main_chain

__all__= [
    "DuplicateOp",
    "JoinOp",
    "BranchOp",
    "CounterOp",
    "If",
    "While",
    "For",
]