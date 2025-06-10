from ..core.op_base import *
from ..core.entry import Entry
from ..lib.utils import FieldsRouter, hash_json


from typing import List, Tuple, Dict, Callable, TYPE_CHECKING
from copy import deepcopy
from abc import ABC, abstractmethod

if TYPE_CHECKING:
    from ..core.op_graph_segment import OpGraphSegment

class Replicate(SplitOp):
    "Replicate an entry to all out_ports, with optionally `replica_idx_field` field set to the out_port index."
    def __init__(self, n_out_ports:int = 2, replica_idx_field:str|None="replica_idx"):
        super().__init__(n_out_ports=n_out_ports)
        self.replica_idx_field = replica_idx_field
    def split(self, entry: Entry) -> Dict[int, Entry]:
        output_entries = {}
        for i in range(self.n_out_ports):
            new_entry = deepcopy(entry)
            if self.replica_idx_field:
                new_entry.data[self.replica_idx_field] = i
            output_entries[i] = new_entry
        return output_entries
    
class Collect(MergeOp):
    "Collect data from in_port 1"
    def __init__(self, *fields):
        """
        - collects data from in_port 1
        - `Collect('field1', 'field2')` 
        """
        super().__init__(n_in_ports=2, wait_all=True)
        self.field_mappings = FieldsRouter(*fields, style="tuple")
    def merge(self, entries: Dict[int, Entry]) -> Entry:
        for field in self.field_mappings.froms:
            if field in entries[1].data:
                entries[0].data[field] = entries[1].data[field]
        return entries[0]


class BeginIfOp(SplitOp,ABC):
    "Switch to port 1 if criteria is met"
    def __init__(self):
        super().__init__(n_out_ports=2)
    def split(self, entry: Entry) -> Dict[int, Entry]:
        if self.criteria(entry):
            return {1: entry}
        else:
            return {0: entry}
    @abstractmethod
    def criteria(self, entry: Entry) -> bool:
        "if true, switch to port 1"
        pass

class BeginIf(BeginIfOp):
    """
    - `BeginIf(lambda data: data['condition'])`
    - `BeginIf(lambda x, y: x > y, 'x', 'y')`
    """
    def __init__(self,criteria:Callable,*fields):
        super().__init__()
        self._criteria = criteria
        self.router = FieldsRouter(*fields, style="tuple") if fields else None
    def criteria(self, entry: Entry) -> bool:
        if self.router:
            return self._criteria(*self.router.read_tuple(entry.data))
        else:
            return self._criteria(entry.data)
    
class EndIf(MergeOp):
    "Take entry from either branch of if"
    def __init__(self):
        super().__init__(n_in_ports=2, wait_all=False)
    def merge(self, entries: Dict[int, Entry]) -> Entry:
        if len(entries)>1:
            raise ValueError("Entries with same idx comes from both branch of If")
        entry = next(iter(entries.values()))
        # should not increase rev here
        return entry
    
def If(criteria:Callable, true_chain:'OpGraphSegment|BaseOp|None', false_chain=None) -> 'OpGraphSegment':
    """
    - `If(lambda data: data['condition'], true_chain, false_chain)`
    """
    from ..core.op_graph_segment import OpGraphSegment
    begin = BeginIf(criteria)
    end = EndIf()
    if false_chain is not None: 
        main_chain = begin | false_chain | end
    else:
        main_chain = begin | end
    if true_chain is not None:
        true_chain = OpGraphSegment.make_seg(true_chain)
        main_chain.wire(begin, true_chain, 1, 0)
        main_chain.wire(true_chain, end, 0, 1)
    else:
        main_chain.wire(begin, end, 1, 1)
    return main_chain

class LoopOp(RouterOp,ABC):
    """
    - please connect loop body from out_port 1 to in_port 1
    - choose the entry with higher rev. if rev is equal, choose from in_port 1
    - then increase rev by 1, and call corresponding methods (see code for detail of execution order)
    - then route the entry to out_port 0 (exit loop) or out_port 1 (continue loop) based on `criteria`
    """
    def __init__(self):
        super().__init__(n_in_ports=2, n_out_ports=2, wait_all=False)

    def route(self, bundle: Dict[int, Entry]) -> Dict[int, Entry]:
        in_port, entry = self._pick(bundle)

        if in_port == 0: self.initialize(entry)
        if in_port == 1: self.post_increment(entry)
        if in_port == 1: entry.rev += 1
        out_port = 1 if self.criteria(entry) else 0
        if out_port == 0: self.finalize(entry)
        elif out_port == 1: self.pre_increment(entry)

        return {out_port: entry}

    def _pick(self, bundle)-> Tuple[int, Entry]:
        entry0, entry1 = bundle.get(0), bundle.get(1)
        if entry1 is not None:
            if entry0 is None or entry1.rev >= entry0.rev: # we haven't increase entry1.rev yet. so entry1 have priority
                return 1, entry1
        if entry0 is not None:
            return 0, entry0
        raise ValueError("Should not pass an empty bundle to an RouterOp")

    @abstractmethod
    def criteria(self, entry: Entry) -> bool:
        "if true, route to loop branch (out_port 1)\nDo not touch rev here"
        pass
    def initialize(self, entry: Entry) -> None:
        "initialize the entry when it enters the loop entrance (coming from in_port 0)\nDo not touch rev here"
        pass
    def pre_increment(self, entry: Entry) -> None:
        "updates the entry each time it enters the loop body (exiting towards out_port 1)\nDo not touch rev here"
        pass
    def post_increment(self, entry: Entry) -> None:
        "updates the entry each time it leaves the loop body (coming from in_port 1)\nDo not touch rev here"
        pass
    def finalize(self, entry: Entry) -> None:
        "updates the entry when it exits the loop (exiting towards out_port 0)\nDo not touch rev here"
        pass

class WhileNode(LoopOp):
    "Please see `While` function for usage."
    def __init__(self, criteria, *criteria_fields):
        super().__init__()
        self._criteria = criteria
        self.criteria_router = FieldsRouter(*criteria_fields, style="tuple") if criteria_fields else None
    def criteria(self, entry: Entry) -> bool:
        if self.criteria_router:
            return self._criteria(*self.criteria_router.read_tuple(entry.data))
        else:
            return self._criteria(entry.data)
        
def While(criteria:Callable, body_chain:'OpGraphSegment|BaseOp') -> 'OpGraphSegment':
    """
    - `While(lambda data: data['condition'], loop_body)`
    """
    from ..core.op_graph_segment import OpGraphSegment
    node = WhileNode(criteria)
    main_chain = OpGraphSegment.make_seg(node)
    body_chain = OpGraphSegment.make_seg(body_chain)
    main_chain.wire(node, body_chain, 1, 0)
    main_chain.wire(body_chain, node, 0, 1)
    return main_chain

class RepeatNode(LoopOp):
    "See `Repeat` function for usage."
    def __init__(self, max_rounds=None, rounds_field="rounds", max_rounds_field=None, initial_value:int|None=0):
        super().__init__()
        self.rounds_field = rounds_field
        self.initial_value:int|None = initial_value
        self.max_rounds = max_rounds
        self.max_rounds_field = max_rounds_field
        if max_rounds is None and max_rounds_field is None:
            raise ValueError("Either max_rounds or max_rounds_field must be provided.")
    def initialize(self, entry: Entry) -> None:
        if self.initial_value is not None:
            entry.data[self.rounds_field] = self.initial_value
        else:
            if self.rounds_field not in entry.data:
                raise ValueError(f"Entry does not have field '{self.rounds_field}' to initialize the loop count.")
    def criteria(self, entry: Entry) -> bool:
        max_rounds = _get_field_or_value(entry.data, self.max_rounds_field, self.max_rounds)
        finished_rounds = entry.data[self.rounds_field]
        return finished_rounds < max_rounds
    def pre_increment(self, entry: Entry) -> None:
        # note we use pre_increment instead of post_increment here
        entry.data[self.rounds_field] += 1


def Repeat(body_chain:'OpGraphSegment|BaseOp', 
           max_rounds=None, rounds_field="rounds", max_rounds_field=None, initial_value:int|None=0):
    """
    - `Repeat(loop_body,5,"rounds")`
    - `Repeat(loop_body,max_rounds_field='max_rounds')`
    - Note the subtle difference compared to `for` clause in c language:
        - **rounds represents how many times it enters the loop body**
        - for example, `Repeat(loop_body,5)` results in `rounds` being 1,2,3,4,5 in loop body, and 5 after exiting the loop
    - If `initial_value` is set to None, it will fetch the initial value from `rounds_field`
    """
    from ..core.op_graph_segment import OpGraphSegment
    node = RepeatNode(max_rounds=max_rounds, rounds_field=rounds_field, 
                      max_rounds_field=max_rounds_field, initial_value=initial_value)
    main_chain = OpGraphSegment.make_seg(node)
    body_chain = OpGraphSegment.make_seg(body_chain)
    main_chain.wire(node, body_chain, 1, 0)
    main_chain.wire(body_chain, node, 0, 1)
    return main_chain

class SpawnFromList(SpawnOp):
    "Explode a list to multiple entries, each with a single item from the list."
    def __init__(self,
                 list_field="list",
                 item_field="item",
                 master_idx_field="master_idx",
                 list_idx_field="list_idx",
                 spawn_idx_list_field="spawn_idx_list",
    ):
        super().__init__()
        self.list_field = list_field
        self.item_field = item_field
        self.master_idx_field = master_idx_field
        self.list_idx_field = list_idx_field
        self.spawn_idx_list_field = spawn_idx_list_field
    def spawn_entries(self, entry: Entry) -> Dict[str, Entry]:
        """Entry->{new_idx:new_Entry}"""
        items = entry.data.get(self.list_field, [])
        if not isinstance(items, list):
            raise ValueError(f"Field '{self.list_field}' is not a list.")
        output_entries = {}
        for list_idx, item in enumerate(items):
            new_data = {self.item_field: item}
            if self.master_idx_field is not None:
                new_data[self.master_idx_field] = entry.idx
            if self.list_idx_field is not None:
                new_data[self.list_idx_field] = list_idx
            spawn_idx = hash_json(new_data)
            spawn_entry = Entry(idx=spawn_idx, data=new_data)
            output_entries[spawn_idx] = spawn_entry
        if self.spawn_idx_list_field is not None:
            entry.data[self.spawn_idx_list_field] = list(output_entries.keys())
        return output_entries
            
class CollectAllToList(CollectAllOp):
    "Concentrate items from multiple entries into a list."
    def __init__(self, 
                item_field="item",
                list_field="list",
                master_idx_field="master_idx",
                list_idx_field="list_idx",
                spawn_idx_list_field="spawn_idx_list",
    ):
        super().__init__()
        self.list_field = list_field
        self.item_field = item_field
        self.master_idx_field = master_idx_field
        self.list_idx_field = list_idx_field
        self.spawn_idx_list_field = spawn_idx_list_field
    def get_master_idx(self, spawn: Entry)->str|None:
        return spawn.data[self.master_idx_field]
    def is_ready(self,master_entry: Entry, spawn_bundle:Dict[str,Entry]) -> bool:
        for spawn_idx in master_entry.data[self.spawn_idx_list_field]:
            if spawn_idx not in spawn_bundle:
                return False
        return True
    def update_master(self, master_entry: Entry, spawn_bundle: Dict[str, Entry])->None:
        items = [] 
        for spawn_idx in master_entry.data[self.spawn_idx_list_field]:
            item = spawn_bundle[spawn_idx].data[self.item_field]
            items.append(item)
        master_entry.data[self.list_field] = items

        






def _get_field_or_value(data,field,value):
    return value if field is None else data[field]

__all__ = [
    "Replicate",
    "Collect",
    "BeginIf",
    "EndIf",
    "If",
    "LoopOp",
    "WhileNode",
    "While",
    "RepeatNode",
    "Repeat",
    "SpawnFromList",
    "CollectAllToList",
]
