from ..core.base_op import *
from ..core.entry import Entry
from ..lib.utils import hash_json, KeysUtil, CollectionsUtil, ReprUtil


from typing import List, Tuple, Dict, Callable, TYPE_CHECKING
from copy import deepcopy
from abc import ABC, abstractmethod

if TYPE_CHECKING:
    from ..core.op_graph import Graph

class Replicate(SplitOp):
    "Replicate an entry to all output ports."
    def __init__(self, n_out_ports:int = 2, replica_idx_key:str|None="replica_idx"):
        super().__init__(n_out_ports=n_out_ports)
        self.replica_idx_key = replica_idx_key
    def split(self, entry: Entry) -> Dict[int, Entry]:
        output_entries = {}
        for i in range(self.n_out_ports):
            new_entry = deepcopy(entry)
            if self.replica_idx_key:
                new_entry.data[self.replica_idx_key] = i
            output_entries[i] = new_entry
        return output_entries
    
class Collect(MergeOp):
    "Collect data from port 1, merge to 0."
    def __init__(self, *keys):
        """
        - collects data from in_port 1
        - `Collect('key1', 'key2')` 
        """
        super().__init__(n_in_ports=2, wait_all=True)
        self.keys = KeysUtil.make_keys(*keys, allow_empty=False)
    def _args_repr(self): return ReprUtil.repr_keys(self.keys)
    def merge(self, entries: Dict[int, Entry]) -> Entry:
        for key in self.keys:
            if key in entries[1].data:
                entries[0].data[key] = entries[1].data[key]
        return entries[0]


class BeginIfOp(SplitOp,ABC):
    "Switch to port 1 if criteria is met."
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
    Switch to port 1 if criteria is met. See `If` function for usage.
    - `BeginIf(lambda data: data['condition'])`
    - `BeginIf(lambda x, y: x > y, 'x', 'y')`
    """
    def __init__(self,criteria:Callable,*keys):
        super().__init__()
        self._criteria = criteria
        self.criteria_keys = KeysUtil.make_keys(*keys) if keys else None
    def _args_repr(self): return ReprUtil.repr_lambda(self._criteria)
    def criteria(self, entry: Entry) -> bool:
        if self.criteria_keys is not None:
            return self._criteria(*KeysUtil.read_dict(entry.data, self.criteria_keys))
        else:
            return self._criteria(entry.data)

class EndIf(MergeOp):
    "Join entries from either port 0 or port 1. See `If` function for usage."
    def __init__(self):
        super().__init__(n_in_ports=2, wait_all=False)
    def merge(self, entries: Dict[int, Entry]) -> Entry:
        if len(entries)>1:
            raise ValueError("Entries with same idx comes from both branch of If")
        entry = next(iter(entries.values()))
        # should not increase rev here
        return entry
    
def If(criteria:Callable, true_chain:'Graph|BaseOp|None', false_chain=None) -> 'Graph':
    """
    Switch to true_chain if criteria is met, otherwise stay on false_chain.
    - `If(lambda data: data['condition'], true_chain, false_chain)`
    """
    begin = BeginIf(criteria)
    end = EndIf()
    if false_chain is not None: 
        main_chain = begin | false_chain | end
    else:
        main_chain = begin | end
    if true_chain is not None:
        true_chain = true_chain.to_graph()
        main_chain.wire(begin, true_chain, 1, 0)
        main_chain.wire(true_chain, end, 0, 1)
    else:
        main_chain.wire(begin, end, 1, 1)
    return main_chain
If._show_in_op_list = True

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
    "Executes the loop body while the criteria is met. See `While` function for usage."
    def __init__(self, criteria, *criteria_keys):
        super().__init__()
        self._criteria = criteria
        self.criteria_keys = KeysUtil.make_keys(*criteria_keys) if criteria_keys else None
    def _args_repr(self): return ReprUtil.repr_lambda(self._criteria)
    def criteria(self, entry: Entry) -> bool:
        if self.criteria_keys is not None:
            return self._criteria(*self.criteria_keys.read_keys(entry.data))
        else:
            return self._criteria(entry.data)
        
def While(criteria:Callable, body_chain:'Graph|BaseOp') -> 'Graph':
    """
    Executes the loop body while the criteria is met.
    - `While(lambda data: data['condition'], loop_body)`
    """
    loop_nome = WhileNode(criteria)
    main_chain = loop_nome.to_graph()
    body_chain = body_chain.to_graph()
    main_chain.wire(loop_nome, body_chain, 1, 0)
    main_chain.wire(body_chain, loop_nome, 0, 1)
    return main_chain
While._show_in_op_list = True

class RepeatNode(LoopOp):
    "Repeat the loop body for a fixed number of rounds. See `Repeat` function for usage."
    def __init__(self, max_rounds=None, rounds_key="rounds", max_rounds_key=None, initial_value:int|None=0):
        super().__init__()
        self.rounds_key = rounds_key
        self.initial_value:int|None = initial_value
        self.max_rounds = max_rounds
        self.max_rounds_key = max_rounds_key
        if max_rounds is None and max_rounds_key is None:
            raise ValueError("Either max_rounds or max_rounds_key must be provided.")
    def _args_repr(self): return self.max_rounds_key or f"{self.max_rounds}"
    def initialize(self, entry: Entry) -> None:
        if self.initial_value is not None:
            entry.data[self.rounds_key] = self.initial_value
        else:
            if self.rounds_key not in entry.data:
                raise ValueError(f"Entry does not have field '{self.rounds_key}' to initialize the loop count.")
    def criteria(self, entry: Entry) -> bool:
        max_rounds = _get_field_or_value(entry.data, self.max_rounds_key, self.max_rounds)
        finished_rounds = entry.data[self.rounds_key]
        return finished_rounds < max_rounds
    def pre_increment(self, entry: Entry) -> None:
        # note we use pre_increment instead of post_increment here
        entry.data[self.rounds_key] += 1


def Repeat(body_chain:'Graph|BaseOp', 
           max_rounds=None, rounds_key="rounds", max_rounds_key=None, initial_value:int|None=0):
    """
    Repeat the loop body for a fixed number of rounds.
    - `Repeat(loop_body,5,"rounds")`
    - `Repeat(loop_body,max_rounds_key='max_rounds')`
    - Note the subtle difference compared to `for` clause in c language:
        - **rounds represents how many times it enters the loop body**
        - for example, `Repeat(loop_body,5)` results in `rounds` being 1,2,3,4,5 in loop body, and 5 after exiting the loop
    - If `initial_value` is set to None, it will fetch the initial value from `rounds_key`
    """
    node = RepeatNode(max_rounds=max_rounds, rounds_key=rounds_key, 
                      max_rounds_key=max_rounds_key, initial_value=initial_value)
    main_chain = node.to_graph()
    body_chain = body_chain.to_graph()
    main_chain.wire(node, body_chain, 1, 0)
    main_chain.wire(body_chain, node, 0, 1)
    return main_chain
Repeat._show_in_op_list = True




def _explode_entry_by_lists(
                    entry: Entry, 
                    in_keys:List[str], out_keys:List[str],
                    master_idx_key: str | None = None,
                    list_idx_key: str | None = None,
                    keep_others: bool = None,
                    ) -> Dict[str, Entry]:
    """Explode an entry to multiple entries based on the router."""
    if keep_others is None: raise ValueError("keep_others must be specified")
    in_keys,out_keys = KeysUtil.make_keys_map(in_keys, out_keys)
    output_entries = {}
    in_lists = KeysUtil.read_dict(entry.data, in_keys)
    in_lists = CollectionsUtil.broadcast_lists(in_lists)
    in_tuples = CollectionsUtil.pivot_cascaded_list(in_lists)
    for list_idx,in_tuple in enumerate(in_tuples):
        new_data = {}
        if keep_others:
            for key in entry.data:
                if key not in in_keys and key not in out_keys:
                    new_data[key] = entry.data[key]
        KeysUtil.write_dict(new_data, out_keys, *in_tuple)
        if master_idx_key is not None:
            new_data[master_idx_key] = entry.idx
        if list_idx_key is not None:
            new_data[list_idx_key] = list_idx
        spawn_idx = hash_json(new_data)
        spawn_entry = Entry(idx=spawn_idx, data=deepcopy(new_data))
        output_entries[spawn_idx] = spawn_entry
    return output_entries


class ExplodeList(BatchOp):
    """
    Explode an entry to multiple entries based on a list (or lists).
    if keep_others == True, copy all other fields expect the in_lists_keys
    """
    def __init__(self, in_lists_keys="list", out_lists_keys="item",
                 master_idx_key="master_idx", list_idx_key="list_idx",
                 keep_others=True,
                 barrier_level=1):
        super().__init__(consume_all_batch=True, barrier_level=barrier_level)
        self.in_keys, self.out_keys = KeysUtil.make_keys_map(in_lists_keys, out_lists_keys)
        self.master_idx_key = master_idx_key
        self.list_idx_key = list_idx_key
        self.keep_others = keep_others
    def _args_repr(self): return ReprUtil.repr_dict_from_tuples(self.in_keys, self.out_keys)
    def update_batch(self, batch: Dict[str, Entry]) -> Dict[str, Entry]:
        output_entries = {}
        for entry in batch.values():
            output_entries.update(_explode_entry_by_lists(entry, 
                                                self.in_keys, self.out_keys,
                                                master_idx_key=self.master_idx_key,
                                                list_idx_key=self.list_idx_key,
                                                keep_others=self.keep_others
                                                ))
        return output_entries


class SpawnFromList(SpawnOp):
    "Spawn multiple spawn entries to port 1 based on a list (or lists)."
    def __init__(self,
                 in_lists_keys="list",
                 out_items_keys="item",
                 master_idx_key="master_idx",
                 list_idx_key="list_idx",
                 spawn_idx_list_key="spawn_idx_list",
    ):
        super().__init__()
        self.in_lists_keys, self.out_items_keys = KeysUtil.make_keys_map(in_lists_keys, out_items_keys)
        self.master_idx_key = master_idx_key
        self.list_idx_key = list_idx_key
        self.spawn_idx_list_key = spawn_idx_list_key
    def _args_repr(self): return ReprUtil.repr_dict_from_tuples(self.in_lists_keys, self.out_items_keys)
    def spawn_entries(self, entry: Entry) -> Dict[str, Entry]:
        """Entry->{new_idx:new_Entry}"""
        output_entries = _explode_entry_by_lists(entry, self.in_lists_keys, self.out_items_keys,
                                        master_idx_key=self.master_idx_key,
                                        list_idx_key=self.list_idx_key,
                                        keep_others=False 
                                        )
        if self.spawn_idx_list_key is not None:
            entry.data[self.spawn_idx_list_key] = list(output_entries.keys())
        return output_entries
            
class CollectAllToList(CollectAllOp):
    "Collect items from spawn entries on port 1 and merge them into a list (or lists if multiple items provided)."
    def __init__(self, 
                in_items_keys="item",
                out_lists_keys="list",
                master_idx_key="master_idx",
                list_idx_key="list_idx",
                spawn_idx_list_key="spawn_idx_list",
                consume_spawns=True
    ):
        super().__init__(consume_spawns=consume_spawns)
        self.in_items_keys, self.out_lists_keys = KeysUtil.make_keys_map(in_items_keys, out_lists_keys)
        self.master_idx_key = master_idx_key
        self.list_idx_key = list_idx_key
        self.spawn_idx_list_key = spawn_idx_list_key
    def _args_repr(self): return ReprUtil.repr_dict_from_tuples(self.in_items_keys, self.out_lists_keys)
    def get_master_idx(self, spawn: Entry)->str|None:
        return spawn.data[self.master_idx_key]
    def is_ready(self,master_entry: Entry, spawn_bundle:Dict[str,Entry]) -> bool:
        for spawn_idx in master_entry.data[self.spawn_idx_list_key]:
            if spawn_idx not in spawn_bundle:
                return False
        return True
    def update_master(self, master_entry: Entry, spawn_bundle: Dict[str, Entry])->None:
        zipped_output_lists = []
        for spawn_idx in master_entry.data[self.spawn_idx_list_key]:
            zipped_output_lists.append(KeysUtil.read_dict(spawn_bundle[spawn_idx].data, self.in_items_keys))
        KeysUtil.write_dict(master_entry.data, self.out_lists_keys, *zip(*zipped_output_lists))
        
def ListParallel(spawn_body:'Graph|BaseOp',
        in_lists_keys:str="list",
        out_items_keys:str|None="item",
        in_items_keys:str|None=None,
        out_lists_keys:str|None=None,
        master_idx_key="master_idx",
        list_idx_key="list_idx",
        spawn_idx_list_key="spawn_idx_list",
        master_body:'Graph|BaseOp|None'=None,
    ):
    "Spawn entries from a list (or lists), process them in parallel, and collect them back to a list (or lists)."
    if out_items_keys is None: 
        out_items_keys = in_lists_keys
    if in_items_keys is None:
        in_items_keys = out_items_keys
    if out_lists_keys is None:
        out_lists_keys = in_lists_keys
    Begin = SpawnFromList(
        in_lists_keys=in_lists_keys,
        out_items_keys=out_items_keys,
        master_idx_key=master_idx_key,
        list_idx_key=list_idx_key,
        spawn_idx_list_key=spawn_idx_list_key
    )
    End = CollectAllToList(
        in_items_keys=in_items_keys or out_items_keys,
        out_lists_keys=out_lists_keys or in_lists_keys,
        master_idx_key=master_idx_key,
        list_idx_key=list_idx_key,
        spawn_idx_list_key=spawn_idx_list_key
    )
    if master_body is not None:
        main_chain = Begin | master_body | End
    else:
        main_chain = Begin | End
    spawn_body = spawn_body.to_graph()
    main_chain.wire(Begin, spawn_body, 1, 0)
    main_chain.wire(spawn_body, End, 0, 1)
    return main_chain
ListParallel._show_in_op_list = True

        
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
    "ExplodeList",
    "SpawnFromList",
    "CollectAllToList",
    "ListParallel",
]
