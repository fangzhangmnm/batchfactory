from ..core.base_op import *
from ..lib.utils import KeysUtil, ReprUtil
from ..core import BrokerJobStatus, Entry
from ._registery import show_in_op_list

from typing import List,Dict, Callable, Any
import random

@show_in_op_list
class Filter(FilterOp):
    """
    Filter entries based on a custom criteria function.
    - `Filter(lambda data:data['keep_if_True'])`
    - `Filter(lambda x:x>5, 'score')`
    """
    def __init__(self,criteria:Callable,*keys,consume_rejected=False):
        super().__init__(consume_rejected=consume_rejected)
        self._criteria = criteria
        self.keys = KeysUtil.make_keys(*keys) if keys is not None else None
    def criteria(self, entry):
        if self.keys is not None:
            return self._criteria(*KeysUtil.read_dict(entry.data, self.keys))
        else:
            return self._criteria(entry.data)
    def _args_repr(self): return ReprUtil.repr_lambda(self._criteria)
        
@show_in_op_list
class FilterFailedEntries(FilterOp):
    """Drop entries that have a status "failed"."""
    def __init__(self,*,status_key="status",consume_rejected=False):
        super().__init__(consume_rejected=consume_rejected)
        self.status_key = status_key
    def _args_repr(self): return ReprUtil.repr_str(self.status_key)
    def criteria(self, entry):
        return BrokerJobStatus(entry.data[self.status_key]) != BrokerJobStatus.FAILED

@show_in_op_list
class FilterMissingFields(FilterOp):
    "Drop entries that do not have specific fields."
    def __init__(self, *keys, consume_rejected=False, allow_None=True):
        super().__init__(consume_rejected=consume_rejected)
        self.keys = KeysUtil.make_keys(*keys,allow_empty=False)
        self.allow_None = allow_None
    def _args_repr(self): return ReprUtil.repr_keys(self.keys)
    def criteria(self, entry):
        if self.allow_None:
            return all(field in entry.data for field in self.keys)
        else:
            return all(entry.data.get(field) is not None for field in self.keys)
    
@show_in_op_list
class Apply(ApplyOp):
    """
    Apply a function to modify the entry data.
    - `Apply(lambda data: operator.setitem(data, 'sum', data['a'] + data['b']))`
    """
    def __init__(self, func:Callable):
        super().__init__()
        self.func = func
    def _args_repr(self): return ReprUtil.repr_lambda(self.func)
    def update(self, entry:Entry)->None:
        self.func(entry.data)

@show_in_op_list
class MapField(ApplyOp):
    """
    Map a function to specific field(s) in the entry data.
    - `MapField(lambda x: x + 1, 'field')`
    - `MapField(lambda x, y: x + y, ['field1', 'field2'], ['result_field'])`
    """
    def __init__(self, func:Callable, *keys):
        super().__init__()
        self.func = func
        self.in_keys, self.out_keys = KeysUtil.make_io_keys(*keys)
    def _args_repr(self): return ReprUtil.repr_lambda(self.func)+":"+ReprUtil.repr_keys(self.in_keys)+"->"+ReprUtil.repr_keys(self.out_keys)
    def update(self, entry:Entry)->None:
        in_values = KeysUtil.read_dict(entry.data, self.in_keys)
        out_values = self.func(*in_values)
        out_values = KeysUtil.extract_out_list_from_func_return(out_values, self.out_keys)
        KeysUtil.write_dict(entry.data, self.out_keys, *out_values)

@show_in_op_list
class SetField(ApplyOp):
    """
    Set fields in the entry data to specific values.
    - `SetField('k1', v1, 'k2', v2, ...)`, see `MapInput` for details
    """
    def __init__(self, *data):
        super().__init__()
        self.data = KeysUtil.make_dict(*data)
    def _args_repr(self): return ReprUtil.repr_dict(self.data)
    def update(self, entry:Entry)->None:
        for field, value in self.data.items():
            entry.data[field] = value

@show_in_op_list
class RemoveField(ApplyOp):
    """
    Remove fields from the entry data.
    - `RemoveField('k1', 'k2', ...)`, see `KeysInput` for details
    """
    def __init__(self, *keys):
        super().__init__()
        self.keys = KeysUtil.make_keys(*keys, allow_empty=False)
    def _args_repr(self): return ReprUtil.repr_keys(self.keys)
    def update(self, entry:Entry)->None:
        for field in self.keys:
            entry.data.pop(field, None)
            
@show_in_op_list
class RenameField(ApplyOp):
    """
    Rename fields in the entry data.
    - `RenameField('from1', 'to1', 'from2', 'to2', ...)`, see `KeysMapInput` for details
    """
    def __init__(self, *keys_map, copy=False):
        super().__init__()
        self.from_keys, self.to_keys = KeysUtil.make_keys_map(*keys_map, non_overlapping=True)
        self.copy = copy
    def _args_repr(self): return ReprUtil.repr_dict_from_tuples(self.from_keys, self.to_keys)
    def update(self, entry:Entry)->None:
        for k1, k2 in zip(self.from_keys, self.to_keys):
            if self.copy:
                entry.data[k2] = entry.data.get(k1, None)
            else:
                entry.data[k2] = entry.data.pop(k1, None)

@show_in_op_list
class Shuffle(BatchOp):
    """Shuffle the entries in a batch randomly."""
    def __init__(self,*, seed, barrier_level = 1):
        super().__init__(consume_all_batch=True, barrier_level=barrier_level)
        self.seed = seed
    def update_batch(self, entries: Dict[str, Entry]) -> Dict[str, Entry]:
        entries_list = list(entries.values())
        rng = random.Random(self.seed)
        rng.shuffle(entries_list)
        entries = {entry.idx: entry for entry in entries_list}
        return entries
    
@show_in_op_list
class TakeFirstN(BatchOp):
    """Takes the first N entries from the batch. discards the rest."""
    def __init__(self, n: int,*, barrier_level = 1):
        super().__init__(consume_all_batch=True, barrier_level=barrier_level)
        self.n = n
    def _args_repr(self): return f"n={self.n}"
    def update_batch(self, entries: Dict[str, Entry]) -> Dict[str, Entry]:
        entries_list = list(entries.values())
        entries_list = entries_list[:self.n]
        entries = {entry.idx: entry for entry in entries_list}
        return entries
    
@show_in_op_list
class Sort(BatchOp):
    """Sort the entries in a batch"""
    def __init__(self, *keys, reverse=False, custom_func: Callable[[Dict],Any] = None, barrier_level = 1):
        super().__init__(consume_all_batch=True, barrier_level=barrier_level)
        self.keys = KeysUtil.make_keys(*keys, allow_empty=False) if keys else None
        self.reverse = reverse
        self.custom_func = custom_func
        if self.custom_func is None and self.keys is None:
            raise ValueError("Either 'keys' or 'custom_func' must be provided for sorting.")
    def _args_repr(self): return ReprUtil.repr_keys(self.keys) if self.keys else ReprUtil.repr_lambda(self.custom_func)
    def _key(self,entry:Entry):
        if self.custom_func is not None:
            if self.keys is not None:
                return self.custom_func(*KeysUtil.read_dict(entry.data, self.keys))
            else:
                return self.custom_func(entry.data)
        else:
            return KeysUtil.read_dict(entry.data, self.keys)
    def update_batch(self, entries: Dict[str, Entry]) -> Dict[str, Entry]:
        entries_list = list(entries.values())
        entries_list = sorted(entries_list, key=self._key, reverse=self.reverse)
        return {entry.idx: entry for entry in entries_list}
    

__all__ = [
    "Filter",
    "FilterFailedEntries",
    "FilterMissingFields",
    "Apply",
    "MapField",
    "SetField",
    "RemoveField",
    "RenameField",
    "Shuffle",
    "Sort",
    "TakeFirstN"
]
