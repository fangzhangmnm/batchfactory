from ..core.op_base import *
from ..lib.utils import KeysUtil
from ..core import BrokerJobStatus, Entry

from typing import List,Dict, Callable
import random

class Filter(FilterOp):
    """
    - `Filter(lambda data:data['keep_if_True'])`
    - `Filter(lambda x:x>5, 'score')`
    """
    def __init__(self,criteria:Callable,*keys,consume_rejected=False):
        super().__init__(consume_rejected=consume_rejected)
        self._criteria = criteria
        self.keys = KeysUtil.make_keys(*keys) if keys else None
    def criteria(self, entry):
        if self.keys is not None:
            return self._criteria(*self.keys.read_keys(entry.data))
        else:
            return self._criteria(entry.data)
        
class FilterFailedEntries(FilterOp):
    "Drops entries with status failed"
    def __init__(self, status_key="status",consume_rejected=False):
        super().__init__(consume_rejected=consume_rejected)
        self.status_key = status_key
    def criteria(self, entry):
        return BrokerJobStatus(entry.data[self.status_key]) != BrokerJobStatus.FAILED
    
class FilterMissingField(FilterOp):
    "Drop entries that do not have all the keys specified in `keys`"
    def __init__(self, *keys, consume_rejected=False, allow_None=True):
        super().__init__(consume_rejected=consume_rejected)
        self.keys = KeysUtil.make_keys(*keys,allow_empty=False)
        self.allow_None = allow_None
    def criteria(self, entry):
        if self.allow_None:
            return all(field in entry.data for field in self.keys)
        else:
            return all(entry.data.get(field) is not None for field in self.keys)
    
class Apply(ApplyOp):
    """
    - `Apply(lambda data: operator.setitem(data, 'sum', data['a'] + data['b']))`
    - `Apply(operator.add, ['a', 'b'], ['sum'])`
    """
    def __init__(self, func:Callable, *keys):
        super().__init__()
        self.func = func
        self.in_keys, self.out_keys = KeysUtil.make_io_keys(*keys) if keys else (None, None)
    def update(self, entry:Entry)->None:
        if self.in_keys is not None:
            out_tuple = self.func(*KeysUtil.read_dict(entry.data, self.in_keys))
            if len(self.out_keys) == 0:
                out_tuple = ()
            elif len(self.out_keys) == 1:
                out_tuple = (out_tuple,)
            KeysUtil.write_dict(entry.data, self.out_keys, *out_tuple)
        else:
            self.func(entry.data)

class SetField(ApplyOp):
    "`SetField('k1', v1, 'k2', v2, ...)`, see `MapInput` for details"
    def __init__(self, *data):
        super().__init__()
        self.data = KeysUtil.make_dict(*data)
    def update(self, entry:Entry)->None:
        for field, value in self.data.items():
            entry.data[field] = value

class RemoveField(ApplyOp):
    "`RemoveField('k1', 'k2', ...)`"
    def __init__(self, *keys):
        super().__init__()
        self.keys = KeysUtil.make_keys(*keys, allow_empty=False)
    def update(self, entry:Entry)->None:
        for field in self.keys:
            entry.data.pop(field, None)
            
class RenameField(ApplyOp):
    "`RenameField('from1', 'to1')`, see `KeysMapInput` for details"
    def __init__(self, *keys_map, copy=False):
        super().__init__()
        self.from_keys, self.to_keys = KeysUtil.make_keys_map(*keys_map, non_overlapping=True)
        self.copy = copy
    def update(self, entry:Entry)->None:
        for k1, k2 in zip(self.from_keys, self.to_keys):
            if self.copy:
                entry.data[k2] = entry.data.get(k1, None)
            else:
                entry.data[k2] = entry.data.pop(k1, None)

class Shuffle(BatchOp):
    """Shuffles the entries in a fixed random order"""
    def __init__(self, seed, barrier_level = 1):
        super().__init__(consume_all_batch=True, barrier_level=barrier_level)
        self.seed = seed
    def update_batch(self, entries: Dict[str, Entry]) -> Dict[str, Entry]:
        entries_list = list(entries.values())
        rng = random.Random(self.seed)
        rng.shuffle(entries_list)
        entries = {entry.idx: entry for entry in entries_list}
        return entries
    
class TakeFirstN(BatchOp):
    """Takes the first N entries from the batch. discards the rest."""
    def __init__(self, n: int, barrier_level = 1):
        super().__init__(consume_all_batch=True, barrier_level=barrier_level)
        self.n = n
    def update_batch(self, entries: Dict[str, Entry]) -> Dict[str, Entry]:
        entries_list = list(entries.values())
        entries_list = entries_list[:self.n]
        entries = {entry.idx: entry for entry in entries_list}
        return entries
    
class Sort(BatchOp):
    """Sort the entries in a batch"""
    def __init__(self, *keys, reverse=False, custom_func: Callable = None, barrier_level = 1):
        super().__init__(consume_all_batch=True, barrier_level=barrier_level)
        self.keys = KeysUtil.make_keys(*keys, allow_empty=False) if keys else None
        self.reverse = reverse
        self.custom_func = custom_func
        if self.custom_func is None and self.keys is None:
            raise ValueError("Either 'keys' or 'custom_func' must be provided for sorting.")
    def _key(self,entry:Entry):
        if self.custom_func is not None:
            if self.keys is not None:
                return self.custom_func(*KeysUtil.read_dict(entry.data, self.keys))
            else:
                return self.custom_func(entry.data)
        else:
            print(KeysUtil.read_dict(entry.data, self.keys))
            return KeysUtil.read_dict(entry.data, self.keys)
    def update_batch(self, entries: Dict[str, Entry]) -> Dict[str, Entry]:
        entries_list = list(entries.values())
        print(entries_list)
        entries_list = sorted(entries_list, key=self._key, reverse=self.reverse)
        print(entries_list)
        return {entry.idx: entry for entry in entries_list}
    

__all__ = [
    "Filter",
    "FilterFailedEntries",
    "FilterMissingField",
    "Apply",
    "SetField",
    "RemoveField",
    "RenameField",
    "Shuffle",
    "Sort",
    "TakeFirstN"
]
