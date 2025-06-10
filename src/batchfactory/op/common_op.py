from ..core.op_base import *
from ..lib.utils import FieldsRouter
from ..core import BrokerJobStatus, Entry

from typing import List,Dict, Callable
import random

class Filter(FilterOp):
    """
    - `Filter(lambda data:data['keep_if_True'])`
    - `Filter(lambda x:x>5, 'score')`
    """
    def __init__(self,criteria:Callable,*fields,consume_rejected=False):
        super().__init__(consume_rejected=consume_rejected)
        self._criteria = criteria
        self.router = FieldsRouter(*fields,style="tuple") if fields else None
    def criteria(self, entry):
        if self.router:
            return self._criteria(*self.router.read_tuple(entry.data))
        else:
            return self._criteria(entry.data)
        
class FilterFailedEntries(FilterOp):
    "Drops entries with status failed"
    def __init__(self, status_field="status",consume_rejected=False):
        super().__init__(consume_rejected=consume_rejected)
        self.status_field = status_field
    def criteria(self, entry):
        return BrokerJobStatus(entry.data[self.status_field]) != BrokerJobStatus.FAILED
    
class FilterMissingField(FilterOp):
    "Drop entries that do not have all the fields specified in `fields`"
    def __init__(self, *fields, consume_rejected=False, allow_None=True):
        super().__init__(consume_rejected=consume_rejected)
        self.router = FieldsRouter(*fields, style="tuple")
        self.allow_None = allow_None
    def criteria(self, entry):
        if self.allow_None:
            return all(field in entry.data for field in self.router.froms)
        else:
            return all(entry.data.get(field) is not None for field in self.router.froms)
    
class Apply(ApplyOp):
    """
    - `Apply(lambda data: operator.setitem(data, 'sum', data['a'] + data['b']))`
    - `Apply(operator.add, ['a', 'b'], ['sum'])`
    """
    def __init__(self, func:Callable, *fields):
        super().__init__()
        self.func = func
        self.router = FieldsRouter(*fields, style="inout") if fields else None
    def update(self, entry:Entry)->None:
        if self.router:
            tuple_or_entry = self.func(*self.router.read_tuple(entry.data))
            if len(self.router.tos)>=2:
                self.router.write_tuple(entry.data, *tuple_or_entry)
            else:
                self.router.write_tuple(entry.data, tuple_or_entry)
        else:
            self.func(entry.data)

class SetField(ApplyOp):
    "`SetField('k1', v1, 'k2', v2, ...)`, see `FieldsRouter` for details"
    def __init__(self, *fields_and_values):
        super().__init__()
        self.router = FieldsRouter(*fields_and_values, style="map",out_types=None)
    def update(self, entry:Entry)->None:
        for field, value in zip(self.router.froms, self.router.tos):
            entry.data[field] = value

class RemoveField(ApplyOp):
    "`RemoveField('k1', 'k2', ...)`"
    def __init__(self, *fields):
        super().__init__()
        self.router = FieldsRouter(*fields, style="tuple")
    def update(self, entry:Entry)->None:
        for field in self.router.froms:
            entry.data.pop(field, None)
            
class RenameField(ApplyOp):
    "`RenameField('from1', 'to1')`, see `FieldsRouter` for details"
    def __init__(self, *fields):
        super().__init__()
        self.router = FieldsRouter(*fields, style="map")
        if set(self.router.froms) & set(self.router.tos):
            raise ValueError("RenameField requires unique froms and tos to avoid ambiguity.")
    def update(self, entry:Entry)->None:
        for k1, k2 in zip(self.router.froms, self.router.tos):
            entry.data[k2] = entry.data.pop(k1, None)

class Shuffle(BatchOp):
    """Shuffles the entries in a fixed random order"""
    def __init__(self, seed):
        super().__init__(consume_all_batch=True)
        self.seed = seed
    def update_batch(self, entries: Dict[str, Entry]) -> Dict[str, Entry]:
        entries_list = list(entries.values())
        rng = random.Random(self.seed)
        rng.shuffle(entries_list)
        entries = {entry.idx: entry for entry in entries_list}
        return entries
    
class TakeFirstN(BatchOp):
    """Takes the first N entries from the batch. discards the rest."""
    def __init__(self, n: int):
        super().__init__(consume_all_batch=True)
        self.n = n
    def update_batch(self, entries: Dict[str, Entry]) -> Dict[str, Entry]:
        entries_list = list(entries.values())
        entries_list = entries_list[:self.n]
        entries = {entry.idx: entry for entry in entries_list}
        return entries


__all__ = [
    "Filter",
    "FilterFailedEntries",
    "FilterMissingField",
    "Apply",
    "SetField",
    "RemoveField",
    "RenameField",
    "Shuffle",
    "TakeFirstN"
]
