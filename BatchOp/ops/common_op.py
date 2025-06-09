from ..core import AtomicOp, BrokerJobStatus, Entry, BatchOp
from ..lib.utils import _to_list_2, FieldsRouter

from typing import List,Dict
import random

class DropFailedOp(AtomicOp):
    def __init__(self, status_field="status"):
        super().__init__()
        self.status_field = status_field
    def update(self, entry:Entry)->Entry|None:
        if BrokerJobStatus(entry.data[self.status_field]) == BrokerJobStatus.FAILED:
            return None
        return entry
    
class DropMissingOp(AtomicOp):
    def __init__(self, fields:str|List):
        super().__init__()
        self.fields = _to_list_2(fields)
    def update(self, entry:Entry)->Entry|None:
        for field in self.fields:
            if field not in entry.data or entry.data[field] is None:
                return None
        return entry
    
class DropFieldOp(AtomicOp):
    def __init__(self, fields:str|List):
        super().__init__()
        self.fields = _to_list_2(fields)
    def update(self, entry:Entry)->Entry|None:
        for field in self.fields:
            entry.data.pop(field, None)
        return entry
    
class ApplyOp(AtomicOp):
    """Applies a function to the entry data."""
    def __init__(self,func):
        super().__init__()
        self.func = func
    def update(self, entry:Entry)->Entry|None:
        self.func(entry.data)
        return entry
    
class RenameOp(AtomicOp):
    def __init__(self, *args):
        super().__init__()
        self.field_router = FieldsRouter(*args)
        if len(self.field_router.froms)!= len(self.field_router.tos):
            raise ValueError("RenameOp requires equal number of froms and tos.")
    def update(self, entry:Entry)->Entry|None:
        for k1,k2 in zip(self.field_router.froms, self.field_router.tos):
            entry.data[k2] = entry.data.pop(k1, None)
        return entry

    
class MapFieldOp(AtomicOp):
    """Maps a function from specific field(s) to another field(s) (or themselves) in the entry data."""
    def __init__(self, func, *args):
        super().__init__()
        self.field_router = FieldsRouter(*args)
        self.func = func
    def update(self, entry:Entry)->Entry|None:
        self.field_router.write_tuple(
            entry.data,
            self.func(*self.field_router.read_tuple(entry.data))
        )
        return entry
    
class InsertFieldOp(AtomicOp):
    """Inserts a field with a default value into the entry data."""
    def __init__(self, *args):
        """
        InsertFieldOp("field_name", default_value)
        InsertFieldOp({"field_name": default_value, "another_field": another_value})
        """
        super().__init__()
        self.router = FieldsRouter(*args)
    def update(self, entry:Entry)->Entry|None:
        for field, value in zip(self.router.froms, self.router.tos):
            entry.data[field] = value
        return entry
    
class ShuffleOp(BatchOp):
    """Shuffles the entries in a fixed random order"""
    def __init__(self, seed):
        super().__init__(consume_all_batch=True)
        self.seed = seed
    def update_batch(self, entries: Dict[str,Entry])->Dict[str,Entry]:
        entries_list = list(entries.values())
        rng=random.Random(self.seed)
        rng.shuffle(entries_list)
        entries = {entry.idx: entry for entry in entries_list}
        return entries
    
class TakeFirstNOp(BatchOp):
    """Takes the first N entries from the batch. discards the rest."""
    def __init__(self, n: int):
        super().__init__(consume_all_batch=True)
        self.n = n
    def update_batch(self, entries: Dict[str,Entry])->Dict[str,Entry]:
        entries_list = list(entries.values())
        entries_list = entries_list[:self.n]
        entries = {entry.idx: entry for entry in entries_list}
        return entries


__all__ = [
    "DropFailedOp",
    "DropMissingOp",
    "DropFieldOp",
    "RenameOp",
    "ApplyOp",
    "MapFieldOp",
    "InsertFieldOp",
    "ShuffleOp",
    "TakeFirstNOp",
]