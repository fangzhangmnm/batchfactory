from ..core import AtomicOp, BrokerJobStatus
from ..lib.utils import _to_list

from typing import List

class DropFailedOp(AtomicOp):
    def __init__(self, status_field="status"):
        super().__init__()
        self.status_field = status_field
    def update(self, entry):
        if BrokerJobStatus(entry.data[self.status_field]) == BrokerJobStatus.FAILED:
            return None
        return entry
    
class DropMissingOp(AtomicOp):
    def __init__(self, fields:str|List):
        super().__init__()
        self.fields = _to_list(fields)
    def update(self, entry):
        for field in self.fields:
            if field not in entry.data or entry.data[field] is None:
                return None
        return entry
    
class DropFieldOp(AtomicOp):
    def __init__(self, fields:str|List):
        super().__init__()
        self.fields = _to_list(fields)
    def update(self, entry):
        for field in self.fields:
            entry.data.pop(field, None)
        return entry
    
class RenameOp(AtomicOp):
    def __init__(self, rename_mapping:dict|str, other=None):
        super().__init__()
        if isinstance(rename_mapping, str) and other is not None:
            rename_mapping = {rename_mapping: other}
        if not isinstance(rename_mapping, dict):
            raise ValueError("rename_mapping must be a dictionary or a string with a corresponding other value.")
        self.rename_mapping = rename_mapping
    def update(self, entry):
        for old_field, new_field in self.rename_mapping.items():
            if old_field in entry.data:
                entry.data[new_field] = entry.data.pop(old_field)
        return entry

class ApplyOp(AtomicOp):
    """Applies a function to the entry data."""
    def __init__(self,func):
        super().__init__()
        self.func = func
    def update(self, entry):
        self.func(entry.data)
        return entry
    
class MapFieldOp(AtomicOp):
    """Maps a function from specific field(s) to another field(s) (or themselves) in the entry data."""
    def __init__(self, input_fields:str|List, output_fields:str|List|None, func):
        super().__init__()
        self.input_fields = _to_list(input_fields)
        self.output_fields = _to_list(output_fields) if output_fields else self.input_fields
        self.func = func
    def update(self, entry):
        input_data = [entry.data.get(field) for field in self.input_fields]
        output_data = self.func(*input_data)
        if not isinstance(output_data, tuple):
            output_data = (output_data,)
        for field, value in zip(self.output_fields, output_data):
            entry.data[field] = value
    
__all__ = [
    "DropFailedOp",
    "DropMissingOp",
    "DropFieldOp",
    "RenameOp",
    "ApplyOp",
    "MapFieldOp",
]