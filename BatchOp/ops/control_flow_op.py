from ..core import AtomicOp, Entry, MergeOp, SplitOp
from ..lib.utils import _to_list_2, FieldsRouter

from typing import List, Tuple
from copy import deepcopy


            
            
            
            
        

class DuplicateOp(SplitOp):
    def __init__(self, n_outputs:int = 2, replica_idx_field:str|None="replica_idx"):
        super().__init__(n_outputs=n_outputs)
        self.replica_idx_field = replica_idx_field
    def route(self, entry: Entry) -> List[Tuple[int, Entry]]:
        output_entries = []
        for i in range(self.n_outputs):
            new_entry = deepcopy(entry)
            if self.replica_idx_field:
                new_entry.data[self.replica_idx_field] = entry.idx
            output_entries.append((i, new_entry))
        return output_entries

