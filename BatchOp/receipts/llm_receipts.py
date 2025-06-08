from ..core import *
from ..ops import *
from ..lib.utils import _to_list_2

class SeparateCotOp(AtomicOp):
    def __init__(self, label="</think>",field="text",cot_field:str|None="cot"):
        super().__init__()
        self.label = label
        self.field = field
        self.cot_field = cot_field
    def update(self, entry: Entry):
        if self.label in entry.data[self.field]:
            cot, response = entry.data[self.field].split(self.label, 1)
        else:
            cot, response = "", entry.data[self.field]
        entry.data[self.field] = response
        if self.cot_field is not None:
            entry.data[self.cot_field] = cot
        return entry
    
class DropLLMRejectionOp(AtomicOp):
    def __init__(self,startings:str|List,field="text"):
        super().__init__()
        self.startings = _to_list_2(startings)
        self.startings = [s.lower().strip() for s in self.startings]
        self.field = field
    def update(self,entry:Entry):
        llm_response=entry.data[self.field].lower().strip()
        for starting in self.startings:
            if llm_response.startswith(starting):
                return None
        return entry


__all__ = [
    "SeparateCotOp",
    "DropLLMRejectionOp",
]