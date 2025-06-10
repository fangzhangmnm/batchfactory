from ..core import ApplyOp, BrokerJobStatus, OutputOp
from ..core.entry import Entry
from ..lib.utils import _to_list_2

from typing import List,Dict
import jsonlines 
import os
from dataclasses import asdict
from copy import deepcopy

class ToList(OutputOp):
    def __init__(self):
        super().__init__()
        self._output_entries = {}
    def output_batch(self, entries: Dict[str, Entry]) -> None:
        for idx, entry in entries.items():
            if idx in self._output_entries:
                if entry.rev < self._output_entries[idx].rev:
                    continue
            self._output_entries[idx] = entry
    def get_output(self) -> List[Entry]:
        return list(self._output_entries.values())

class PrintEntry(OutputOp):
    def __init__(self,first_n=None):
        super().__init__()
        self.first_n = first_n
    def output_batch(self, entries: Dict[str, Entry]) -> None:
        if not entries: return
        print("Entries:")
        for entry in list(entries.values())[:self.first_n]:
            print("idx:", entry.idx, "rev:", entry.rev)
            print(entry.data)
            print()
        print()

class Print(OutputOp):
    def __init__(self, field="text", first_n=None):
        super().__init__()
        self.field = field
        self.first_n = first_n

    def output_batch(self,entries:Dict[str,Entry])->None:
        print("Text Entries:")
        for entry in list(entries.values())[:self.first_n]:
            print(f"Index: {entry.idx}, Revision: {entry.rev}")
            print(entry.data.get(self.field, None))
            print()
        print()

class WriteJsonl(OutputOp):
    def __init__(self, path: str, 
                 output_fields: str|List[str]=None,
                 only_current:bool=False):
        """if only_current, will ignore old entries in the output file that are not appearing in the current batch,
        otherwise will update on old entries based on idx and rev if output file already exists.
        will only output entry.data, but flattened idx and rev into entry.data
        """
        super().__init__()
        self.path = path
        self.only_current = only_current
        self.output_fields = _to_list_2(output_fields) if output_fields else None
        self._output_entries = {}
    def output_batch(self,entries:Dict[str,Entry])->None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self._output_entries.clear()
        if (not self.only_current) and os.path.exists(self.path):
            with jsonlines.open(self.path, 'r') as reader:
                for record in reader:
                    entry = Entry(
                        idx=record['idx'],
                        rev=record.get('rev', 0),
                        data=record
                    )
                    self._update(entry)
        for entry in entries.values():
            self._update(entry)
        with jsonlines.open(self.path, 'w') as writer:
            for entry in self._output_entries.values():
                record = self._prepare_output(entry)
                writer.write(record)
        print(f"Output {len(self._output_entries)} entries to {os.path.abspath(self.path)}")
        self._output_entries.clear()
    def _prepare_output(self,entry:Entry):
        if not self.output_fields:
            record = deepcopy(entry.data)
        else:
            record = {k: entry.data[k] for k in self.output_fields}
        record['idx'] = entry.idx
        record['rev'] = entry.rev
        return record
    def _update(self,new_entry):
        if new_entry.idx in self._output_entries and new_entry.rev < self._output_entries[new_entry.idx].rev:
                print("failed")
                return
        self._output_entries[new_entry.idx] = new_entry

__all__ = [
    "ToList",
    "PrintEntry",
    "Print",
    "WriteJsonl"
]