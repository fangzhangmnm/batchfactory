from ..core import AtomicOp, BrokerJobStatus, OutputOp
from ..core.entry import Entry
from ..lib.utils import _to_list_2

from typing import List
import jsonlines 
import os
from dataclasses import asdict
from copy import deepcopy


class PrintEntryOp(OutputOp):
    def output_batch(self, entries: List['Entry']):
        print("Entries:")
        for entry in entries:
            print(entry.idx)
            print(entry.data)
            print()

class PrintTextOp(OutputOp):
    def __init__(self, field="text"):
        super().__init__()
        self.field = field

    def output_batch(self, entries: List['Entry']):
        print("Text Entries:")
        for entry in entries:
            print(f"Index: {entry.idx}, Revision: {entry.rev}")
            print(entry.data.get(self.field, "No text found"))
            print()

class OutputJsonlOp(OutputOp):
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
    def output_batch(self, entries: List['Entry']):
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
        for entry in entries:
            self._update(entry)
        with jsonlines.open(self.path, 'w') as writer:
            for entry in self._output_entries.values():
                record = self._prepare_output(entry)
                writer.write(record)
        self._output_entries.clear()
        print(f"Output {len(self._output_entries)} entries to {os.path.abspath(self.path)}")
    def _prepare_output(self,entry:Entry):
        if not self.output_fields:
            record = deepcopy(entry.data)
        else:
            record = {k: entry.data[k] for k in self.output_fields}
        record['idx'] = entry.idx
        record['rev'] = entry.rev
        return record
    def _update(self,new_entry):
        if new_entry.idx in self._output_entries:
            if new_entry.rev < self._output_entries[new_entry.idx].rev:
                return
        self._output_entries[new_entry.idx] = new_entry    
