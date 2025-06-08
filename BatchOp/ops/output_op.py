from ..core import AtomicOp, BrokerJobStatus, OutputOp
from ..core.entry import Entry
from ..lib.utils import _to_list

from typing import List


class CLIPrintOp(OutputOp):
    def output_batch(self, entries: List['Entry']):
        for entry in entries:
            print(f"Entry {entry.idx}: {entry.data}")

