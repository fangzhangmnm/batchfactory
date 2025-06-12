from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from typing import Union, List, Any, Tuple, Iterator
from ..lib.utils import ReprUtil

@dataclass
class Entry:
    # Mutable data structure representing a task in the pipeline.
    idx: str 
        # The unique identifier for the entry, used to track the task throughout the pipeline.
        # can only be executed once unless in loop (see rev)
    rev: int = 0
        # Counts how many times the task has passed the same loop
        # Tasks with higher rev always override lower ones.
    data: dict = field(default_factory=dict)
        # the actual data of the task
        # can be used to pass parameters or results between tasks   
        # must be json serializable
    meta: dict = field(default_factory=dict)
        # metadata of the task
        # can be used to store additional information like timestamps, status, etc.
    def __repr__(self): return repr_entry(self)


def repr_entry(entry: Entry) -> str:
    s = ""
    s+=f"Entry {entry.idx} (rev {entry.rev})\n"
    for k,v in entry.data.items():
        s+=f"  {k}: {ReprUtil.repr_item(v,max_len=50)}\n"
    return s
        
__all__ = [
    'Entry',
]