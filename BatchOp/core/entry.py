from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from typing import Union, List, Any, Tuple, Iterator

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
        
__all__ = [
    'Entry',
]