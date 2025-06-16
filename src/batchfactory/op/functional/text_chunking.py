from .._registery import show_in_op_list

from typing import List, Dict, NamedTuple, Set, Tuple, Any
import re
import itertools as it

def lines(text:str,*, non_empty:bool=True, strip:bool=False):
    """Split text into lines, optionally stripping whitespace and filtering out empty lines."""
    lines = text.splitlines()
    if strip:
        lines = [line.strip() for line in lines]
    if non_empty:
        lines = [line for line in lines if line]
    return lines

def label_line_numbers(lines:List[str],*,offset=1):
    """Label each line with its line number, starting from `offset`."""
    return [f"{i + offset}: {line}" for i, line in enumerate(lines)]

def chunk_lines(lines:List[str], *, chunk_length) -> List[List[str]]:
    "Group lines by suggested chunk_length. (May exceed if a single line is too long)"
    groups = [[]]
    last_group_length = 0
    for i,line in enumerate(lines):
        if last_group_length ==0 or (last_group_length + len(line) + 1 <= chunk_length):
            groups[-1].append(line)
            last_group_length += len(line) + 1
        else:
            groups.append([line])
            last_group_length = len(line) + 1
    return groups

def split_lines(lines:List[str], line_labels:List[int], *, offset=1) -> List[List[str]]:
    """Split lines into groups based on the provided line labels."""
    groups = [[]]
    for i, line in enumerate(lines):
        if i + offset in line_labels and groups[-1]:
            groups.append([])
        groups[-1].append(line)
    return groups

def join_lines(lines:List[str], *, separator="\n") -> str:
    """Join a list of lines into a single text with the specified separator."""
    return separator.join(lines)

def flatten_list(lst:List[List[Any]]) -> List[Any]:
    """Flatten a list of lists into a single list."""
    return list(it.chain.from_iterable(lst))

__all__ = [
    "lines",
    "label_line_numbers",
    "chunk_lines",
    "split_lines",
    "join_lines",
    "flatten_list",
]