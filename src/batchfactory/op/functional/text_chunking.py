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

def label_texts(texts:List[str],*,offset=1):
    """Label each text with a number, starting from `offset`."""
    return [f"{i + offset}: {text}" for i, text in enumerate(texts)]

def group_texts_by_length(texts:List[str], *, chunk_length) -> List[List[str]]:
    "Group texts by suggested chunk_length. (May exceed if a single line is too long)"
    groups = [[]]
    last_group_length = 0
    for i,line in enumerate(texts):
        if last_group_length ==0 or (last_group_length + len(line) + 1 <= chunk_length):
            groups[-1].append(line)
            last_group_length += len(line) + 1
        else:
            groups.append([line])
            last_group_length = len(line) + 1
    return groups

def partition_list_by_labels(lst:List[Any], labels:List[int], *, offset=1) -> List[List[str]]:
    """Partition a list into groups based on labels, which indicate the start of new groups."""
    groups = [[]]
    for i, item in enumerate(lst):
        if i + offset in labels and groups[-1]:
            groups.append([])
        groups[-1].append(item)
    return groups

def create_parent_map_by_labels(labels:List[int],total_num:int, *, offset=1)->Dict[int,int]:
    """Create a mapping from each label to its parent label."""
    parent_map = {}
    current_parent = offset-1
    for i in range(total_num):
        if i + offset in labels:
            current_parent += 1
        parent_map[i + offset] = current_parent
    return parent_map

def create_children_map(parent_map:Dict[int,int]) -> Dict[int, List[int]]:
    children_map = {}
    for child in sorted(parent_map.keys()):
        parent = parent_map[child]
        children_map.setdefault(parent, []).append(child)
    return children_map

def join_texts(texts:List[str], *, separator="\n") -> str:
    """Join a list of texts into a single string with a specified separator."""
    return separator.join(texts)

def flatten_list(lst:List[List[Any]]) -> List[Any]:
    """Flatten a list of lists into a single list."""
    return list(it.chain.from_iterable(lst))

def label_and_chunk_lines(text:str, chunk_length:int) -> List[str]:
    lines = F.lines(text, non_empty=True, strip=False)
    return label_and_chunk_texts(lines, chunk_length=chunk_length)

def label_and_chunk_texts(texts:List[str], chunk_length:int) -> List[str]:
    texts = label_texts(texts)
    groups = group_texts_by_length(texts, chunk_length=chunk_length)
    chunks = [join_texts(group) for group in groups]
    return chunks

def postprocess_labels(labels:List[List[int]], texts:List[str]):
    labels = flatten_list(labels)
    new_texts = partition_list_by_labels(texts, labels)
    new_texts = [join_texts(chunk, separator="\n\n") for chunk in new_texts]
    parent_map = create_parent_map_by_labels(labels, len(texts))
    return new_texts, parent_map

def remove_markup_header(text):
    return re.sub(r'^#+\s*', '', text, flags=re.MULTILINE)

__all__ = [
    "lines",
    "label_texts",
    "group_texts_by_length",
    "partition_list_by_labels",
    "join_texts",
    "flatten_list",
    "label_and_chunk_texts",
    "postprocess_labels",
    "create_parent_map_by_labels",
    "create_children_map",
    "remove_markup_header"
]