import jsonlines
from pathlib import Path
from typing import List, Dict, Set

def collect_all_idx_from_jsonl(glob_str: str)->Set[str]:
    all_idx = set()
    for file_path in Path().glob(glob_str):
        if file_path.suffix == '.jsonl':
            with jsonlines.open(file_path) as reader:
                for entry in reader:
                    if 'idx' in entry:
                        all_idx.add(entry['idx'])
    return all_idx

__all__ = [
    "collect_all_idx_from_jsonl",
]