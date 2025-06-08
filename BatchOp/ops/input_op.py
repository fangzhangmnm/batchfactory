from ..core import *
from ..lib.utils import hash_text
from ..lib.markdown_utils import iter_markdown_lines, iter_markdown_entries

from typing import Union, List, Dict, Any, Literal, Iterator, Tuple
import jsonlines,json
import pyarrow.parquet as pq
from glob import glob
import itertools as itt
from abc import abstractmethod, ABC

class BaseReaderOp(InputOp, ABC):
    def __init__(self,
                    fields: List[str],
                    offset: int = 0,
                    max_count: int = None,
                    fire_once: bool = True
                    ):
        super().__init__(fire_once=fire_once)
        self.fields = fields
        self.offset = offset
        self.max_count = max_count
    @abstractmethod
    def _iter_records(self) -> Iterator[Tuple[str,Dict]]:
        """Abstract method to iterate over records in the data source."""
        pass
    def generate_batch(self)-> List[Entry]:
        stop = self.offset + self.max_count if self.max_count is not None else None
        entries = []
        for idx,json_obj in itt.islice(self._iter_records(), self.offset, stop):
            entry = Entry(idx=idx)
            for field in self.fields:
                entry.data[field] = json_obj.get(field, None)
            entries.append(entry)
        return entries


def generate_idx_from_json(json_obj: Dict[str, Any], idx_field: str|None=None, hash_fields: Union[List[str], None]=None) -> str:
    """Generate an index for the entry based on idx_field and/or hash_fields."""
    idx = ""
    if idx_field:
        idx = json_obj.get(idx_field, "")
    if hash_fields:
        if idx:
            idx += "_"
        idx += hash_text(json.dumps({field: json_obj.get(field, "") for field in hash_fields}, sort_keys=True))
    return idx

class ReadJsonOp(BaseReaderOp):
    def __init__(self, 
                 glob_str: str, 
                 fields: List[str],
                 idx_field: str = None,
                 hash_fields: Union[str, List[str]] = None,
                 offset: int = 0,
                 max_count: int = None,
                 fire_once: bool = True
                 ):
        super().__init__(fields=fields, offset=offset, max_count=max_count, fire_once=fire_once)
        if not idx_field and not hash_fields:
            raise ValueError("At least one of idx_field or hash_fields must be provided.")
        self.glob_str = glob_str
        self.idx_field = idx_field
        self.hash_fields = hash_fields if isinstance(hash_fields, list) else [hash_fields] if hash_fields else None
    def _iter_records(self) -> Iterator[Tuple[str,Dict]]:
        for path in sorted(glob(self.glob_str)):
            if path.endswith('.jsonl'):
                with jsonlines.open(path) as reader:
                    for record in reader:
                        idx = generate_idx_from_json(record, self.idx_field, self.hash_fields)
                        yield idx, record
            elif path.endswith('.json'):
                with open(path, 'r', encoding='utf-8') as f:
                    records = json.load(f)
                    if isinstance(records, dict):
                        records = [records]  # Ensure data is a list of dicts
                    for record in records:
                        idx = generate_idx_from_json(record, self.idx_field, self.hash_fields)
                        yield idx, record

def generate_directory_str(directory: List[str]) -> str:
    directory = [d.strip().replace(" ", "_").replace("/", "_") for d in directory]
    return "/".join(directory)

def generate_idx_from_directory_keyword(directory: List[str], keyword: str)-> str:
    directory = [d.strip().replace(" ", "_").replace("/", "_") for d in directory]
    keyword = keyword.strip().replace(" ", "_").replace("/", "_")
    return hash_text("/".join(directory) + "/" + keyword)

class ReadMarkdownLinesOp(BaseReaderOp):
    def __init__(self, 
                    glob_str: str,
                    keyword_field: str,
                    directory_list_field: str|None = None,
                    directory_str_field: str|None = None,
                    offset: int = 0,
                    max_count: int|None = None,
                    fire_once: bool = True
                    ):
        fields = [keyword_field, directory_list_field, directory_str_field]
        fields = [f for f in fields if f]
        super().__init__(fields=fields, offset=offset, max_count=max_count, fire_once=fire_once)
        self.glob_str = glob_str
        self.keyword_field = keyword_field
        self.directory_list_field = directory_list_field
        self.directory_str_field = directory_str_field
    def _iter_records(self) -> Iterator[Dict[str, Any]]:
        for path in sorted(glob(self.glob_str)):
            for directory, keyword, _ in iter_markdown_lines(path):
                idx = generate_idx_from_directory_keyword(directory, keyword)
                record = {self.keyword_field: keyword}
                if self.directory_list_field:
                    record[self.directory_list_field] = directory
                if self.directory_str_field:
                    record[self.directory_str_field] = generate_directory_str(directory)
                yield idx, record

__all__ = [
    "ReadJsonOp",
    "ReadMarkdownLinesOp",
]