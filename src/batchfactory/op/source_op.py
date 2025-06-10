from ..core import *
from ..lib.utils import hash_text, hash_texts, hash_json, FieldsRouter
from ..lib.markdown_utils import iter_markdown_lines, iter_markdown_entries

from typing import Union, List, Dict, Any, Literal, Iterator, Tuple
import jsonlines,json
from glob import glob
import itertools as itt
from abc import abstractmethod, ABC
from copy import deepcopy

class ReaderOp(SourceOp, ABC):
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
    def generate_batch(self)-> Dict[str,Entry]:
        stop = self.offset + self.max_count if self.max_count is not None else None
        entries = {}
        for idx,json_obj in itt.islice(self._iter_records(), self.offset, stop):
            entry = Entry(idx=idx)
            for field in self.fields:
                entry.data[field] = json_obj.get(field, None)
            entries[idx] = entry
        return entries

class ReadJson(ReaderOp):
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
        self.hash_fields = FieldsRouter(hash_fields,style="tuple") if hash_fields is not None else None
    def _iter_records(self) -> Iterator[Tuple[str,Dict]]:
        for path in sorted(glob(self.glob_str)):
            if path.endswith('.jsonl'):
                with jsonlines.open(path) as reader:
                    for record in reader:
                        idx = self._generate_idx(record)#, self.idx_field, self.hash_fields)
                        yield idx, record
            elif path.endswith('.json'):
                with open(path, 'r', encoding='utf-8') as f:
                    records = json.load(f)
                    if isinstance(records, dict):
                        records = [records]  # Ensure data is a list of dicts
                    for record in records:
                        idx = self._generate_idx(record)#, self.idx_field, self.hash_fields)
                        yield idx, record
    def generate_idx_from_json(self, json_obj) -> str:
        """Generate an index for the entry based on idx_field and/or hash_fields."""
        idx = ""
        if self.index_field is not None:
            idx = json_obj.get(self.index_field, "")
        if self.hash_fields is not None:
            if idx:
                idx += "_"
            idx += hash_json({k:json_obj.get(k,"") for k in self.hash_fields.froms})
        return idx

def generate_directory_str(directory: List[str]) -> str:
    directory = [d.strip().replace(" ", "_").replace("/", "_") for d in directory]
    return "/".join(directory)

def generate_idx_from_directory_keyword(directory: List[str], keyword: str)-> str:
    directory = [d.strip().replace(" ", "_").replace("/", "_") for d in directory]
    keyword = keyword.strip().replace(" ", "_").replace("/", "_")
    return hash_text("/".join(directory) + "/" + keyword)

class ReadMarkdownLines(ReaderOp):
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

class FromList(SourceOp):
    def __init__(self,
                 input_list: List[Dict],
                 fire_once: bool = True,
                 output_field: str = None,
                 ):
        super().__init__(fire_once=fire_once)
        self.input_list = input_list
        self.output_field = output_field
    def generate_batch(self) -> Dict[str, Entry]:
        entries = {}
        for obj in self.input_list:
            entry = self._make_entry(obj)
            entries[entry.idx] = entry
        return entries
    def _make_entry(self,obj):
        if isinstance(obj, Entry):
            return obj
        elif isinstance(obj, dict):
            if all(k in obj for k in ["idx", "data"]):
                return Entry(idx=obj["idx"], data=obj["data"])
            else:
                if "idx" in obj:
                    return Entry(idx=obj["idx"], data=deepcopy(obj))
                else:
                    return Entry(idx=hash_json(obj), data=deepcopy(obj))
        elif isinstance(obj, (int, float, str, bool)) and self.output_field is not None:
            return Entry(idx=hash_text(str(obj)), data={self.output_field: obj})
        else:
            raise ValueError(f"Unsupported object type for entry creation: {type(obj)}")


__all__ = [
    "ReaderOp",
    "ReadJson",
    "ReadMarkdownLines",
    "FromList",
]