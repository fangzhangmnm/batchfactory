from ..core import ApplyOp, BrokerJobStatus, OutputOp, SourceOp
from ..core.entry import Entry
from ..lib.utils import _to_list_2, hash_text, hash_texts, hash_json, KeysUtil, ReprUtil
from ..lib.markdown_utils import iter_markdown_lines, iter_markdown_entries, write_markdown, markdown_sort_key
from .common_op import Sort

from typing import Union, List, Dict, Any, Literal, Iterator, Tuple
import re
import jsonlines,json
from glob import glob
import itertools as itt
from abc import abstractmethod, ABC
from collections.abc import Hashable
from copy import deepcopy
import os
from dataclasses import asdict
from copy import deepcopy


class ReaderOp(SourceOp, ABC):
    def __init__(self,
                    keys: List[str]|None,
                    offset: int = 0,
                    max_count: int = None,
                    fire_once: bool = True
                    ):
        super().__init__(fire_once=fire_once)
        self.keys = KeysUtil.make_keys(keys) if keys is not None else None
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
            if self.keys is not None:
                entry.data.update(KeysUtil.make_dict(self.keys,KeysUtil.read_dict(json_obj, self.keys)))
            else:
                entry.data.update(json_obj)
            entries[idx] = entry
        return entries

class ReadJsonl(ReaderOp):
    """Read JSON Lines files."""
    def __init__(self, 
                 glob_str: str, 
                 keys: List[str]=None,
                 idx_key: str = None,
                 hash_keys: Union[str, List[str]] = None,
                 offset: int = 0,
                 max_count: int = None,
                 fire_once: bool = True
                 ):
        super().__init__(keys=keys, offset=offset, max_count=max_count, fire_once=fire_once)
        self.glob_str = glob_str
        self.idx_key = idx_key
        self.hash_keys = KeysUtil.make_keys(hash_keys) if hash_keys is not None else None
        if self.idx_key is not None and self.hash_keys is not None:
            raise ValueError("Cannot specify both idx_key and hash_keys. Use one or the other.")
    def _args_repr(self): return ReprUtil.repr_str(self.glob_str)
    def _iter_records(self) -> Iterator[Tuple[str,Dict]]:
        for path in sorted(glob(self.glob_str)):
            if path.endswith('.jsonl'):
                with jsonlines.open(path) as reader:
                    for record in reader:
                        idx = self.generate_idx_from_json(record)
                        yield idx, record
            elif path.endswith('.json'):
                with open(path, 'r', encoding='utf-8') as f:
                    records = json.load(f)
                    if isinstance(records, dict):
                        records = [records]  # Ensure data is a list of dicts
                    for record in records:
                        idx = self._generate_idx(record)
                        yield idx, record
    def generate_idx_from_json(self, json_obj) -> str:
        """Generate an index for the entry based on idx_key and/or hash_keys."""
        if self.idx_key is not None:
            return json_obj.get(self.idx_key, "")
        elif self.hash_keys is not None:
            json_to_hash = {k:json_obj.get(k) for k in sorted(self.hash_keys)}
            return hash_json(json_to_hash)
        else:
            return hash_json(json_obj)

class WriteJsonl(OutputOp):
    """Write entries to a JSON Lines file."""
    def __init__(self, path: str, 
                 output_keys: str|List[str]=None,
                 only_current:bool=False):
        """if only_current, will ignore old entries in the output file that are not appearing in the current batch,
        otherwise will update on old entries based on idx and rev if output file already exists.
        will only output entry.data, but flattened idx and rev into entry.data
        """
        super().__init__()
        self.path = path
        self.only_current = only_current
        self.output_keys = _to_list_2(output_keys) if output_keys else None
        self._output_entries = {}
    def _args_repr(self): return ReprUtil.repr_str(self.path)
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
        if not self.output_keys:
            record = deepcopy(entry.data)
        else:
            record = {k: entry.data[k] for k in self.output_keys}
        record['idx'] = entry.idx
        record['rev'] = entry.rev
        return record
    def _update(self,new_entry):
        if new_entry.idx in self._output_entries and new_entry.rev < self._output_entries[new_entry.idx].rev:
                print("failed")
                return
        self._output_entries[new_entry.idx] = new_entry

def generate_directory_str(directory: List[str]) -> str:
    directory = [d.strip().replace(" ", "_").replace("/", "_") for d in directory]
    return "/".join(directory)

def generate_idx_from_directory_keyword(directory: List[str], keyword: str)-> str:
    directory = [d.strip().replace(" ", "_").replace("/", "_") for d in directory]
    keyword = keyword.strip().replace(" ", "_").replace("/", "_")
    return hash_text("/".join(directory) + "/" + keyword)

def remove_markdown_headings(text: str) -> str:
    text= re.sub(r'^#+\s*', '', text, flags=re.MULTILINE)
    return text

class ReadTxtFolder(ReaderOp):
    "Collect all txt files in a folder."
    def __init__(self, 
                glob_str: str,
                text_key: str = "text",
                filename_key = "filename",
                    offset: int = 0,
                    max_count: int = None,
                    fire_once: bool = True,
    ):
        fields = [filename_key, "text"]
        super().__init__(keys=[f for f in fields if f], offset=offset, max_count=max_count, fire_once=fire_once)
        self.filename_key = filename_key
        self.glob_str = glob_str
        self.text_key = text_key
    def _args_repr(self): return ReprUtil.repr_str(self.glob_str)
    def _iter_records(self) -> Iterator[Tuple[str, Dict]]:
        for path in sorted(glob(self.glob_str)):
            if not path.endswith('.txt'):
                continue
            with open(path, 'r', encoding='utf-8') as f:
                text = f.read()
            idx = hash_text(path)
            record = {self.text_key: text}
            if self.filename_key:
                record[self.filename_key] = os.path.basename(path)
            yield idx, record

class ReadMarkdown(ReaderOp):
    def __init__(self, 
                    glob_str: str,
                    context_key = "text",
                    keyword_key = "keyword",
                    directory_key = "directory",
                    offset: int = 0,
                    max_count: int = None,
                    fire_once: bool = True,
                    directory_mode: Literal['list', 'str'] = 'list',
                    format: Literal['lines', 'entries'] = 'entries',
                    ):
        if format == 'lines': context_key = None
        keys = [keyword_key, context_key, directory_key]
        keys = [f for f in keys if f]
        super().__init__(keys=keys, offset=offset, max_count=max_count, fire_once=fire_once)
        self.glob_str = glob_str
        self.keyword_key = keyword_key
        self.context_key = context_key
        self.directory_key = directory_key
        self.directory_mode = directory_mode
        self.format = format
    def _args_repr(self): return ReprUtil.repr_str(self.glob_str)
    def _iter_records(self) -> Iterator[Dict[str, Any]]:
        factory = {"lines": iter_markdown_lines, "entries": iter_markdown_entries}[self.format]
        for path in sorted(glob(self.glob_str)):
            for directory, keyword, context in factory(path):
                idx = generate_idx_from_directory_keyword(directory, keyword)
                record = {self.keyword_key: keyword}
                if self.context_key and self.format == "entries":
                    record[self.context_key] = context
                if self.directory_key:
                    if self.directory_mode == 'list':
                        record[self.directory_key] = directory
                    elif self.directory_mode == 'str':
                        record[self.directory_key] = generate_directory_str(directory)
                yield idx, record

class ReadMarkdownLines(ReadMarkdown):
    "Read Markdown files and extract non-empty lines as keyword with markdown heading hierarchy as directory."
    def __init__(self,*args, **kwargs):
        kwargs['format'] = 'lines'
        super().__init__(*args, **kwargs)
class ReadMarkdownEntries(ReadMarkdown):
    "Read Markdown files and extract entries with markdown heading hierarchy as directory and keyword."
    def __init__(self,*args, **kwargs):
        kwargs['format'] = 'entries'
        super().__init__(*args, **kwargs)

class WriteMarkdownEntries(OutputOp):
    "Write entries to a Markdown file, with heading hierarchy defined by directory and keyword."
    def __init__(self, path: str, 
                 context_key: str = "text",
                 keyword_key: str = "keyword",
                 directory_key: str = "directory",
                 sort: bool = True,
                 ):
        super().__init__()
        self.path = path
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        self.directory_key = directory_key
        self.keyword_key = keyword_key
        self.context_key = context_key
        self.sort = sort
    def _args_repr(self): return ReprUtil.repr_str(self.path)
    def output_batch(self, entries: Dict[str, Entry]) -> None:
        tuple_deduplicate ={}
        if os.path.exists(self.path):
            # tuples.extend(iter_markdown_entries(self.path))
            input_tuples = iter_markdown_entries(self.path)
            for directory, keyword, context in input_tuples:
                tuple_deduplicate[(tuple(directory), keyword)] = (directory, keyword, context)
        for entry in entries.values():
            directory = entry.data.get("directory", [])
            if isinstance(directory, str):
                directory = directory.split("/")
            keyword = entry.data.get(self.keyword_key, "")
            context = entry.data.get(self.context_key, "")
            context = remove_markdown_headings(context)
            # tuples.append((directory, keyword, context))
            tuple_deduplicate[(tuple(directory), keyword)] = (directory, keyword, context)
        tuples = [v for v in tuple_deduplicate.values()]
        write_markdown(tuples, self.path, sort=self.sort)
        print(f"Output {len(entries)} entries to {os.path.abspath(self.path)}")

class SortMarkdownEntries(Sort):
    def __init__(self,
                 directory_key: str = "directory",
                    keyword_key: str = "keyword",
                    barrier_level = 1,
    ):
        super().__init__(custom_func=self._sort_key, barrier_level=barrier_level)
    def _sort_key(self, data: Dict) -> Tuple:
        directory = data.get("directory", [])
        if isinstance(directory, str):
            directory = directory.split("/")
        keyword = data.get("keyword", "")
        return markdown_sort_key((directory, keyword, ""))




class FromList(SourceOp):
    "Create entries from a list of dictionaries or objects, each representing an entry."
    def __init__(self,
                 input_list: List[Dict]|List[Any],
                 output_key: str = None,
                 fire_once: bool = True,
                 ):
        super().__init__(fire_once=fire_once)
        self.input_list = input_list
        self.output_key = output_key
    def set_input(self, input_list: List[Dict]|List[Any]) -> None:
        self.input_list = input_list
    def generate_batch(self) -> Dict[str, Entry]:
        entries = {}
        for obj in self.input_list:
            entry = self._make_entry(obj)
            entries[entry.idx] = entry
        return entries
    def _make_entry(self,obj):
        if isinstance(obj, Entry) and self.output_key is None:
            return obj
        elif isinstance(obj, dict) and self.output_key is None:
            if all(k in obj for k in ["idx", "data"]):
                return Entry(idx=obj["idx"], data=obj["data"])
            else:
                if "idx" in obj:
                    return Entry(idx=obj["idx"], data=deepcopy(obj))
                else:
                    return Entry(idx=hash_json(obj), data=deepcopy(obj))
        elif isinstance(obj, (int, float, str, bool)) and self.output_key is not None:
            return Entry(idx=hash_text(str(obj)), data={self.output_key: obj})
        else:
            raise ValueError(f"Unsupported object type for entry creation: {type(obj)}")


class ToList(OutputOp):
    "Output a list of specific field(s) from entries."
    def __init__(self,*output_keys):
        super().__init__()
        self._output_entries = {}
        self.output_keys = KeysUtil.make_keys(output_keys) if output_keys else None
    def _args_repr(self): return ReprUtil.repr_keys(self.output_keys) if self.output_keys else ""
    def output_batch(self, entries: Dict[str, Entry]) -> None:
        for idx, entry in entries.items():
            if idx in self._output_entries:
                if entry.rev < self._output_entries[idx].rev:
                    continue
            if self.output_keys is not None:
                if len(self.output_keys) == 1:
                    record = entry.data[self.output_keys[0]]
                else:
                    record = {k: entry.data[k] for k in self.output_keys if k in entry.data}
            else:
                record = deepcopy(entry.data)
            self._output_entries[idx] = record
    def get_output(self) -> List[Dict|Any]:
        return list(self._output_entries.values())

class PrintEntry(OutputOp):
    "Print the first n entries information."
    def __init__(self,first_n=None):
        super().__init__()
        self.first_n = first_n
    def output_batch(self, entries: Dict[str, Entry]) -> None:
        if not entries: return
        for entry in list(entries.values())[:self.first_n]:
            print("idx:", entry.idx, "rev:", entry.rev)
            print(entry.data)
            print()
        print()

class PrintField(OutputOp):
    "Print the specific field(s) from the first n entries."
    def __init__(self, field="text", first_n=5):
        super().__init__()
        self.field = field
        self.first_n = first_n
    def _args_repr(self): return ReprUtil.repr_str(self.field)
    def output_batch(self,entries:Dict[str,Entry])->None:
        if not entries: return
        for entry in list(entries.values())[:self.first_n]:
            print(f"Index: {entry.idx}, Revision: {entry.rev} Field: '{self.field}'")
            print(entry.data.get(self.field, None))
            print()
        print()

__all__ = [
    "ToList",
    "PrintEntry",
    "PrintField",
    "WriteJsonl",
    "ReaderOp",
    "ReadJsonl",
    "ReadTxtFolder",
    "ReadMarkdownLines",
    "ReadMarkdownEntries",
    "WriteMarkdownEntries",
    "SortMarkdownEntries",
    "FromList",
]