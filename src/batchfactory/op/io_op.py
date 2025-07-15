from ..core import ApplyOp, BrokerJobStatus, OutputOp, SourceOp, BatchOp
from ..core.entry import Entry
from ..lib.utils import _to_list_2, hash_text, hash_texts, hash_json, KeysUtil, ReprUtil, to_glob
from ..lib.markdown_utils import iter_markdown_lines, iter_markdown_entries, write_markdown_lines, write_markdown_entries, build_sort_key_from_headings, escape_markdown_headings
from .common_op import Sort
from ._registery import show_in_op_list

from typing import Union, List, Dict, Any, Literal, Iterator, Tuple, Set
import re
import jsonlines,json
from glob import glob
import itertools as itt
from abc import abstractmethod, ABC
from collections.abc import Hashable
from copy import deepcopy
import pyarrow.parquet as pq
import os
from dataclasses import asdict
from copy import deepcopy
from pathlib import Path
from tqdm.auto import tqdm
import random
import numpy as np

class ReaderOp(SourceOp, ABC):
    def __init__(self,
                    keys: List[str]|None,
                    *,
                    shuffle: bool = False,
                    seed: int = 42,
                    offset: int = 0,
                    max_count: int = None,
                    fire_once: bool = True
                    ):
        super().__init__(fire_once=fire_once)
        self.keys = KeysUtil.make_keys(keys) if keys is not None else None
        self.shuffle = shuffle
        self.offset = offset
        self.max_count = max_count
        self.seed = seed
    @abstractmethod
    def _estimate_size(self) -> int:
        "Estimate the upper bound of the number of records"
        pass
    @abstractmethod
    def _iter_record_proxy(self) -> Iterator[Any]:
        """Abstract method to iterate over unprocessed records in the data source."""
        pass
    @abstractmethod
    def _load_and_process_record(self, record_proxy: Any) -> Tuple[str, Dict]:
        """Process a single record and return its index and data."""
        pass
    def _generate_entry(self, idx: str, record: Dict) -> Entry:
        if self.keys is not None:
            record = KeysUtil.make_dict(self.keys, KeysUtil.read_dict(record, self.keys))
        return Entry(idx=idx, data=record)
    def generate_batch(self)-> Iterator[Entry]:
        if self.shuffle:
            return self.generate_batch_shuffled()
        else:
            return self.generate_batch_unshuffled()
    def generate_batch_shuffled(self)-> Iterator[Entry]:
        assert self.shuffle
        n_records = self._estimate_size()
        # indices = list(range(n_records))
        indices = np.arange(n_records, dtype=np.int64)
        rng = random.Random(self.seed)
        rng.shuffle(indices)
        indices = indices[self.offset:self.offset + self.max_count] if self.max_count is not None else indices[self.offset:]
        indice_map = {i:pos for pos,i in enumerate(indices)}
        output = [None]*len(indice_map)
        n_found = 0
        for i, record_proxy in tqdm(enumerate(self._iter_record_proxy())):
            if i in indice_map:
                idx, record = self._load_and_process_record(record_proxy)
                output[indice_map[i]] = self._generate_entry(idx, record)
                n_found += 1
            if n_found == len(indice_map):
                break
        for entry in output:
            if entry is not None:
                yield entry
    def generate_batch_unshuffled(self)->Iterator[Entry]:
        assert not self.shuffle
        for i,record_proxy in tqdm(enumerate(self._iter_record_proxy())):
            if i < self.offset:
                continue
            if self.max_count is not None and i >= self.offset + self.max_count:
                break
            idx, record = self._load_and_process_record(record_proxy)
            yield self._generate_entry(idx, record)

@show_in_op_list
class ReadJsonl(ReaderOp):
    """Read JSON Lines files. (also supports json array)"""
    def __init__(self, 
                glob_str: str|Path, 
                keys: List[str]=None,
                *,
                idx_key: str = None,
                hash_keys: Union[str, List[str]] = None,
                shuffle: bool = False,
                seed: int = 42,
                offset: int = 0,
                max_count: int = None,
                fire_once: bool = True
                ):
        if idx_key is None and hash_keys is None:
            raise ValueError("Must specify either idx_key or hash_keys to generate unique indices for entries.")
        if idx_key is not None and hash_keys is not None:
            raise ValueError("Cannot specify both idx_key and hash_keys. Use one or the other.")
        super().__init__(keys=keys, shuffle=shuffle, seed=seed, offset=offset, max_count=max_count, fire_once=fire_once)
        self.glob_str = to_glob(glob_str)
        self.idx_key = idx_key
        self.hash_keys = KeysUtil.make_keys(hash_keys) if hash_keys is not None else None
    def _args_repr(self): return ReprUtil.repr_glob(self.glob_str)
    def _estimate_size(self) -> int:
        total_size = 0
        for path in sorted(glob(self.glob_str)):
            if path.endswith('.jsonl'):
                total_size+=self._estimate_jsonl_size(path)
            elif path.endswith('.json'):
                total_size+=self._estimate_json_size(path)
            else:
                raise ValueError(f"Unsupported file format: {path}. Only .jsonl and .json files are supported.")
        return total_size
    def _iter_record_proxy(self):
        for path in sorted(glob(self.glob_str)):
            if path.endswith('.jsonl'):
                yield from self._iter_jsonl(path)
            elif path.endswith('.json'):
                yield from self._iter_json(path)
            else:
                raise ValueError(f"Unsupported file format: {path}. Only .jsonl and .json files are supported.")
    def _load_and_process_record(self, raw_record: Dict) -> Tuple[str, Dict]:
        idx = generate_idx_from_dict(raw_record, self.idx_key, self.hash_keys)
        return idx, raw_record
    def _iter_jsonl(self,path):
        with jsonlines.open(path) as reader:
            for record in reader:
                yield record
    def _iter_json(self,path):
        with open(path, 'r', encoding='utf-8') as f:
            records = json.load(f)
            if isinstance(records, dict):
                records = [records]
            for record in records:
                yield record
    def _estimate_jsonl_size(self,path):
        with open(path, 'r', encoding='utf-8') as f:
            return sum(1 for line in f if line.strip() and not line.startswith('#'))  # Skip empty lines and comments
    def _estimate_json_size(self,path):
        return sum(1 for _ in self._iter_json(path))


def generate_idx_from_dict(record, idx_key, hash_keys) -> str:
    """Generate an index for the entry based on idx_key and/or hash_keys."""
    if idx_key is not None:
        return record.get(idx_key, "")
    elif hash_keys is not None:
        json_to_hash = {k:record.get(k) for k in sorted(hash_keys)}
        return hash_json(json_to_hash)
    else:
        raise ValueError("Must specify either idx_key or hash_keys to generate unique indices for entries.")


@show_in_op_list
class ReadParquet(ReaderOp):
    """Read Parquet files."""
    def __init__(self, 
                glob_str: str|Path, 
                keys: List[str]=None,
                *,
                idx_key: str = None,
                hash_keys: Union[str, List[str]] = None,
                shuffle: bool = False,
                seed: int = 42,
                offset: int = 0,
                max_count: int = None,
                fire_once: bool = True,
                ):
        if idx_key is None and hash_keys is None:
            raise ValueError("Must specify either idx_key or hash_keys to generate unique indices for entries.")
        if idx_key is not None and hash_keys is not None:
            raise ValueError("Cannot specify both idx_key and hash_keys. Use one or the other.")
        super().__init__(keys=keys, shuffle=shuffle, seed=seed, offset=offset, max_count=max_count, fire_once=fire_once)
        self.glob_str = to_glob(glob_str)
        self.idx_key = idx_key
        self.hash_keys = KeysUtil.make_keys(hash_keys) if hash_keys is not None else None
    def _args_repr(self): return ReprUtil.repr_glob(self.glob_str)
    def _estimate_size(self):
        total_size = 0
        for path in sorted(glob(self.glob_str)):
            if path.endswith('.parquet'):
                total_size += self._estimate_parquet_size(path)
            else:
                raise ValueError(f"Unsupported file format: {path}. Only .parquet files are supported.")
        return total_size
    def _iter_record_proxy(self) -> Iterator[Any]:
        for path in sorted(glob(self.glob_str)):
            if path.endswith('.parquet'):
                yield from self._iter_parquet(path)
            else:
                raise ValueError(f"Unsupported file format: {path}. Only .parquet files are supported.")
    def _estimate_parquet_size(self, path: str) -> int:
        pq_file = pq.ParquetFile(path)
        return pq_file.metadata.num_rows
    def _iter_parquet(self, path: str) -> Iterator[int]:
        self._current_pq_file = pq.ParquetFile(path)
        self._last_row_group_index = None
        self._last_row_group = None
        for i in range(0, self._current_pq_file.metadata.num_rows):
            yield i
        del self._current_pq_file
        del self._last_row_group_index
        del self._last_row_group
    def _find_row_group(self, pos):
        offset = 0
        for i in range(self._current_pq_file.num_row_groups):
            rg_meta = self._current_pq_file.metadata.row_group(i)
            n_rows = rg_meta.num_rows
            if pos<offset+n_rows:
                return i,pos-offset
            offset += n_rows
        raise ValueError(f"Position {pos} out of bounds for file {self._current_pq_file.path}")
    def _load_and_process_record(self, pos:int):
        row_group_index, row_index = self._find_row_group(pos)
        if row_group_index != self._last_row_group_index:
            self._last_row_group = self._current_pq_file.read_row_group(row_group_index)
            self._last_row_group_index = row_group_index
        record = self._last_row_group.slice(row_index,1).to_pydict()
        record = {k: v[0] for k, v in record.items()}
        idx = generate_idx_from_dict(record, self.idx_key, self.hash_keys)
        return idx, record

@show_in_op_list
class WriteJsonl(OutputOp):
    """Write entries to a JSON Lines file."""
    def __init__(self, path: str, 
                 output_keys: str|List[str]=None,
                 ):
        """
        will only output entry.data, but flattened idx and rev into entry.data
        """
        super().__init__()
        self.path = path
        self.output_keys = _to_list_2(output_keys) if output_keys else None
    def _args_repr(self): return ReprUtil.repr_path(self.path)
    def output_batch(self,batch:Dict[str,Entry])->None:
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        output_entries = {}
        for entry in batch.values():
            if entry.idx in output_entries and entry.rev < output_entries[entry.idx].rev:
                print("failed to update entry:", entry.idx, "rev:", entry.rev)
                continue
            output_entries[entry.idx] = entry
        with jsonlines.open(self.path, 'w') as writer:
            for entry in output_entries.values():
                record = self._prepare_output(entry)
                writer.write(record)
        print(f"[WriteJsonl]: Output {len(output_entries)} entries to {os.path.abspath(self.path)}")
    def _prepare_output(self,entry:Entry):
        if not self.output_keys:
            record = deepcopy(entry.data)
        else:
            record = {k: entry.data[k] for k in self.output_keys}
        record['idx'] = entry.idx
        record['rev'] = entry.rev
        return record

def generate_idx_from_strings(strings: List[str]) -> str:
    def escape_string(s):
        return s.replace(" ", "_").replace("/", "_").replace("\\", "_")
    escaped_strings = [escape_string(s.strip()) for s in strings]
    return hash_text("/".join(escaped_strings))

def remove_markdown_headings(text: str) -> str:
    text= re.sub(r'^#+\s*', '', text, flags=re.MULTILINE)
    return text

@show_in_op_list
class ReadTxtFolder(ReaderOp):
    "Collect all txt files in a folder."
    def __init__(self, 
                glob_str: str|Path,
                text_key: str = "text",
                *,
                filename_key = "filename",
                remove_extension_in_filename = True,
                shuffle: bool = False,
                seed: int = 42,
                offset: int = 0,
                max_count: int = None,
                fire_once: bool = True,
    ):
        keys = [filename_key, "text"]
        keys = [f for f in keys if f]
        super().__init__(keys=keys, shuffle=shuffle, seed=seed, offset=offset, max_count=max_count, fire_once=fire_once)
        self.glob_str = to_glob(glob_str)
        self.text_key = text_key
        self.filename_key = filename_key
        self.remove_extension_in_filename = remove_extension_in_filename
    def _args_repr(self): return ReprUtil.repr_glob(self.glob_str)
    def _estimate_size(self):
        return len(list(glob(self.glob_str)))
    def _iter_record_proxy(self) -> Iterator[str]:
        for path in sorted(glob(self.glob_str)):
            yield path
    def _load_and_process_record(self, path: str) -> Tuple[str, Dict]:
        with open(path, 'r', encoding='utf-8') as f:
            text = f.read()
        idx = hash_text(path)
        record = {self.text_key: text}
        if self.filename_key:
            filename = os.path.basename(path)
            if self.remove_extension_in_filename:
                filename = os.path.splitext(filename)[0]
            record[self.filename_key] = filename
        return idx, record

@show_in_op_list
class WriteTxtFolder(OutputOp):
    "Write entries to a folder as txt files."
    def __init__(self, 
                directory: str, 
                text_key: str = "text",
                *,
                filename_key: str = "filename",
    ):
        super().__init__()
        self.directory = directory
        self.text_key = text_key
        self.filename_key = filename_key
    def _args_repr(self): return ReprUtil.repr_path(self.directory)
    def output_batch(self, batch: Dict[str, Entry]) -> None:
        os.makedirs(self.directory, exist_ok=True)
        for entry in batch.values():
            text = entry.data.get(self.text_key, "")
            filename = entry.data.get(self.filename_key, f"{entry.idx}.txt")
            if self.filename_key and not filename.endswith('.txt'):
                filename += '.txt'
            path = os.path.join(self.directory, filename)
            with open(path, 'w', encoding='utf-8') as f:
                f.write(text)
        print(f"[WriteTxtFolder]: Output {len(batch)} entries to {os.path.abspath(self.directory)}")

    def _args_repr(self): return ReprUtil.repr_glob(self.glob_str)



@show_in_op_list
class ReadMarkdownLines(ReaderOp):
    "Read Markdown files and extract non-empty lines as keyword with markdown headings as a list."
    def __init__(self,
                glob_str:str|Path,
                *,
                keyword_key = "keyword",
                headings_key = "headings",
                filename_key = "filename",
                remove_extension_in_filename = True,
                shuffle: bool = False,
                seed: int = 42,
                offset: int = 0,
                max_count: int = None,
                fire_once: bool = True,
                ):
        keys = [keyword_key, headings_key, filename_key]
        keys = [f for f in keys if f]
        super().__init__(keys=keys, shuffle=shuffle, seed=seed, offset=offset, max_count=max_count, fire_once=fire_once)
        self.glob_str = to_glob(glob_str)
        self.keyword_key = keyword_key
        self.headings_key = headings_key
        self.filename_key = filename_key
        self.remove_extension_in_filename = remove_extension_in_filename
    def _iter_record_proxy(self) -> Iterator[Tuple]:
        for path in sorted(glob(self.glob_str)):
            filename = os.path.basename(path)
            if self.remove_extension_in_filename:
                filename = os.path.splitext(filename)[0]
            for headings, keyword in iter_markdown_lines(path):
                yield filename, headings, keyword
    def _estimate_size(self) -> int:
        return sum(1 for _ in self._iter_record_proxy())
    def _load_and_process_record(self, record:Tuple):
        filename, headings, keyword = record
        idx = generate_idx_from_strings([filename]+headings+[keyword])
        record = {self.keyword_key: keyword}
        if self.headings_key:
            record[self.headings_key] = headings
        if self.filename_key:
            record[self.filename_key] = filename
        return idx, record




@show_in_op_list
class WriteMarkdownLines(OutputOp):
    """
    Write keyword lists to Markdown file(s) as lines, with heading hierarchy defined by headings:list.
    - if filename_key is provided, entries will be saved into different files based on the filename_key.
    """
    def __init__(self, 
                path_or_folder: str, 
                *,
                keyword_key = "keyword",
                headings_key = "headings",
                filename_key = None,
                sort: bool = False,
                ):
        super().__init__()
        self.path_or_folder = path_or_folder
        self.keyword_key = keyword_key
        self.headings_key = headings_key
        self.filename_key = filename_key
        self.sort = sort
    def _output_single_file(self, path, entries: Dict[str, Entry]) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        output_entries = []
        for entry in entries.values():
            headings = entry.data.get(self.headings_key, [])
            keyword = entry.data.get(self.keyword_key, "")
            if isinstance(headings, str):
                headings = [headings]
            headings = [h.strip() for h in headings]
            keyword = keyword.strip()
            if not keyword:
                continue
            output_entries.append((headings, keyword))
        if self.sort:
            output_entries.sort(key=lambda x: (build_sort_key_from_headings(x[0]), x[1]))
        write_markdown_lines(path, output_entries)
    def output_batch(self, batch):
        if self.filename_key is None:
            path = self.path_or_folder
            self._output_single_file(path, batch)
            print(f"[WriteMarkdownLines]: Output {len(batch)} entries to {os.path.abspath(path)}")
        else:
            batch_per_filename = {}
            for idx, entry in batch.items():
                filename = entry.data[self.filename_key]
                batch_per_filename.setdefault(filename, {})[idx] = entry
            for filename, entries in batch_per_filename.items():
                if os.path.splitext(filename)[1] == '':
                    filename += '.md'
                path = os.path.join(self.path_or_folder, filename)
                self._output_single_file(path, entries)
            print(f"[WriteMarkdownLines]: Output {len(batch)} entries to {os.path.abspath(self.path_or_folder)}")

@show_in_op_list
class ReadMarkdownEntries(ReaderOp):
    "Read Markdown files and extract nonempty text under every headings with markdown headings as a list."
    def __init__(self, 
                glob_str: str|Path,
                *,
                output_key = "text",
                headings_key = "headings",
                filename_key = "filename",
                remove_extension_in_filename = True,
                shuffle: bool = False,
                seed: int = 42,
                offset: int = 0,
                max_count: int = None,
                fire_once: bool = True,
                include_text_in_idx_hash = False,
                ):
        keys = [output_key, headings_key, filename_key]
        keys = [f for f in keys if f]
        super().__init__(keys=keys, shuffle=shuffle, seed=seed, offset=offset, max_count=max_count, fire_once=fire_once)
        self.glob_str = to_glob(glob_str)
        self.output_key = output_key
        self.headings_key = headings_key
        self.filename_key = filename_key
        self.remove_extension_in_filename = remove_extension_in_filename
        self.include_text_in_idx_hash = include_text_in_idx_hash
    def _args_repr(self): return ReprUtil.repr_glob(self.glob_str)
    def _iter_record_proxy(self):
        for path in sorted(glob(self.glob_str)):
            filename = os.path.basename(path)
            if self.remove_extension_in_filename:
                filename = os.path.splitext(filename)[0]
            for headings, text in iter_markdown_entries(path):
                if not text.strip():
                    continue
                yield filename, headings, text
    def _estimate_size(self) -> int:
        return sum(1 for _ in self._iter_record_proxy())
    def _load_and_process_record(self, record):
        filename, headings, text = record
        if self.include_text_in_idx_hash:
            idx = generate_idx_from_strings([filename] + headings + [text])
        else:
            idx = generate_idx_from_strings([filename] + headings)
        record = {self.output_key: text}
        if self.headings_key:
            record[self.headings_key] = headings
        if self.filename_key:
            record[self.filename_key] = filename
        return idx, record

@show_in_op_list
class WriteMarkdownEntries(OutputOp):
    """
    Write entries to Markdown file(s), with heading hierarchy defined by headings and text as content.
    - if filename_key is provided, entries will be saved into different files based on the filename_key.
    """
    def __init__(self, 
                 path_or_folder: str, 
                *,
                 output_key = "text",
                 headings_key = "headings",
                 filename_key = None,
                 sort: bool = False,
                 ):
        super().__init__()
        self.path_or_folder = path_or_folder
        self.output_key = output_key
        self.headings_key = headings_key
        self.filename_key = filename_key
        self.sort = sort
    def _output_single_file(self, path, entries: Dict[str, Entry]) -> None:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        output_entries = []
        for entry in entries.values():
            headings = entry.data.get(self.headings_key, [])
            text = entry.data.get(self.output_key, "")
            if isinstance(headings, str):
                headings = [headings]
            headings = [h.strip() for h in headings]
            text = escape_markdown_headings(text)
            output_entries.append((headings, text))
        if self.sort:
            output_entries.sort(key=lambda x: (build_sort_key_from_headings(x[0]), x[1]))
        write_markdown_entries(path, output_entries)
    def output_batch(self, batch):
        if self.filename_key is None:
            path = self.path_or_folder
            self._output_single_file(path, batch)
            print(f"[WriteMarkdownEntries]: Output {len(batch)} entries to {os.path.abspath(path)}")
        else:
            batch_per_filename = {}
            for idx, entry in batch.items():
                filename = entry.data[self.filename_key]
                batch_per_filename.setdefault(filename, {})[idx] = entry
            for filename, entries in batch_per_filename.items():
                if os.path.splitext(filename)[1] == '':
                    filename += '.md'
                path = os.path.join(self.path_or_folder, filename)
                self._output_single_file(path, entries)
            print(f"[WriteMarkdownEntries]: Output {len(batch)} entries to {os.path.abspath(self.path_or_folder)}")

@show_in_op_list
class SortMarkdownEntries(Sort):
    """
    Sort Markdown entries based on headings and (optional) keyword.
    """
    def __init__(self,
                *,
                headings_key: str = "headings",
                keyword_key: str = None,
                barrier_level = 1,
                ):
        super().__init__(custom_func=self._sort_key, barrier_level=barrier_level)
        self.headings_key = headings_key
        self.keyword_key = keyword_key
    def _sort_key(self, data: Dict) -> Tuple:
        headings = data.get(self.headings_key, [])
        if isinstance(headings, str):
            headings = [headings]
        headings = [h.strip() for h in headings]
        if self.keyword_key:
            keyword = data.get(self.keyword_key, "").strip()
            return (build_sort_key_from_headings(headings), keyword)
        else:
            return build_sort_key_from_headings(headings)

@show_in_op_list
class FromList(SourceOp):
    "Create entries from a list of dictionaries or objects, each representing an entry."
    def __init__(self,
                input_list: List[Dict]|List[Any],
                output_key: str = None,
                *,
                fire_once: bool = True,
                ):
        super().__init__(fire_once=fire_once)
        self.input_list = input_list
        self.output_key = output_key
    def set_input(self, input_list: List[Dict]|List[Any]) -> None:
        self.input_list = input_list
    def generate_batch(self) -> Iterator[Entry]:
        for obj in self.input_list:
            yield self._make_entry(obj)
    def _make_entry(self,obj:Entry|dict|int|float|str|bool)->Entry:
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

@show_in_op_list
class ToList(OutputOp):
    "Output a list of specific field(s) from entries."
    def __init__(self,*output_keys):
        super().__init__()
        self._output_entries = {}
        self.output_keys = KeysUtil.make_keys(output_keys) if output_keys else None
    def _args_repr(self): return ReprUtil.repr_keys(self.output_keys) if self.output_keys else ""
    def output_batch(self, batch: Dict[str, Entry]) -> None:
        for idx, entry in batch.items():
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
    
@show_in_op_list
class OutputEntries(OutputOp):
    "Output entries to a list."
    def __init__(self):
        super().__init__()
        self._output_entries = {}
    def output_batch(self, batch: Dict[str, Entry]) -> None:
        for idx, entry in batch.items():
            if idx in self._output_entries:
                if entry.rev < self._output_entries[idx].rev:
                    continue
            self._output_entries[idx] = entry
    def get_output(self) -> List[Entry]:
        return list(self._output_entries.values())


@show_in_op_list
class PrintEntry(OutputOp):
    "Print the first n entries information."
    def __init__(self,*,first_n=None):
        super().__init__()
        self.first_n = first_n
    def output_batch(self, batch: Dict[str, Entry]) -> None:
        if not batch: return
        for entry in list(batch.values())[:self.first_n]:
            print("idx:", entry.idx, "rev:", entry.rev)
            print(entry.data)
            print()
        print()

@show_in_op_list
class PrintField(OutputOp):
    "Print the specific field(s) from the first n entries."
    def __init__(self, field="text",*, first_n=5):
        super().__init__()
        self.field = field
        self.first_n = first_n
    def _args_repr(self): return ReprUtil.repr_str(self.field)
    def output_batch(self,batch:Dict[str,Entry])->None:
        if not batch: return
        for entry in list(batch.values())[:self.first_n]:
            print(f"Index: {entry.idx}, Revision: {entry.rev} Field: '{self.field}'")
            print(entry.data.get(self.field, None))
            print()
        print()




__all__ = [
    "ReaderOp",
    "WriteJsonl",
    "ReadJsonl",
    "ReadParquet",
    "ReadTxtFolder",
    "ReadMarkdownLines",
    "WriteMarkdownLines",
    "ReadMarkdownEntries",
    "WriteMarkdownEntries",
    "SortMarkdownEntries",
    "FromList",
    "ToList",
    "PrintEntry",
    "PrintField",
    "OutputEntries",
]