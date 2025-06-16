from typing import  List, Dict, Callable, Mapping, Iterable, Any, Set
import os
import jsonlines,json
import aiofiles,asyncio
from copy import deepcopy
from pathlib import Path

COMPACT_ON_RESUME=False
DELETE_NONE=True

class _Ledger:
    """Cache synced storage based on jsonlines and atomic append
    also supports compact and autocast on retrieve."""
    def __init__(self, path: str|Path):
        self.path = Path(path)
        os.makedirs(self.path.parent, exist_ok=True)
        self._index = {} # should be guarded by deepcopy
        self._lock = asyncio.Lock()
        self._load()
    def reset(self):
        self._index.clear()
    def resume(self):
        self._load()
        if COMPACT_ON_RESUME:
            self.compact()
    def compact(self):
        self.rewrite_cache()
    def rewrite_cache(self):
        self._rewrite_cache(self._index)


    def append_many(self, records:Dict, allow_override:bool=False,serializer=None):
        records = _apply_many(records, func=serializer)
        _check_input_many(records)
        for record in records.values():
            if not allow_override and record['idx'] in self._index:
                raise ValueError(f"Record {record['idx']} already exists.")
        for record in records.values():
            self._index[record['idx']] = deepcopy(record)
        self._append_cache(records)

    def update_one(self, new_record:Dict, compact=False, serializer=None):
        new_record = _apply_one(new_record, func=serializer)
        _check_input_one(new_record)
        self._update_many({new_record['idx']: new_record}, sync=True, compact=compact)

    def update_many(self,updates:Dict,compact=False,serializer=None):
        updates = _apply_many(updates, func=serializer)
        _check_input_many(updates)
        self._update_many(updates,sync=True,compact=compact)

    async def update_one_async(self,new_record:Dict,serializer=None):
        new_record = _apply_one(new_record, func=serializer)
        _check_input_one(new_record)
        await self._update_many({new_record['idx']:new_record},sync=False,compact=False)

    def filter_many(self, criteria ,builder=None)->Dict[str,Any]:
        """returns a dict of deepcopied records satisfies criteria(record)==True"""
        records={}
        for idx,record in self._index.items():
            record=_apply_one(deepcopy(record), func=builder) # note that now record might be any type, not necessarily a dict
            if criteria(record):
                records[idx] = record
        return records
    
    def get_one(self, idx:str, builder=None, default=None)->Dict|Any:
        """returns a deepcopy of the record"""
        record = self._index.get(idx, default)
        record = _apply_one(deepcopy(record), func=builder)
        return record
    
    def get_all(self, builder=None) -> Dict[str, Any]:
        records = {}
        for idx, record in self._index.items():
            record = _apply_one(deepcopy(record), func=builder)
            records[idx] = record
        return records


    def contains(self, idx:str) -> bool:
        return idx in self._index
    
    def remove_many(self, idxs:Set):
        for idx in set(idxs):
            if idx in self._index:
                del self._index[idx]
        self.rewrite_cache()


    def _update_many(self, updates:Dict, sync, compact=False):
        if (not sync) and compact: raise ValueError("Cannot compact while updating asynchronously.")
        for record in updates.values():
            _deep_update(self._index.setdefault(record['idx'],{}), record, delete_none=True, _deepcopy=True)
        if sync:
            if compact:
                self.rewrite_cache()
            else:
                self._append_cache(updates)
        else:
            return self._append_cache_async(updates)
        
    def _append_cache(self,records:Dict):
        with jsonlines.open(self.path, mode='a') as writer:
            for item in records.values():
                writer.write(item)

    def _rewrite_cache(self,records:Dict):
        tmp_path = self.path.with_suffix('.tmp')
        with jsonlines.open(tmp_path, mode='w') as writer:
            for item in records.values():
                writer.write(item)
        for attempts in range(10):
            try:
                os.replace(tmp_path, self.path)
                break
            except OSError as e:
                if attempts == 9:
                    raise e
                asyncio.sleep(0.1)

    async def _append_cache_async(self, records:Dict):
        text="".join(json.dumps(item, ensure_ascii=False) + "\n" for item in records.values())
        async with self._lock:
            async with aiofiles.open(self.path,mode='a',encoding='utf-8') as f:
                await f.write(text)

    def _load(self):
        self._index.clear()
        if not os.path.exists(self.path): 
            return
        with jsonlines.open(self.path, 'r') as reader:
            for item in reader:
                _deep_update(self._index.setdefault(item['idx'],{}), item, delete_none=True, _deepcopy=True)


def _apply_many(records:Dict, func:Callable|None=None) -> Dict:
    if func is not None:
        return {k: func(v) for k, v in records.items()}
    return records

def _apply_one(record:Dict, func:Callable|None=None) -> Dict:
    if func is not None:
        return func(record)
    return record

def _check_input_many(records:Dict):
    if not isinstance(records, dict):
        raise ValueError("Records must be a dictionary of {idx: record} pairs.")
    for dict_idx, record in records.items():
        if not isinstance(record, dict):
            raise ValueError(f"Please input a dictionary of records, not a single record")
        if "idx" not in record or record["idx"] != dict_idx:
            raise ValueError(f"Please make sure each record has an 'idx' field that matches the key in the records dictionary.")
        
def _check_input_one(record:Dict):
    if not isinstance(record, dict):
        raise ValueError("Record must be a dictionary.")
    if "idx" not in record:
        raise ValueError("Record must have an 'idx' field.")

def _deep_update(original:Dict, updates:Dict, delete_none:bool, _deepcopy:bool):
    for k,v in updates.items():
        if v is None and delete_none:
            original.pop(k, None)
        elif isinstance(v, dict) and isinstance(original.get(k), dict):
            _deep_update(original[k], v, delete_none=delete_none, _deepcopy=_deepcopy)
        else:
            if _deepcopy:
                original[k] = deepcopy(v)
            else:
                original[k] = v


__all__ = [
]
