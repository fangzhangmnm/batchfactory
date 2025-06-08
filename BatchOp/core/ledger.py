from typing import  List, Dict, Callable, Mapping, Iterable, Any
import os
import jsonlines,json
import aiofiles,asyncio
from copy import deepcopy
from ..lib.utils import _to_list_2, _deep_update

COMPACT_ON_RESUME=False
DELETE_NONE=True

class _Ledger:
    """Cache synced storage based on jsonlines and atomic append
    also supports compact and autocast on retrieve."""
    def __init__(self, cache_path: str):
        self.cache_path = cache_path
        os.makedirs(os.path.dirname(cache_path), exist_ok=True)
        self._index = {} # should be guarded by deepcopy
        self._lock = asyncio.Lock()
        self._load()

    def resume(self):
        self._load()
        if COMPACT_ON_RESUME:
            self.compact()
    def compact(self):
        self.rewrite_cache()
    def rewrite_cache(self):
        self._rewrite_cache(list(self._index.values()))

    def append(self, records:Any|List, allow_override:bool=False,serializer=None):
        records=_to_list_2(records)
        if serializer is not None:
            records = [serializer(item) for item in records]
        for item in records:
            if not allow_override and item['idx'] in self._index:
                raise ValueError(f"Record {item['idx']} already exists.")
        for item in records:
            self._index[item['idx']] = deepcopy(item)
        self._append_cache(records)
    def update(self,updates:Dict|List,compact=False,serializer=None):
        if serializer is not None:
            updates = [serializer(item) for item in _to_list_2(updates)]
        self._update_many(updates,sync=True,compact=compact)
    async def update_async(self,updates:Dict|List,serializer=None):
        if serializer is not None:
            updates = [serializer(item) for item in _to_list_2(updates)]
        await self._update_many(updates,sync=False,compact=False)
    def filter(self, predicate:Callable[[Dict|Any], bool],builder=None)->List[Dict|Any]:
        """returns a list of deepcopied records that match the predicate function"""
        if builder is None: builder = lambda x: x
        return [builder(deepcopy(item)) for item in self._index.values() if predicate(builder(item))]
    def get(self, idx:str|List,builder=None)->Dict|Any|List[Dict|Any]:
        """returns a deepcopy of the record or a list of such records"""
        if builder is None: builder = lambda x: x
        if isinstance(idx, str):
            return builder(deepcopy(self._index.get(idx, None)))
        elif isinstance(idx, list):
            return [builder(deepcopy(self._index.get(i, None))) for i in idx]
        
    def contains(self, idx:str) -> bool:
        return idx in self._index
    def remove(self, idx:str|List):
        """removes a record or a list of records by idx"""
        idx= _to_list_2(idx)
        for i in idx:
            if i in self._index:
                del self._index[i]
        self.rewrite_cache()

    def _update_many(self, updates:Dict|List, sync=True, compact=False):
        if (not sync) and compact: raise ValueError("Cannot compact while updating asynchronously.")
        updates= _to_list_2(updates)
        for item in updates:
            _deep_update(self._index.setdefault(item['idx'],{}), deepcopy(item), delete_none=DELETE_NONE)
        if sync:
            if compact:
                self.rewrite_cache()
            else:
                self._append_cache(updates)
        else:
            return self._append_cache_async(updates)
    def _append_cache(self,records:Dict|List):
        records = _to_list_2(records)
        with jsonlines.open(self.cache_path, mode='a') as writer:
            for item in records:
                writer.write(item)
    def _rewrite_cache(self,records:Dict|List):
        records = _to_list_2(records)
        tmp_path = self.cache_path + '.tmp'
        with jsonlines.open(tmp_path, mode='w') as writer:
            for item in records:
                writer.write(item)
        os.replace(tmp_path, self.cache_path)
    async def _append_cache_async(self, records:Dict|List):
        records = _to_list_2(records)
        text="".join(json.dumps(item) + "\n" for item in records)
        async with self._lock:
            async with aiofiles.open(self.cache_path,mode='a',encoding='utf-8') as f:
                await f.write(text)
    def _load(self):
        self._index.clear()
        if not os.path.exists(self.cache_path): 
            return
        with jsonlines.open(self.cache_path, 'r') as reader:
            for item in reader:
                _deep_update(self._index.setdefault(item['idx'],{}), deepcopy(item), delete_none=DELETE_NONE)


__all__ = [
    '_Ledger',
]

