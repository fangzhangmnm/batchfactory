from typing import  List, Dict, Callable, Mapping, Iterable, Any, Set
import os
import jsonlines,json
import aiofiles,asyncio
from copy import deepcopy
from pathlib import Path
import sqlite3
import msgpack

DELETE_NONE=True
COMPACT_ON_INIT=True

class Ledger2:
    def __init__(self, path: str|Path):
        self.path = Path(path)
        if self.path.suffix == '.jsonl':
            print(f"[Ledger2] Warning: Ledger2 is designed to use SQLite, not JSONL. Converting {self.path} to SQLite format.")
        self.path = self.path.with_suffix('.sqlite')
        os.makedirs(self.path.parent, exist_ok=True)
        self.conn = sqlite3.connect(self.path)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.cursor = self.conn.cursor()
        self._lock = asyncio.Lock()
        self._create_table()
        self._upgrade_from_old_format()
        if COMPACT_ON_INIT:
            self.compact()
    def __del__(self):
        if self.conn:
            self.conn.commit()
            self.conn.close()
    def _create_table(self):
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS entries (
                idx TEXT PRIMARY KEY,
                data BLOB
            )
        ''')
        self.conn.commit()
    def compact(self):
        self.conn.execute('PRAGMA wal_checkpoint(TRUNCATE);')
        self.conn.commit()
    def update_many_sync(self,updates:Dict,serializer=None):
        for idx, record in updates.items():
            if serializer is not None:
                record = serializer(record)
            assert isinstance(record, dict), "Record must be a dictionary."
            assert idx == record['idx'], "Index must match record['idx']."
            data_blob = msgpack.packb(record, use_bin_type=True)
            self.cursor.execute('''
                INSERT OR REPLACE INTO entries (idx, data) VALUES (?, ?)
            ''', (idx, data_blob))
        self.conn.commit()
    async def update_one_async(self, new_record:Dict, serializer=None):
        if serializer is not None:
            new_record = serializer(new_record)
        assert isinstance(new_record, dict), "Record must be a dictionary."
        idx = new_record['idx']
        data_blob = msgpack.packb(new_record, use_bin_type=True)
        async with self._lock:
            self.cursor.execute('''
                INSERT OR REPLACE INTO entries (idx, data) VALUES (?, ?)
            ''', (idx, data_blob))
            self.conn.commit()
    def get_one(self, idx:str, builder=None, default=None) -> Dict|Any|None:
        self.cursor.execute('SELECT data FROM entries WHERE idx = ?', (idx,))
        row = self.cursor.fetchone()
        if row is None:
            return default
        data_blob = row[0]
        record = msgpack.unpackb(data_blob, raw=False)
        if builder is not None:
            record = builder(record)
        return record
    def get_all(self, builder=None)->Dict[str, Any]:
        self.cursor.execute('SELECT idx, data FROM entries')
        records = {}
        for idx, data_blob in self.cursor.fetchall():
            record = msgpack.unpackb(data_blob, raw=False)
            if builder is not None:
                record = builder(record)
            records[idx] = record
        return records
    def filter_many(self, criteria:Callable, builder:Callable=None) -> Dict[str, Any]:
        """returns a dict of records that satisfy criteria(record)==True"""
        records = {}
        self.cursor.execute('SELECT idx, data FROM entries')
        for idx, data_blob in self.cursor.fetchall():
            record = msgpack.unpackb(data_blob, raw=False)
            if builder is not None:
                record = builder(record)
            if criteria(record):
                records[idx] = record
        return records
    def contains(self, idx:str) -> bool:
        self.cursor.execute('SELECT 1 FROM entries WHERE idx = ?', (idx,))
        return self.cursor.fetchone() is not None
    def remove_many(self, idxs:Set):
        for idx in idxs:
            self.cursor.execute('DELETE FROM entries WHERE idx = ?', (idx,))
        self.conn.commit()
    def _upgrade_from_old_format(self):
        if self.path.with_suffix('.jsonl').exists():
            print(f"[Ledger2] Upgrading from old format at {self.path.with_suffix('.jsonl')}")
            with jsonlines.open(self.path.with_suffix('.jsonl'), 'r') as reader:
                for record in reader:
                    idx = record['idx']
                    data_blob = msgpack.packb(record, use_bin_type=True)
                    self.cursor.execute('''
                        INSERT OR REPLACE INTO entries (idx, data) VALUES (?, ?)
                    ''', (idx, data_blob))
            self.conn.commit()
            self.path.with_suffix('.jsonl').unlink()

__all__ = [
]
