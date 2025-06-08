import hashlib
from string import Formatter
from typing import Dict, Iterable, Union, List
from pydantic import BaseModel
from collections.abc import Mapping
from dataclasses import dataclass, fields
from copy import deepcopy

def format_number(val):
    # use K M T Y
    if val<1e3: return str(val)
    elif val<1e6: return f'{val/1e3:.1f}K'
    elif val<1e9: return f'{val/1e6:.1f}M'
    elif val<1e12: return f'{val/1e9:.1f}B'
    else: return f'{val/1e12:.1f}T'

class TokenCounter:
    def __init__(self):
        self.input_tokens = 0
        self.output_tokens = 0
        self.total_price = 0
    def reset_counter(self):
        self.input_tokens = 0
        self.output_tokens = 0
        self.total_price = 0
    def summary(self)->str:
        rtval = f"{format_number(self.input_tokens)}↑ {format_number(self.output_tokens)}↓"
        if self.total_price > 0:
            rtval += f" ${self.total_price:.2f}"
        return rtval
    def update(self, input_tokens, output_tokens, input_price_M=0, output_price_M=0):
        self.input_tokens += input_tokens
        self.output_tokens += output_tokens
        self.total_price += (input_tokens / 1e6) * input_price_M + (output_tokens / 1e6) * output_price_M

def hash_text(text,*args):
    if args:
        text='@'.join(f"{len(arg)}:{arg}" for arg in (text, *args))
    return hashlib.sha256(text.encode('utf-8')).hexdigest()

def get_format_keys(prompt):
    formatter= Formatter()
    keys=[]
    for _, key, _, _ in formatter.parse(prompt):
        if key:
            keys.append(key)
    return keys



def _to_record(obj:BaseModel|Dict):
    if isinstance(obj, BaseModel):
        return obj.model_dump()
    return obj
def _to_BaseModel(obj, cls=None, allow_None=True) -> BaseModel|None:
    if obj is None and allow_None: return None
    elif cls and issubclass(cls, BaseModel): return cls.model_validate(obj)
    else: return obj
def _is_batch(x):
    return isinstance(x, Iterable) and not isinstance(x, (str,bytes,Mapping)) and not hasattr(x, '__fields__')
# def _to_list(items):
#     return items if _is_batch(items) else [items]
def _to_list_2(x):
    if x is None or x==[]: return []
    if _is_batch(x): return list(x)
    else: return [x]
def _make_list_of_list(x):
    if x is None or x==[]: return [[]]
    if not isinstance(x, list): return [[x]]
    elif not isinstance(x[0], list): return [x]
    else: return x
def _dict_to_dataclass(d:Dict, cls):
    field_names={f.name for f in fields(cls)}
    filtered_dict = {k: v for k, v in d.items() if k in field_names}
    return cls(**filtered_dict)

def _deep_update(original:Dict, updates:Dict, delete_none:bool=False):
    for k,v in updates.items():
        if v is None and delete_none:
            original.pop(k, None)
        elif isinstance(v, dict) and isinstance(original.get(k), dict):
            _deep_update(original[k], v, delete_none=delete_none)
        else:
            original[k] = deepcopy(v)

def _number_dict_to_list(d:Dict, default_value=None) -> list:
    if not d:return []
    max_key= max(d.keys(), default=0)
    return [d.get(i, default_value) for i in range(max_key + 1)]

def _setdefault_hierarchy(dict,path:List[str],default=None):
    current = dict
    for key in path[:-1]:
        if key not in current:
            current[key] = {}
        current = current[key]
    return current.setdefault(path[-1], default)

class FieldsRouter:
    """Usage:
        FieldsRouter([1,2],[3])
        FieldsRouter({'a': 'b', 'c': 'd'})
        FieldsRouter("input",["output1", "output2"])
        FieldsRouter("input")
    """
    froms: List[str]
    tos: List[str]
    def __init__(self,*args):
        if len(args)>2:raise ValueError("FieldsRouter accepts 1 or 2 arguments.")
        if len(args)==1:args=(args[0], [])
        source, other_source = args
        if isinstance(source,FieldsRouter):
            self.froms,self.tos = source.froms,source.tos
        elif isinstance(source,dict):
            self.froms, self.tos = zip(*source.items())
            self.froms, self.tos = list(self.froms), list(self.tos)
        else:
            self.froms = _to_list_2(source)
            self.tos = _to_list_2(other_source)
    def read_tuple(self, entry:Dict) -> tuple:
        return tuple(entry[field] for field in self.froms)
    def write_tuple(self, entry:Dict, values:tuple):
        values = _to_list_2(values)
        if len(values) != len(self.tos):
            raise ValueError(f"Expected {len(self.tos)} values, got {len(values)}.")
        for field, value in zip(self.tos, values):
            entry[field] = value
        


__all__ = [
    "format_number",
    "TokenCounter",
    "hash_text",
    "get_format_keys",
    "_to_record",
    "_to_BaseModel",
    "_is_batch",
    # "_to_list",
    "_to_list_2",
    "_make_list_of_list",
    "_dict_to_dataclass",
    "_deep_update",
    "_number_dict_to_list",
    "_setdefault_hierarchy",
    "FieldsRouter"
]