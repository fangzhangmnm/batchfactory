import hashlib
from string import Formatter
from typing import Dict, Iterable, Union, List, Any, Literal, Tuple
from pydantic import BaseModel
from collections.abc import Mapping
from dataclasses import dataclass, fields
from copy import deepcopy
import json

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

def hash_texts(*args):
    text = '@'.join(f"{len(arg)}:{arg}" for arg in args)
    return hash_text(text)

def hash_json(json_obj)->str:
    return hash_text(json.dumps(json_obj, sort_keys=True))

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

def _pivot_cascaded_dict(dict):
    new_dict = {}
    for key1,value1 in dict.items():
        for key2,value2 in value1.items():
            new_dict.setdefault(key2, {})[key1] = value2
    return new_dict


class FieldsRouter:
    """
    Usage:
    - `FieldsRouter({'k1':'v2','k2':'v2'},style="map")`
    - `FieldsRouter(['in1','in2','in3'],['out1','out2'],style="inout")`
    - `FieldsRouter('in1','in2','in3',style="tuple")`
    - `FieldsRouter('k1','v1','k2','v2',style="map")`
    - `FieldsRouter('in1','out1',style="inout")` (more than 2 args are ambiguous in "inout" style)
    """
    froms: List[str]
    tos: List[str]
    def __init__(self,*args,style:Literal["map","tuple","inout"],in_types=(str,), out_types=(str,)):
        if style not in ["map", "tuple", "inout"]:
            raise ValueError("prefers must be one of 'map', 'tuple', or 'inout'.")
        if len(args) == 0:
            self.froms, self.tos = [], []
        elif isinstance(args[0], FieldsRouter):
            if len(args) > 1: raise ValueError("Invalid FieldsRouter arguments: "+str(args))
            self.froms, self.tos = args[0].froms, args[0].tos
        elif isinstance(args[0], dict):
            if len(args) > 1: raise ValueError("Invalid FieldsRouter arguments: "+str(args))
            self.froms, self.tos = zip(*args[0].items())
        elif isinstance(args[0], list):
            if len(args) > 2: raise ValueError("Invalid FieldsRouter arguments: "+str(args))
            elif len(args) == 2:
                if isinstance(args[1], list):
                    self.froms, self.tos = args[0], args[1]
                else:
                    self.froms, self.tos = args[0], [args[1]]
            else:
                self.froms, self.tos = args[0], []
        else: # we are sure args[0] should be a key now
            if len(args) == 1:
                self.froms, self.tos = [args[0]], []
            elif isinstance(args[1], list):
                if len(args) > 2: raise ValueError("Invalid FieldsRouter arguments: "+str(args))
                self.froms, self.tos = [args[0]], args[1]
            else: # we are sure both args[0] and args[1] should be keys now
                if style == "inout":
                    if len(args) > 2: raise ValueError("Invalid FieldsRouter arguments: "+str(args))
                    self.froms, self.tos = [args[0]], [args[1]]
                elif style == "map":
                    if len(args)%2 != 0: raise ValueError("Invalid FieldsRouter arguments: "+str(args))
                    self.froms, self.tos = args[0::2], args[1::2]
                elif style == "tuple":
                    self.froms, self.tos = args, []
        if in_types and not all(isinstance(k, in_types) for k in self.froms):
            raise ValueError(f"FieldsRouter expects all froms to be of type {in_types}, got {self.froms}")
        if out_types and not all(isinstance(k, out_types) for k in self.tos):
            raise ValueError(f"FieldsRouter expects all tos to be of type {out_types}, got {self.tos}")
        if style == "tuple" and len(self.tos)>0:
            raise ValueError("FieldsRouter expects a tuple")
        if style == "map" and len(self.froms) != len(self.tos):
            raise ValueError("FieldsRouter expects a 1-to-1 mapping")
    def read_tuple(self, entry:Dict)->Tuple:
        return tuple(entry[field] for field in self.froms)
    def write_tuple(self, entry:Dict, *values)->None:
        if len(values) != len(self.tos):
            raise ValueError(f"Expected {len(self.tos)} values, got {len(values)}.")
        for field, value in zip(self.tos, values):
            entry[field] = value

def _number_to_label(n: int) -> str:
    label = ""
    while n > 0:
        n, remainder = divmod(n - 1, 26)
        label = chr(65 + remainder) + label
    return label

def _pick_field_or_value_strict(dict,field:str|None,value:Any|None=None,default=None):
    if field is not None and value is not None: raise ValueError("Only one of field or value should be provided.")
    if field is not None: return dict[field]
    if value is not None: return value
    if default is not None: return default
    raise ValueError("Either field, value or default must be provided.")


__all__ = [
    "format_number",
    "TokenCounter",
    "hash_text",
    "hash_texts",
    "hash_json",
    "get_format_keys",
    "FieldsRouter",
]