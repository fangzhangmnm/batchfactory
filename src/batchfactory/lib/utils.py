import hashlib
from string import Formatter
from typing import Dict, Iterable, Union, List, Any, Literal, Tuple, Callable
from pydantic import BaseModel
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, fields
from copy import deepcopy
from typing import overload, get_args, get_origin, Tuple
import json
import itertools as itt
import inspect, ast, textwrap
from pathlib import Path
import os

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
    prompt = str(prompt)
    formatter= Formatter()
    keys=[]
    for _, key, _, _ in formatter.parse(prompt):
        if key:
            keys.append(key)
    return keys


def to_glob(glob_str: str|Path, default_extension: str = None) -> str:
    p = Path(glob_str)
    if '*' in str(p) or p.is_file():
        return str(p)
    elif p.exists():
        if p.is_file():
            return str(p)
        else:
            ext = default_extension.lstrip('.') if default_extension else ""
            return str(p / f"**/*.{ext}") if ext else str(p / "**")
    else:
        if p.suffix:
            return str(p)
        else:
            ext = default_extension.lstrip('.') if default_extension else ""
            return str(p / f"**/*.{ext}") if ext else str(p / "**")

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

class CollectionsUtil:
    @staticmethod
    def pivot_cascaded_dict(dict_:Dict[Any,Dict]):
        new_dict = {}
        for key1, value1 in dict_.items():
            for key2, value2 in value1.items():
                new_dict.setdefault(key2, {})[key1] = value2
        return new_dict
    @staticmethod
    def pivot_cascaded_list(list_:List[List]):
        return [list(x) for x in zip(*list_)]
    @staticmethod
    def is_list_like(x):
        return isinstance(x, Iterable) and not isinstance(x, (str,bytes,Mapping)) and not hasattr(x, '__fields__')
    @staticmethod
    def broadcast_lists(lists):
        lists = list(lists)
        max_len = max(len(lst) for lst in lists if CollectionsUtil.is_list_like(lst))
        for i in range(len(lists)):
            if not CollectionsUtil.is_list_like(lists[i]):
                lists[i] = [lists[i]] * max_len
        return lists

class KeysUtil:
    @staticmethod
    def read_dict(dict:Dict, keys:List[str])->Tuple[Any]:
        return tuple(dict.get(key) for key in keys)
    @staticmethod
    def write_dict(dict:Dict, keys:List[str], *values:Any):
        if len(values) != len(keys):
            raise ValueError(f"Expected {len(keys)} values, got {len(values)}.")
        for key, value in zip(keys, values):
            dict[key] = value
    @overload
    @staticmethod
    def make_keys(k1:str, *others, allow_empty=True)->List[str]: None
    @overload
    @staticmethod
    def make_keys(ks:Sequence[str], allow_empty=True)->List[str]: None
    @staticmethod
    def make_keys(*args, allow_empty=True)->List[str]:
        if len(args) == 0 and allow_empty:
            return []
        elif len(args) == 1 and CollectionsUtil.is_list_like(args[0]) and all(isinstance(k, str) for k in args[0]):
            return list(args[0])
        elif all(isinstance(k, str) for k in args):
            return list(args)
        else:
            raise TypeError("Invalid arguments for make_keys.")
    @overload
    @staticmethod
    def make_dict(k1:str, v1:Any, *others)->Dict[str, Any]: None
    @overload
    @staticmethod
    def make_dict(kv:Mapping[str, Any])->Dict[str, Any]: None
    @overload
    @staticmethod
    def make_dict(ks:Sequence[str], vs:Sequence[Any])->Dict[str, Any]: None
    @staticmethod
    def make_dict(*args)->Dict[str, Any]:
        if len(args) == 0:
            return {}
        elif len(args) == 1 and isinstance(args[0], Mapping):
            return dict(args[0])
        elif len(args) == 2 and CollectionsUtil.is_list_like(args[0]) and CollectionsUtil.is_list_like(args[1]) and len(args[0]) == len(args[1]):
            return {k: v for k, v in zip(args[0], args[1])}
        elif all(isinstance(k, str) for k in args[::2]) and len(args) % 2 == 0:
            return {args[i]: args[i + 1] for i in range(0, len(args), 2)}
        else:
            raise TypeError("Invalid arguments for make_dict.")
    @overload
    @staticmethod
    def make_io_keys(ins:Sequence[str], outs:Sequence[str])->Tuple[List[str], List[str]]: None
    @overload
    @staticmethod
    def make_io_keys(iomap:Mapping[str, str])->Tuple[List[str], List[str]]: None
    @overload
    @staticmethod
    def make_io_keys(ins:Sequence[str], out:str)->Tuple[List[str], List[str]]: None
    @overload
    @staticmethod
    def make_io_keys(ins:str, outs:Sequence[str])->Tuple[List[str], List[str]]: None
    @overload
    @staticmethod
    def make_io_keys(ins:str, outs:str)->Tuple[List[str], List[str]]: None
    @overload
    @staticmethod
    def make_io_keys(inouts:str) -> Tuple[List[str], List[str]]: None
    @overload
    @staticmethod
    def make_io_keys(inouts:Sequence[str]) -> Tuple[List[str], List[str]]: None
    @staticmethod
    def make_io_keys(*args,_suppress_error=False):
        if len(args) == 0:
            return [], []
        elif len(args) == 1 and isinstance(args[0], Mapping) and all(isinstance(k, str) for k in args[0]) and all(isinstance(v, str) for v in args[0].values()):
            return list(args[0].keys()), list(args[0].values())
        elif len(args) == 2 and CollectionsUtil.is_list_like(args[0]) and CollectionsUtil.is_list_like(args[1]) and all(isinstance(k, str) for k in args[0]) and all(isinstance(k, str) for k in args[1]):
            return list(args[0]), list(args[1])
        elif len(args) == 2 and isinstance(args[0], str) and CollectionsUtil.is_list_like(args[1]) and all(isinstance(k, str) for k in args[1]):
            return [args[0]], list(args[1])
        elif len(args) == 2 and CollectionsUtil.is_list_like(args[0]) and all(isinstance(k, str) for k in args[0]) and isinstance(args[1], str):
            return list(args[0]), [args[1]]
        elif len(args) == 2 and isinstance(args[0], str) and isinstance(args[1], str):
            return [args[0]], [args[1]]
        elif len(args) == 1 and isinstance(args[0], str):
            return [args[0]], [args[0]]
        elif len(args) == 1 and CollectionsUtil.is_list_like(args[0]) and all(isinstance(k, str) for k in args[0]):
            return list(args[0]), list(args[0])
        else:
            if not _suppress_error:
                return None
            else:
                raise TypeError("Invalid arguments for make_io_keys.")
    @staticmethod
    def make_keys_map(*args,non_overlapping=False)->Tuple[List[str],List[str]]:
        from_keys, to_keys = KeysUtil.make_io_keys(*args)
        if len(from_keys) != len(to_keys):
            raise ValueError("Invalid arguments for make_keys_map.")
        if non_overlapping:
            if set(from_keys) & set(to_keys):
                raise ValueError("make_keys_map requires unique froms and tos to avoid ambiguity.")
        return from_keys, to_keys
    
    @staticmethod
    def extract_out_list_from_func_return(tuple_or_list_or_any, out_keys:List[str]) -> Tuple[Any]:
        if len(out_keys) == 1: # should expect an object despite the return is a list (but raise error if returns a tuple with length more than 1
            if isinstance(tuple_or_list_or_any, tuple) and len(tuple_or_list_or_any) == 1:
                return (tuple_or_list_or_any[0],)
            elif isinstance(tuple_or_list_or_any, tuple) and len(tuple_or_list_or_any) > 1:
                raise ValueError(f"Expected a single return value, but got {len(tuple_or_list_or_any)} values.")
            elif tuple_or_list_or_any is None:
                raise ValueError("Expected a single return value, but got None.")
            else:
                return (tuple_or_list_or_any,)
        elif len(out_keys) == 0:
            if isinstance(tuple_or_list_or_any, tuple) and len(tuple_or_list_or_any) == 0: # do not auto unpack list
                return ()
            elif isinstance(tuple_or_list_or_any, tuple) and len(tuple_or_list_or_any) > 0:
                raise ValueError(f"Expected no return values, but got {len(tuple_or_list_or_any)} values.")
            elif tuple_or_list_or_any is None:
                return ()
            else:
                raise ValueError(f"Expected no return values, but got a {type(tuple_or_list_or_any).__name__}.")
        else:
            if isinstance(tuple_or_list_or_any, tuple) and len(tuple_or_list_or_any) == len(out_keys):
                return tuple(tuple_or_list_or_any)
            elif isinstance(tuple_or_list_or_any, tuple) and len(tuple_or_list_or_any) != len(out_keys):
                raise ValueError(f"Expected {len(out_keys)} return values, but got {len(tuple_or_list_or_any)} values.")
            elif tuple_or_list_or_any is None:
                raise ValueError(f"Expected {len(out_keys)} return values, but got None.")
            else:
                raise ValueError(f"Expected {len(out_keys)} return values, but got a {type(tuple_or_list_or_any).__name__}.")

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

class ReprUtil:
    @staticmethod
    def repr_lambda(lambda_func: Callable) -> str:
        if hasattr(lambda_func, '__name__') and lambda_func.__name__ != '<lambda>':
            return lambda_func.__name__
        try:
            # pull out the source and normalize indentation
            source_lines, _ = inspect.getsourcelines(lambda_func)
            src = textwrap.dedent(''.join(source_lines))
            # parse it and look for the first Lambda node
            tree = ast.parse(src)
            for node in ast.walk(tree):
                if isinstance(node, ast.Lambda):
                    # ast.get_source_segment gives you exactly the lambda’s text
                    snippet = ast.get_source_segment(src, node)
                    return snippet if len(snippet) <= 50 else snippet[:47] + '...'
        except (OSError, TypeError):
            pass
        return '<lambda>'
    @staticmethod
    def repr_keys(keys: Iterable[str]) -> str:
        return ', '.join(f"'{key}'" for key in keys)
    @staticmethod
    def repr_item(obj: Any, max_len=10) -> str:
        if isinstance(obj,(int,bool,float)):
            return repr(obj)
        elif isinstance(obj, str):
            return ReprUtil.repr_str(obj,max_len=max_len)
        else:
            return f"<{type(obj).__name__}>"
    @staticmethod
    def repr_dict(dict_: Dict[str, Any]) -> str:
        return "{"+', '.join(f"'{k}': {ReprUtil.repr_item(v)}" for k, v in dict_.items())+ "}"
    @staticmethod
    def repr_dict_from_tuples(froms,tos):
        return ReprUtil.repr_dict({k:v for k,v in zip(froms,tos)})
    @staticmethod
    def repr_str(s:str,max_len=35) -> str:
        s= s.replace("'", "\\'").replace('\n', '\\n').replace('\r', '\\r')
        return f"'{s[:max_len]}...'" if len(s) > max_len else f"'{s}'"
    @staticmethod
    def repr_path(p:str|Path,max_len=35) -> str:
        s = Path(p).name
        return ReprUtil.repr_str(s, max_len=max_len)
    @staticmethod
    def repr_glob(g:str|Path,max_len=35) -> str:
        if isinstance(g, Path): g = str(g)
        g = g.rsplit('\\', 1)[-1].rsplit('/', 1)[-1]
        return ReprUtil.repr_str(g, max_len=max_len)




__all__ = [
    "format_number",
    "TokenCounter",
    "hash_text",
    "hash_texts",
    "hash_json",
    "get_format_keys",
    "IOKeysInput",
    "KeysUtil",
    "CollectionsUtil",
    "ReprUtil",
    "to_glob",
]