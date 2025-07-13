from .._registery import show_in_op_list

from typing import List, Dict, NamedTuple, Set, Tuple, Any
import re

_compiled_re_cache: Dict[str, re.Pattern] = {}
def _get_compiled_re(pattern: str) -> re.Pattern:
    if isinstance(pattern, re.Pattern):
        return pattern
    if pattern not in _compiled_re_cache:
        _compiled_re_cache[pattern] = re.compile(pattern)
    return _compiled_re_cache[pattern]


def remove_speaker_tag(line):
    "Remove speaker tags."
    pattern = r'^\s*[*_~`]*\w+[*_~`]*[:：][*_~`]*\s*'
    return re.sub(pattern, '', line)

def split_cot(text)->Tuple[str,str]:
    "Split the LLM response into text and chain of thought (CoT)."
    cot = ""
    if "</think>" in text:
        cot, text = text.split("</think>", 1)
        if cot.strip().startswith("<think>"):
            cot = cot.strip()[len("<think>"):]
        text = text.lstrip()
    return text, cot.strip()

def remove_cot(text):
    "Remove the chain of thought (CoT) from the LLM response."
    return split_cot(text)[0]

def text_to_integer_list(text):
    return [int(i) for i in text.split()]

def discard_after(text, regex):
    "Discard text include and after the regex."
    regex = _get_compiled_re(regex)
    match = regex.search(text)
    if match:
        return text[:match.start()]
    return text

def get_first_regex_match(text: str, regex: str, default=""):
    regex = _get_compiled_re(regex)
    match = regex.search(text)
    if match:
        return match.group(0)
    return default



__all__ = [
    "remove_speaker_tag",
    "split_cot",
    "remove_cot",
    "text_to_integer_list",
    "discard_after",
    "get_first_regex_match",
]