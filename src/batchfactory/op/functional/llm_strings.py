from .._registery import show_in_op_list

from typing import List, Dict, NamedTuple, Set, Tuple, Any
import re

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
    return text, cot.strip()

def remove_cot(text):
    "Remove the chain of thought (CoT) from the LLM response."
    return split_cot(text)[0]

def text_to_integer_list(text):
    return [int(i) for i in text.split()]

__all__ = [
    "remove_speaker_tag",
    "split_cot",
    "remove_cot",
    "text_to_integer_list",
]