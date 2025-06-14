from abc import ABC, abstractmethod
from typing import Dict, Any
from .utils import ReprUtil, get_format_keys


class LLMPromptMaker(ABC):
    PROMPT = "{text}"
    @abstractmethod
    def make_prompt(self, data:Dict[str, Any]) -> str:
        "self.PROMPT.format(**{k: data[k] for k in get_format_keys(self.PROMPT)})"
        pass
    @classmethod
    def from_prompt(cls, prompt:"str|LLMPromptMaker") -> "LLMPromptMaker":
        if isinstance(prompt, str):
            return BasicLLMPromptMaker(prompt)
        elif isinstance(prompt, LLMPromptMaker):
            return prompt
        else:
            raise ValueError(f"Unsupported prompt type: {type(prompt)}. Must be str or LLMPromptMaker.")
    def __repr__(self) -> str:
        return f"<{self.__class__.__name__}>"

class BasicLLMPromptMaker(LLMPromptMaker):
    def __init__(self, prompt:str):
        self.PROMPT = prompt
    def make_prompt(self, data:Dict[str, Any]) -> str:
        return self.PROMPT.format(**{k: data[k] for k in get_format_keys(self.PROMPT)})
    def __repr__(self) -> str:
        return ReprUtil.repr_str(self.PROMPT)


__all__=[
    "LLMPromptMaker",
    "BasicLLMPromptMaker",
]