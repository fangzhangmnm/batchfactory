from .model_list import model_desc, client_desc
from .utils import format_number, TokenCounter, hash_text

from openai import OpenAI,AsyncOpenAI
from typing import Union, Dict, Tuple
import os
from functools import lru_cache
from pydantic import BaseModel
from typing import List, Union, Iterable, Dict, Tuple
from asyncio import Lock


class LLMMessage(BaseModel):
    role: str
    content: str

class LLMRequest(BaseModel):
    custom_id: str
    messages: List[LLMMessage]
    model: str
    max_completion_tokens: int


class LLMResponse(BaseModel):
    custom_id: str
    model: str
    message: LLMMessage
    prompt_tokens: int
    completion_tokens: int

def get_provider_name(model:str) -> str:
    return model.split('@', 1)[-1] if '@' in model else 'openai'
def get_model_name(model:str) -> str:
    return model.split('@', 1)[0] if '@' in model else model

class OpenAIClientCache:
    def __init__(self):
        self.clients = {}
        self.lock = Lock()
    def _create_client(self, provider:str, async_:bool=False) -> Union[OpenAI, AsyncOpenAI]:
        if provider not in client_desc:
            raise ValueError(f"Provider {provider} is not supported.")
        factory = AsyncOpenAI if async_ else OpenAI
        client_info = client_desc[provider]
        base_url = client_info.get('base_url', None)
        api_key = os.getenv(client_info['api_key_environ'])
        if not api_key:
            raise ValueError(f"API key for {provider} is not set in environment variables.")
        return factory(api_key=api_key, base_url=base_url)
    def get_client(self, provider:str, async_:bool=False) -> Union[OpenAI, AsyncOpenAI]:
        if (provider,async_) not in self.clients:
            self.clients[(provider, async_)] = self._create_client(provider, async_)
        return self.clients[(provider, async_)]
    async def get_client_async(self, provider:str, async_:bool=True) -> Union[OpenAI, AsyncOpenAI]:
        async with self.lock:
            return self.get_client(provider, async_=async_)
    def get_price_M(self, model:str):
        if model not in model_desc:
            raise ValueError(f"Model {model} is not supported.")
        return model_desc[model]['price_per_input_token_M'], model_desc[model]['price_per_output_token_M']


def immediately_query(query_str:str,model:str,max_tokens:int=4096,token_counter:TokenCounter=None) -> str:
    client = openai_client_cache.get_client(get_provider_name(model), async_=False)
    completion = client.chat.completions.create(
        model=get_model_name(model),
        messages=[
            {"role": "user", "content": query_str},
        ],
        max_completion_tokens=max_tokens
    )
    if token_counter:
        input_price_M, output_price_M = openai_client_cache.get_price_M(model)
        token_counter.update(
            input_tokens=completion.usage.input_tokens,
            output_tokens=completion.usage.output_tokens,
            input_price_M=input_price_M,
            output_price_M=output_price_M
        )
    return completion.choices[0].message.content

openai_client_cache = OpenAIClientCache()

__all__ = [
    "LLMMessage",
    "LLMRequest",
    "LLMResponse",
    "openai_client_cache",
    "immediately_query",
    "get_provider_name",
    "get_model_name",
]