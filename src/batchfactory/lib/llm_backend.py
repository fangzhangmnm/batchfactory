from .model_list import model_desc, client_desc
from .utils import format_number, hash_text

from openai import OpenAI,AsyncOpenAI
from typing import Union, Dict, Tuple, Literal
import os
from functools import lru_cache
from pydantic import BaseModel
from typing import List, Union, Iterable, Dict, Tuple
import asyncio
from asyncio import Lock
import numpy as np
from .base64_utils import encode_ndarray
from enum import Enum

def get_provider_name(model:str) -> str:
    return model.split('@', 1)[-1]
def get_model_name(model:str) -> str:
    return model.split('@', 1)[0]
def get_model_provider_str(model,provider):
    return model+'@'+provider if '@' not in model else model

class LLMClientHub:
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
    async def get_client_async(self, provider:str, async_client:bool=True) -> Union[OpenAI, AsyncOpenAI]:
        async with self.lock:
            return self.get_client(provider, async_=async_client)
    def get_price_M(self, model:str, is_batch=False):
        if model not in model_desc:
            raise ValueError(f"Model {model} is not supported.")
        input_price_M = self.get_property(model, 'price_per_input_token_M', 0.0)
        output_price_M = self.get_property(model, 'price_per_output_token_M', 0.0)
        batch_discount = self.get_property(model, 'batch_price_discount', 1.0)
        if is_batch:
            input_price_M, output_price_M = input_price_M * batch_discount, output_price_M * batch_discount
        return input_price_M, output_price_M
    def get_property(self, model:str, property_name:str, default=None):
        if model not in model_desc:
            raise ValueError(f"Model {model} is not supported.")
        return model_desc[model].get(property_name, default)
    def is_chat_completion_model(self, model:str) -> bool:
        return self.get_property(model, 'chat_completions', False)
    def is_embedding_model(self, model:str) -> bool:
        return self.get_property(model, 'embeddings', False)

    def list_all_models(self, *, endpoint:str=None, provider:str=None) -> List[str]:
        models = []
        for model, desc in model_desc.items():
            if endpoint and desc.get(endpoint, False) is False:
                continue
            if provider and get_provider_name(model) != provider:
                continue
            models.append(model)
        return models

llm_client_hub = LLMClientHub()

def list_all_models(*,endpoint:str=None, provider:str=None) -> List[str]:
    return llm_client_hub.list_all_models(endpoint=endpoint, provider=provider)

def compute_llm_cost(prompt_tokens:int, completion_tokens:int, model:str, is_batch=False) -> float:
    input_price_M, output_price_M = llm_client_hub.get_price_M(model, is_batch=is_batch)
    total_cost = (prompt_tokens * input_price_M + completion_tokens * output_price_M) / 1e6
    return total_cost

class LLMTokenCounter:
    def __init__(self):
        self.input_tokens = 0
        self.output_tokens = 0
        self.total_price = 0
    def reset(self):
        self.input_tokens = 0
        self.output_tokens = 0
        self.total_price = 0
    def get_summary_str(self)->str:
        rtval = f"{format_number(self.input_tokens)}↑ {format_number(self.output_tokens)}↓"
        if self.total_price > 0:
            rtval += f" ${self.total_price:.2f}"
        return rtval
    def update(self, input_tokens, output_tokens, cost):
        self.input_tokens += input_tokens
        self.output_tokens += output_tokens
        self.total_price += cost

class LLMMessage(BaseModel):
    role: str
    content: str

class LLMRequest(BaseModel):
    custom_id: str
    model: str # model@provider
    messages: List[LLMMessage]
    max_completion_tokens: int
    estimated_prompt_tokens: int|None = None

class LLMResponse(BaseModel):
    custom_id: str
    model: str # model@provider
    message: LLMMessage
    prompt_tokens: int
    completion_tokens: int
    cost: float

async def get_llm_response_async(llm_request:LLMRequest, mock=False)->LLMResponse:
    if not llm_client_hub.get_property(llm_request.model, 'chat_completions',False):
        raise ValueError(f"Model {llm_request.model} does not support chat completions.")
    if mock:
        await asyncio.sleep(0.1)
        return _get_dummy_llm_response(llm_request)
    client:AsyncOpenAI = await llm_client_hub.get_client_async(get_provider_name(llm_request.model), async_client=True)
    completion = await client.chat.completions.create(
        model=get_model_name(llm_request.model),
        messages=llm_request.messages,
        max_completion_tokens=llm_request.max_completion_tokens,
    )
    return LLMResponse(
        custom_id=llm_request.custom_id,
        model=llm_request.model,
        message=LLMMessage(
            role=completion.choices[0].message.role,
            content=completion.choices[0].message.content
        ),
        prompt_tokens=completion.usage.prompt_tokens,
        completion_tokens=completion.usage.completion_tokens,
        cost=compute_llm_cost(
            prompt_tokens=completion.usage.prompt_tokens,
            completion_tokens=completion.usage.completion_tokens,
            model=llm_request.model,
            is_batch=False
        )
    )

def _get_dummy_llm_response(llm_request:LLMRequest) -> LLMResponse:
    return LLMResponse(
        custom_id=llm_request.custom_id,
        model=llm_request.model,
        message=LLMMessage(
            role='assistant',
            content=f"Dummy response for {llm_request.custom_id}"
        ),
        prompt_tokens=1,
        completion_tokens=1,
        cost=compute_llm_cost(
            prompt_tokens=1,
            completion_tokens=1,
            model=llm_request.model,
            is_batch=False
        )
    )

class LLMEmbeddingRequest(BaseModel):
    custom_id: str
    model: str # model@provider
    input_text: str
    dimensions: int|None = None
    dtype: Literal['float32', 'float16'] = 'float32'

class LLMEmbeddingResponse(BaseModel):
    custom_id: str
    model: str # model@provider
    embedding_base64: Dict
    dimensions: int
    dtype: Literal['float32', 'float16']
    prompt_tokens: int
    cost: float

async def get_llm_embedding_async(llm_embedding_request:LLMEmbeddingRequest, mock=False) -> LLMEmbeddingResponse:
    client:AsyncOpenAI = await llm_client_hub.get_client_async(get_provider_name(llm_embedding_request.model), async_client=True)
    if not llm_client_hub.get_property(llm_embedding_request.model, 'embeddings', False):
        raise ValueError(f"Model {llm_embedding_request.model} does not support embeddings.")
    dimensions = llm_embedding_request.dimensions
    client_dimensions = llm_client_hub.get_property(llm_embedding_request.model, 'embedding_dimension', 0)
    client_custom_dimension = llm_client_hub.get_property(llm_embedding_request.model, 'custom_embedding_dimension', False)
    if not client_custom_dimension and dimensions is not None and dimensions != client_dimensions:
        raise ValueError(f"Model {llm_embedding_request.model} does not support custom embedding dimensions. Expected {client_dimensions}, got {dimensions}.")
    
    if mock:
        await asyncio.sleep(0.1)
        return get_dummy_llm_embedding_response(llm_embedding_request)

    args = {}
    args['model'] = get_model_name(llm_embedding_request.model)
    args['input'] = llm_embedding_request.input_text

    if dimensions != client_dimensions and client_custom_dimension:
        args['dimensions'] = dimensions
    
    embedding = await client.embeddings.create(
        **args
    )

    embedding_data = embedding.data[0].embedding
    embedding_array = np.array(embedding_data, dtype=llm_embedding_request.dtype)
    embedding_base64 = encode_ndarray(embedding_array)
    cost = compute_llm_cost(
        prompt_tokens=embedding.usage.prompt_tokens,
        completion_tokens=0,
        model=llm_embedding_request.model,
        is_batch=False
    )

    return LLMEmbeddingResponse(
        custom_id=llm_embedding_request.custom_id,
        model=llm_embedding_request.model,
        embedding_base64=embedding_base64,
        dimensions=embedding_array.shape[0],
        dtype=llm_embedding_request.dtype,
        prompt_tokens=embedding.usage.prompt_tokens,
        cost=cost
    )

def get_dummy_llm_embedding_response(llm_embedding_request:LLMEmbeddingRequest) -> LLMEmbeddingResponse:
    dimensions = llm_embedding_request.dimensions or llm_client_hub.get_property(llm_embedding_request.model, 'embedding_dimension', 1536)
    dummy_embedding = np.ones(dimensions, dtype=llm_embedding_request.dtype)
    dummy_embedding_base64 = encode_ndarray(dummy_embedding)
    return LLMEmbeddingResponse(
        custom_id=llm_embedding_request.custom_id,
        model=llm_embedding_request.model,
        embedding_base64=dummy_embedding_base64,
        dimensions=dummy_embedding.shape[0],
        dtype=llm_embedding_request.dtype,
        prompt_tokens=1,
        cost=compute_llm_cost(
            prompt_tokens=1,
            completion_tokens=0,
            model=llm_embedding_request.model,
            is_batch=False
        )
    )

__all__ = [
    "LLMMessage",
    "LLMRequest",
    "LLMResponse",
    "LLMEmbeddingRequest",
    "LLMEmbeddingResponse",
    "LLMTokenCounter",
    "llm_client_hub",
    "list_all_models",
    "get_llm_response_async",
    "get_llm_embedding_async",
]