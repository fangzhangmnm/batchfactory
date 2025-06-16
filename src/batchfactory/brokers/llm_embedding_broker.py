from .concurrent_api_call_broker import ConcurrentAPICallBroker
from ..core.broker import BrokerJobRequest, BrokerJobResponse, BrokerJobStatus
from ..lib.llm_backend import *
from openai import AsyncOpenAI
from openai.types.chat import ChatCompletion

from typing import List,Iterable,Dict
import asyncio
from tqdm.auto import tqdm

class LLMEmbeddingBroker(ConcurrentAPICallBroker):
    def __init__(self, 
                cache_path:str,
                *,
                concurrency_limit:int=100,
                rate_limit:int=100,
                max_number_per_batch:int=None
    ):
        super().__init__(cache_path=cache_path,
                         request_cls=LLMEmbeddingRequest,
                         response_cls=LLMEmbeddingResponse,
                         concurrency_limit=concurrency_limit,
                         rate_limit=rate_limit,
                         max_number_per_batch=max_number_per_batch
        )
        self.token_counter = LLMTokenCounter()
    async def _call_api_async(self, request: BrokerJobRequest, mock: bool) -> BrokerJobResponse:
        embedding_request: LLMEmbeddingRequest = request.request_object
        embedding_response = await get_llm_embedding_async(embedding_request, mock=mock)
        return BrokerJobResponse(
            job_idx=request.job_idx,
            status=BrokerJobStatus.DONE if embedding_response else BrokerJobStatus.FAILED,
            response_object=embedding_response,
        )
    async def _update_statistics(self, pbar: tqdm | None,
                                 request: BrokerJobRequest, response: BrokerJobResponse):
        if response.response_object is not None:
            embedding_response: LLMEmbeddingResponse = response.response_object
            self.token_counter.update(
                input_tokens=embedding_response.prompt_tokens,
                output_tokens=0,
                cost=embedding_response.cost,
            )
        if pbar:
            pbar.set_postfix_str(self.token_counter.get_summary_str())
            pbar.update(1)
    async def _output_and_reset_statistics(self):
        print(f"Token usage: {self.token_counter.get_summary_str()}")
        self.token_counter.reset()

__all__ = [
    "LLMEmbeddingBroker",
]




    
