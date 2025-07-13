from .concurrent_api_call_broker import ConcurrentAPICallBroker
from ..core.broker import BrokerJobRequest, BrokerJobResponse, BrokerJobStatus
from ..lib.llm_backend import *

from typing import List,Iterable,Dict
from tqdm.auto import tqdm


class LLMBroker(ConcurrentAPICallBroker):
    def __init__(self, 
                    cache_path:str,
                    *,
                    concurrency_limit:int=250,
                    rate_limit:int=50,
                    max_number_per_batch:int=None
    ):
        super().__init__(cache_path=cache_path,
                            request_cls=LLMRequest,
                            response_cls=LLMResponse,
                            concurrency_limit=concurrency_limit,
                            rate_limit=rate_limit,
                            max_number_per_batch=max_number_per_batch
        )
        self.token_counter = LLMTokenCounter()
    async def _call_api_async(self, request: BrokerJobRequest, mock: bool)-> BrokerJobResponse:
        llm_request: LLMRequest = request.request_object
        llm_response = await get_llm_response_async(llm_request, mock=mock)
        return BrokerJobResponse(
            job_idx=request.job_idx,
            status=BrokerJobStatus.DONE if llm_response else BrokerJobStatus.FAILED,
            response_object=llm_response,
        )
    async def _update_statistics(self, pbar:tqdm|None,
                                    request: BrokerJobRequest, response:BrokerJobResponse):
        if response.response_object is not None:
            llm_response: LLMResponse = response.response_object
            self.token_counter.update(
                input_tokens=llm_response.prompt_tokens,
                output_tokens=llm_response.completion_tokens,
                cost=llm_response.cost,
            )
        if pbar:
            pbar.set_postfix_str(self.token_counter.get_summary_str())
            pbar.update(1)
    async def _output_and_reset_statistics(self):
        print(f"Token usage: {self.token_counter.get_summary_str()}")
        self.token_counter.reset()

__all__ = [
    "LLMBroker",
]