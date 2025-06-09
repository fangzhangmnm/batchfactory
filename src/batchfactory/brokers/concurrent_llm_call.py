from ..core import *
from ..lib.utils import format_number, TokenCounter
from ..lib.llm_backend import *
from openai import OpenAI, AsyncOpenAI
from openai.types.chat import ChatCompletion

from typing import List,Iterable,Dict
from dataclasses import dataclass
from pydantic import BaseModel
import asyncio,aiofiles
from aiolimiter import AsyncLimiter
from asyncio import Semaphore, Lock
from tqdm.auto import tqdm


class ConcurrentLLMCallBroker(ImmediateBroker):
    def __init__(self, 
                    cache_path:str,
                    concurrency_limit:int=100,
                    rate_limit:int=100,
                    max_number_per_batch:int=None
    ):
        super().__init__(cache_path=cache_path,
                            request_cls=LLMRequest,
                            response_cls=LLMResponse
        )
        self.concurrency_limit = concurrency_limit
        self.rate_limit = rate_limit
        self.max_number_per_batch = max_number_per_batch
        self.token_counter = TokenCounter()
        self.global_lock = Lock()
        self.concurrency_semaphore = None
        self.pbar = None
        self.rate_limiter = None
        self.dummy_test = False  # for testing purposes, set to True to skip actual LLM calls
        self.verbose = 0

    def process_jobs(self, jobs: List[BrokerJobRequest]):
        if len(jobs)==0:
            return
        print(f"{repr(self)}: processing {len(jobs)} jobs.")
        asyncio.run(self._process_requests_async(jobs))

    def _get_dummy_response(self, request:LLMRequest) -> LLMResponse:
        return LLMResponse(
            custom_id=request.custom_id,
            model=request.model,
            message=LLMMessage(
                role="assistant",
                content=f"Dummy response for {request.custom_id}"
            ),
            prompt_tokens=100,
            completion_tokens=50
        )

    async def _call_llm_async(self, client:AsyncOpenAI, request:LLMRequest):
        async with self.concurrency_semaphore:
            if self.verbose>=1:
                print(f"Processing request {request.custom_id} with model {request.model}")
            if self.dummy_test:
                await asyncio.sleep(10)
                return self._get_dummy_response(request)
            completion:ChatCompletion = await client.chat.completions.create(
                model=get_model_name(request.model),
                messages=request.messages,
                max_completion_tokens=request.max_completion_tokens,
            )
            response = LLMResponse(
                custom_id=request.custom_id,
                model=completion.model,
                message=LLMMessage(
                    role=completion.choices[0].message.role,
                    content=completion.choices[0].message.content
                ),
                prompt_tokens=completion.usage.prompt_tokens,
                completion_tokens=completion.usage.completion_tokens
            )
            return response
        
    async def _task_async(self, job:BrokerJobRequest):
        try:
            request:LLMRequest = job.request_object
            provider = get_provider_name(request.model)
            client:AsyncOpenAI = await llm_client_hub.get_client_async(provider, async_=True)
            response:LLMResponse = await self._call_llm_async(client, request)
            input_price_M, output_price_M = llm_client_hub.get_price_M(request.model)
            async with self.global_lock:
                self.token_counter.update(
                    input_tokens=response.prompt_tokens,
                    output_tokens=response.completion_tokens,
                    input_price_M=input_price_M,
                    output_price_M=output_price_M
                )
            await self._ledger.update_async({
                "idx": job.job_idx,
                "status": BrokerJobStatus.DONE,
                "response": response.model_dump(),
                "meta": {},
            })
        except Exception as e:
            print(f"Error processing {request.custom_id}: {e}")
            # await self.ledger._append_to_response_cache_async(job)
            await( self._ledger.update_async({
                "idx": job.job_idx,
                "status": BrokerJobStatus.FAILED,
                "meta": {"error": str(e)},
            }))
        if self.pbar:
            async with self.global_lock:
                self.pbar.set_postfix_str(self.token_counter.summary())
                self.pbar.update(1)

    async def _process_requests_async(self, jobs:Iterable[BrokerJobRequest]):
        input_jobs = list(jobs)
        if not input_jobs: 
            return
        self.token_counter.reset_counter()
        self.pbar = tqdm(total=len(input_jobs))
        self.concurrency_semaphore = Semaphore(self.concurrency_limit)
        self.rate_limiter = AsyncLimiter(self.rate_limit, 1)  # rate limit of requests per second
        tasks = []
        try:
            for job in input_jobs:
                async with self.rate_limiter:
                    task = asyncio.create_task(self._task_async(job))
                    tasks.append(task)
            await asyncio.gather(*tasks)
        except asyncio.CancelledError:
            print("Processing was cancelled.")
        finally:
            self.pbar.close()
            self.concurrency_semaphore = None
            self.rate_limiter = None
            self.pbar = None
            print(f"Token usage: {self.token_counter.summary()}")
            self.token_counter.reset_counter()



__all__ = [
    "ConcurrentLLMCallBroker",
]