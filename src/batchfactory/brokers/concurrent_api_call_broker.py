from ..core.broker import ImmediateBroker, BrokerJobRequest, BrokerJobResponse, BrokerJobStatus
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

from abc import ABC, abstractmethod
import traceback


class ConcurrentAPICallBroker(ImmediateBroker, ABC):
    def __init__(self, 
                cache_path: str,
                request_cls: type,
                response_cls: type = None,
                *,
                concurrency_limit: int,
                rate_limit: int,
                max_number_per_batch: int = None
    ):
        super().__init__(cache_path=cache_path,request_cls=request_cls,response_cls=response_cls)
        self.concurrency_limit = concurrency_limit
        self.rate_limit = rate_limit
        self.max_number_per_batch = max_number_per_batch
        self.global_lock = Lock()
        self.concurrency_semaphore = None
        self.pbar = None
        self.rate_limiter = None

    def process_jobs(self, jobs: Dict[str, BrokerJobRequest], mock: bool = False):
        if len(jobs) == 0: return
        print(f"{repr(self)}: processing {len(jobs)} jobs.")
        asyncio.run(self._process_all_tasks_async(jobs, mock=mock))

    @abstractmethod
    async def _call_api_async(self, request: BrokerJobRequest, mock: bool)-> BrokerJobResponse:
        "global locks are handled by _task_async, statistics is handled by _update_progress_bar, just make the call and assembly the BrokerJobResponse object here."
        pass

    @abstractmethod
    async def _update_statistics(self, pbar:tqdm|None,
                                    request: BrokerJobRequest, response:BrokerJobResponse):
        "global locks are handled by _task_async, just update the statistics here."
        pass
    
    @abstractmethod
    def _output_and_reset_statistics(self):
        pass

    async def _task_async(self, request: BrokerJobRequest, mock: bool):
        try:
            response = await self._call_api_async(request, mock=mock)
        except Exception as e:
            print(f"Error processing request {request.job_idx}: {e}")
            response = BrokerJobResponse(
                job_idx=request.job_idx,
                status=BrokerJobStatus.FAILED,
                response_object=None,
                meta={**(request.meta or {}), "error": str(e)}
            )
        await self._ledger2.update_one_async({
            "idx": request.job_idx,
            "status": response.status.value,
            "response": response.response_object.model_dump() if response.response_object else None,
            "meta": {**(request.meta or {}), **(response.meta or {})},
        })
        async with self.global_lock:
            await self._update_statistics(self.pbar, request, response)

    async def _worker(self, queue: asyncio.Queue, mock: bool):
        while True:
            request = await queue.get()
            try:
                async with self.rate_limiter:
                    async with self.concurrency_semaphore:
                        await self._task_async(request, mock=mock)
            finally:
                queue.task_done()

    async def _process_all_tasks_async(self, requests: Dict[str, BrokerJobRequest], mock: bool):
        requests = list(requests.values())
        if not requests: return
        self.pbar = tqdm(total=len(requests))
        self.concurrency_semaphore = Semaphore(self.concurrency_limit)
        self.rate_limiter = AsyncLimiter(self.rate_limit, 1)
        queue = asyncio.Queue()
        for request in requests[::self.max_number_per_batch]:
            queue.put_nowait(request)
        try:
            workers = [
                asyncio.create_task(self._worker(queue, mock)) for _ in range(self.concurrency_limit)
            ]
            await queue.join()
            for w in workers:
                w.cancel()
        except asyncio.CancelledError:
            print("Processing was cancelled.")
        finally:
            self.pbar.close()
            self.concurrency_semaphore = None
            self.rate_limiter = None
            self.pbar = None
            await self._output_and_reset_statistics()

        



        # tasks = []
        # try:
        #     for request in requests:
        #         async with self.rate_limiter:
        #             task = asyncio.create_task(self._task_async(request, mock))
        #             tasks.append(task)
        #     await asyncio.gather(*tasks)
        # except asyncio.CancelledError:
        #     print("Processing was cancelled.")
        # finally:
        #     self.pbar.close()
        #     self.concurrency_semaphore = None
        #     self.rate_limiter = None
        #     self.pbar = None
        #     await self._output_and_reset_statistics()

__all__ = [
    "ConcurrentAPICallBroker",
]
    


