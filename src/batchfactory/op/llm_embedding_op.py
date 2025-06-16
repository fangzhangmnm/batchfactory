from ..core.entry import Entry
from ..core.base_op import ApplyOp, OutputOp
from ..lib.llm_backend import LLMEmbeddingRequest, LLMEmbeddingResponse, llm_client_hub
from ..lib.base64_utils import encode_ndarray, decode_ndarray
from ..lib.utils import get_format_keys, hash_texts, ReprUtil
from ..brokers.llm_embedding_broker import LLMEmbeddingBroker
from ..core.broker import BrokerJobStatus
from .common_op import RemoveField
from .broker_op import BrokerOp, BrokerFailureBehavior
from ..core.project_folder import ProjectFolder
from ._registery import show_in_op_list
from typing import List, Dict, NamedTuple, Set, Tuple, Any, Literal

class GenerateLLMEmbeddingRequest(ApplyOp):
    "Generate LLM embedding requests from input_key."
    def __init__(self,input_key,
                    *,
                    model,
                    output_key="embedding_request",
                    dimensions:int|None=None,
                    dtype:Literal['float32', 'float16'] = 'float32',
    ):
        if not llm_client_hub.is_embedding_model(model): raise ValueError(f"{model} is not an embedding model.")
        super().__init__()
        self.model = model
        self.dimensions = dimensions or llm_client_hub.get_property(model, "dimensions")
        self.dtype = dtype
        self.input_key = input_key
        self.output_key = output_key
    def update(self, entry:Entry) -> None:
        input_text = str(entry.data[self.input_key])
        request_obj = LLMEmbeddingRequest(
            custom_id=self._generate_custom_id(input_text, self.model, self.dimensions),
            model=self.model,
            input_text=input_text,
            dimensions=self.dimensions,
            dtype=self.dtype,
        )
        entry.data[self.output_key] = request_obj.model_dump()
    @staticmethod
    def _generate_custom_id(input_text,model,dimensions):
        texts = [model,str(dimensions),input_text]
        return hash_texts(*texts)
    
class ExtractResponseEmbedding(ApplyOp):
    "Extract the embedding object (base64 encoded numpy array) from the LLM response and store it to entry data."
    def __init__(self,
                    input_key="embedding_response",
                    output_key="embedding"
    ):
        super().__init__()
        self.input_key = input_key
        self.output_key = output_key
    def update(self, entry:Entry) -> None:
        embedding_response = entry.data.get(self.input_key,None)
        embedding_response: LLMEmbeddingResponse = LLMEmbeddingResponse.model_validate(embedding_response)
        entry.data[self.output_key] = embedding_response.embedding_base64

class CallLLMEmbedding(BrokerOp):
    "Dispatch concurrent API calls for embedding models — may induce API billing from external providers."
    def __init__(self,
                *,
                cache_path: str=None,
                broker: LLMEmbeddingBroker=None,
                input_key="embedding_request",
                output_key="embedding_response",
                status_key="status",
                job_idx_key="job_idx",
                keep_all_rev: bool = True,
                failure_behavior: BrokerFailureBehavior = BrokerFailureBehavior.STAY,
                barrier_level: int = 1,
                ):
        if broker is None: broker = ProjectFolder.get_current().get_default_broker(LLMEmbeddingBroker)
        if not isinstance(broker, LLMEmbeddingBroker): raise ValueError(f"Expected broker to be of type LLMEmbeddingBroker, got {type(broker)}")
        super().__init__(
            cache_path=cache_path,
            broker=broker,
            input_key=input_key,
            output_key=output_key,
            status_key=status_key,
            job_idx_key=job_idx_key,
            keep_all_rev=keep_all_rev,
            failure_behavior=failure_behavior,
            barrier_level=barrier_level
        )

    def generate_job_idx(self, entry):
        return entry.data[self.input_key]["custom_id"]

    def get_request_object(self, entry:Entry)->Dict:
        return LLMEmbeddingRequest.model_validate(entry.data[self.input_key]).model_dump()
    
    def dispatch_broker(self, mock:bool=False)->None:
        if self.failure_behavior == BrokerFailureBehavior.RETRY:
            allowed_status = [BrokerJobStatus.FAILED, BrokerJobStatus.QUEUED]
        else:
            allowed_status = [BrokerJobStatus.QUEUED]
        requests = self.broker.get_job_requests(allowed_status)
        if len(requests) == 0:
            return
        self.broker.process_jobs(requests, mock=mock)

class CleanupLLMEmbeddingData(RemoveField):
    "Clean up the internal fields for LLM processing, such as `embedding_request`, `embedding_response`, `status`, `job_idx`."
    def __init__(self,keys=["embedding_request", "embedding_response", "status", "job_idx"]):
        super().__init__(*keys)

class DecodeBase64Embedding(ApplyOp):
    "Decode the base64 encoded embedding into python array."
    def __init__(self, input_key="embedding", output_key="embedding"):
        super().__init__()
        self.input_key = input_key
        self.output_key = output_key
    def update(self, entry:Entry) -> None:
        embedding_base64 = entry.data[self.input_key]
        embedding = decode_ndarray(embedding_base64).tolist()
        entry.data[self.output_key] = embedding

@show_in_op_list(highlight=True)
def EmbedText(
    input_key: str,
    *,
    model: str,
    output_key: str = "embedding",
    cache_path: str = None,
    broker: LLMEmbeddingBroker = None,
    dimensions: int | None = None,
    dtype: Literal['float32', 'float16'] = 'float32',
    output_format: Literal['base64', 'list'] = 'base64',
):
    "Get the embedding vector for the input text."
    g = GenerateLLMEmbeddingRequest(
        input_key=input_key,
        model=model,
        output_key="embedding_request",
        dimensions=dimensions,
        dtype=dtype,
    )
    g |= CallLLMEmbedding(
        cache_path=cache_path,
        broker=broker,
        input_key="embedding_request",
        output_key="embedding_response",
        status_key="status",
        job_idx_key="job_idx",
    )
    g |= ExtractResponseEmbedding(
        input_key="embedding_response",
        output_key=output_key,
    )
    g |= CleanupLLMEmbeddingData()
    if output_format == 'list':
        g |= DecodeBase64Embedding(
            input_key=output_key,
            output_key=output_key,
        )
    return g

__all__ = [
    "GenerateLLMEmbeddingRequest",
    "ExtractResponseEmbedding",
    "CallLLMEmbedding",
    "CleanupLLMEmbeddingData",
    "DecodeBase64Embedding",
    "EmbedText",
]


