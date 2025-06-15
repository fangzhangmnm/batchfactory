from batchfactory.lib.llm_backend import get_llm_response_async, get_llm_embedding_async
from batchfactory.lib.llm_backend import LLMRequest, LLMResponse, LLMMessage, LLMEmbeddingRequest, LLMEmbeddingResponse

def test_get_llm_response_async():
    import asyncio
    from pprint import pprint

    async def main(dummy=False):
        llm_request = LLMRequest(
            custom_id="test_request",
            model="gpt-4o-mini@openai",
            messages=[
                LLMMessage(role="user", content="Hello, how are you?")
            ],
            max_completion_tokens=50
        )
        response = await get_llm_response_async(llm_request, mock=dummy)
        pprint(response.model_dump())
        assert response.custom_id == llm_request.custom_id
        assert response.model == llm_request.model
        assert response.message.role == "assistant"
        assert response.message.content is not None

    asyncio.run(main(dummy=True))
    # asyncio.run(main(dummy=False))


def test_get_llm_embedding_async():
    import asyncio
    from pprint import pprint

    async def main(dummy=False):
        llm_embedding_request = LLMEmbeddingRequest(
            custom_id="test_embedding",
            model="text-embedding-3-small@openai",
            input_text="This is a test sentence for embedding.",
            dimensions=1536,
            dtype='float32'
        )
        response = await get_llm_embedding_async(llm_embedding_request, mock=dummy)
        pprint(response.model_dump())
        assert response.custom_id == llm_embedding_request.custom_id
        assert response.model == llm_embedding_request.model
        assert response.dimensions == llm_embedding_request.dimensions
        assert response.embedding_base64 is not None
        assert response.dtype == llm_embedding_request.dtype
        
    asyncio.run(main(dummy=True))
    # asyncio.run(main(dummy=False))