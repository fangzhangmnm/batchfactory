from batchfactory.brokers import ConcurrentLLMCallBroker
from batchfactory import LLMRequest, LLMMessage, LLMResponse, BrokerJobRequest, BrokerJobStatus

def test_concurrent_llm_call_broker(tmp_path):
    cache_path = tmp_path / "concurrent_llm_call_broker.jsonl"
    broker = ConcurrentLLMCallBroker(cache_path=cache_path,
                                     concurrency_limit=10,
                                     rate_limit=5,
                                     max_number_per_batch=100)
    
    broker.verbose = 1
    # Create mock requests
    requests = {
        f"job_{i}": BrokerJobRequest(
            job_idx=f"job_{i}",
            status=BrokerJobStatus.QUEUED,
            request_object=LLMRequest(
                custom_id=f"request_{i}",
                model="gpt-4o-mini@openai",
                messages=[LLMMessage(role="user", content=f"Hello, this is request {i}.")],
                max_completion_tokens=50
            ),
            meta={}
        ) for i in range(20)
    }
    # Enqueue requests
    broker.enqueue(requests)
    # Process requests
    broker.process_jobs(requests, mock=True)
    # Check results
    results = broker.get_job_responses()
    assert len(results) == len(requests), "Not all requests were processed."
    for job_idx, response in results.items():
        assert response.status == BrokerJobStatus.DONE, f"Job {job_idx} did not complete successfully."
        assert response.response_object is not None, f"Response for job {job_idx} is None."
        assert response.response_object.message.content.startswith("Dummy response for"), f"Unexpected response content for {job_idx}."
    # Try Recovering from cache
    del broker
    broker = ConcurrentLLMCallBroker(cache_path=cache_path,
                                     concurrency_limit=10,
                                     rate_limit=5,
                                     max_number_per_batch=100)
    broker.verbose = 1
    broker.resume()
    results = broker.get_job_responses()
    assert len(results) == len(requests), "Not all requests were recovered from cache."