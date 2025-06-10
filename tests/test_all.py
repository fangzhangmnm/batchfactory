import os


def test_ledger():
    from batchfactory.core.ledger import _Ledger
    # Clean up any existing cache file
    cache_path = "./data/.tmp/test_cache.jsonl"
    if os.path.exists(cache_path):
        os.remove(cache_path)
    ledger = _Ledger(cache_path)
    ledger.resume()
    # Append some records
    ledger.append_many({
        "1": {"idx": "1", "data": "test1"},
        "2": {"idx": "2", "data": "test2"},
    })
    assert ledger.contains("1")
    assert ledger.contains("2")
    assert not ledger.contains("3")
    assert ledger.get_one("1") == {"idx": "1", "data": "test1"}
    assert ledger.get_one("3") is None
    # Update records
    ledger.update_many({
        "1": {"idx": "1", "data": "updated_test1"},
        "3": {"idx": "3", "data": "test3"},
    })
    assert ledger.get_one("1") == {"idx": "1", "data": "updated_test1"}
    assert ledger.get_one("3") == {"idx": "3", "data": "test3"}
    ledger.update_one({"idx": "2", "data": "updated_test2"})
    assert ledger.get_one("2") == {"idx": "2", "data": "updated_test2"}
    # Filter records
    filtered = ledger.filter_many(lambda x: "3" in x["data"])
    assert filtered == {"3": {"idx": "3", "data": "test3"}}
    # Remove records
    ledger.remove_many({"1", "2"})
    assert not ledger.contains("1")
    assert not ledger.contains("2")
    assert ledger.contains("3")
    assert ledger.get_one("3") == {"idx": "3", "data": "test3"}
    # Compact the cache
    ledger.compact()
    assert os.path.exists(ledger.cache_path)
    # Resume
    del ledger
    ledger = _Ledger(cache_path)
    ledger.resume()
    # Check if the records are still there after resume
    assert ledger.contains("3")
    assert ledger.get_one("3") == {"idx": "3", "data": "test3"}
    # Clean up
    if os.path.exists(cache_path):
        os.remove(cache_path)





def test_concurrent_llm_call_broker():
    from batchfactory.brokers import ConcurrentLLMCallBroker
    from batchfactory import LLMRequest, LLMMessage, LLMResponse, BrokerJobRequest, BrokerJobStatus
    # cleanup cache
    cache_path = "./data/.tmp/concurrent_llm_call_broker.jsonl"
    if os.path.exists(cache_path):
        os.remove(cache_path)
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
    # Cleanup
    if os.path.exists(cache_path):
        os.remove(cache_path)




def test_concurrent_llm_call_op():
    from batchfactory.brokers import ConcurrentLLMCallBroker
    from batchfactory.op import ConcurrentLLMCall
    from batchfactory import Entry, LLMRequest, LLMResponse, LLMMessage, BrokerJobStatus

    op_cache="./data/.tmp/concurrent_llm_call_op.json"
    broker_cache="./data/.tmp/concurrent_llm_call_broker.json"
    if os.path.exists(op_cache):
        os.remove(op_cache)
    if os.path.exists(broker_cache):
        os.remove(broker_cache)
    broker = ConcurrentLLMCallBroker(
        cache_path=broker_cache,
        concurrency_limit=10,
        rate_limit=5,
        max_number_per_batch=100
    )
    broker.verbose = 1
    op = ConcurrentLLMCall(
        cache_path=op_cache,
        broker=broker,
        input_field="llm_request",
        output_field="llm_response",
        status_field="status",
        retry_failed=True,
        drop_failed=True
    )
    # Create mock entries
    entries = {
        f"entry_{i}": Entry(
            idx=f"entry_{i}",
            data={
                "llm_request": {
                    "custom_id": f"request_{i}",
                    "model": "gpt-4o-mini@openai",
                    "messages": [
                        {"role": "user", "content": f"Hello, this is request {i}."}
                    ],
                    "max_completion_tokens": 50
                },
            "status": BrokerJobStatus.QUEUED.value
            }
        ) for i in range(20)
    }
    # Enqueue entries
    op.enqueue(entries)
    # Dispatch the broker to process the jobs
    op.dispatch_broker(mock=True)
    # Check results
    results = op.get_results()
    assert len(results) == len(entries), "Not all entries were processed."
    for entry_idx, entry in results.items():
        assert entry.data["status"] == BrokerJobStatus.DONE.value, f"Entry {entry_idx} did not complete successfully."
        assert "llm_response" in entry.data, f"Response for entry {entry_idx} is missing."
        llm_response = LLMResponse.model_validate(entry.data["llm_response"])
        assert llm_response.message.content.startswith("Dummy response for"), f"Unexpected response content for {entry_idx}."
        assert llm_response.custom_id == entry.data["llm_request"]["custom_id"], f"Custom ID mismatch for {entry_idx}."
    # Try Recovering from cache
    del op
    broker = ConcurrentLLMCallBroker(
        cache_path=broker_cache,
        concurrency_limit=10,
        rate_limit=5,
        max_number_per_batch=100
    )
    broker.verbose = 1
    broker.resume()
    op = ConcurrentLLMCall(
        cache_path=op_cache,
        broker=broker,
        input_field="llm_request",
        output_field="llm_response",
        status_field="status",
        retry_failed=True,
        drop_failed=True
    )
    op.resume()
    results = op.get_results()
    assert len(results) == len(entries), "Not all entries were processed after recovery."
    # Clean up
    del op
    del broker
    if os.path.exists(op_cache):
        os.remove(op_cache)
    if os.path.exists(broker_cache):
        os.remove(broker_cache)