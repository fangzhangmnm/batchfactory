from batchfactory.core.ledger2 import Ledger2
import os
import asyncio

def test_ledger2(tmp_path):
    cache_path = tmp_path / "test_ledger2_cache.sqlite"
    ledger = Ledger2(cache_path)
    # do not need resume
    # Append some records
    ledger.update_many_sync({
        "1": {"idx": "1", "data": "test1"},
        "2": {"idx": "2", "data": "test2"},
    })
    assert ledger.contains("1")
    assert ledger.contains("2")
    assert not ledger.contains("3")
    assert ledger.get_one("1") == {"idx": "1", "data": "test1"}
    assert ledger.get_one("3") is None
    # Update records
    ledger.update_many_sync({
        "1": {"idx": "1", "data": "updated_test1"},
        "3": {"idx": "3", "data": "test3"},
    })
    assert ledger.get_one("1") == {"idx": "1", "data": "updated_test1"}
    assert ledger.get_one("3") == {"idx": "3", "data": "test3"}
    asyncio.run(ledger.update_one_async({"idx": "2", "data": "updated_test2"}))
    # Check if the update was successful
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
    assert os.path.exists(ledger.path)
    # Resume
    del ledger
    ledger = Ledger2(cache_path)
    # do not need resume
    # Check if the records are still there after resume
    assert ledger.contains("3")
    assert ledger.get_one("3") == {"idx": "3", "data": "test3"}
    # Cleanup
    all_records = ledger.get_all()
    ledger.remove_many(set(all_records.keys()))
    ledger.compact()
    del ledger