from batchfactory.core.ledger import _Ledger
import os

def test_ledger(tmp_path):
    # Clean up any existing cache file
    cache_path = tmp_path / "test_ledger_cache.json"
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
    assert os.path.exists(ledger.path)
    # Resume
    del ledger
    ledger = _Ledger(cache_path)
    ledger.resume()
    # Check if the records are still there after resume
    assert ledger.contains("3")
    assert ledger.get_one("3") == {"idx": "3", "data": "test3"}
