import batchfactory as bf
from batchfactory.op import *
import operator

import nest_asyncio; nest_asyncio.apply()  # For Jupyter and pytest compatibility

def test_Repeat():
    # Lets calculate 1! = 1  and 5! = 120 using Repeat
    g = bf.Graph()
    g |= FromList([{"n": 1},{"n": 5}])
    g |= SetField({"prod":1})
    g1 = MapField(operator.mul, ["prod", "rounds"], ["prod"])
    g |= Repeat(g1, max_rounds_key="n")
    g |= Sort("n")
    entries = g.execute(dispatch_brokers=False, mock=True)
    print(g)
    
    assert len(entries) == 2, f"Expected 2 entries, got {len(entries)}"
    assert entries[0].data["prod"] == 1, f"Expected 1, got {entries[0].data['sum']}"
    assert entries[1].data["prod"] == 120, f"Expected 120, got {entries[1].data['sum']}"

def test_If():
    # Lets test whether [3,8] < 5
    g = bf.Graph()
    g |= FromList([{"n": 3},{"n": 8}])
    g1 = SetField("result","less than 5")
    g2 = SetField("result","greater than or equal to 5")
    g |= If(lambda data:data['n'] < 5, g1, g2)
    g |= Sort("n")
    entries = g.execute(dispatch_brokers=False, mock=True)
    print(g)

    assert len(entries) == 2, f"Expected 2 entries, got {len(entries)}"
    assert entries[0].data["result"] == "less than 5", f"Expected 'less than 5', got {entries[0].data['result']}"
    assert entries[1].data["result"] == "greater than or equal to 5", f"Expected 'greater than or equal to 5', got {entries[1].data['result']}"

def test_ListParallel():
    # Lets calculate 1^2 + 2^2 + 3^2  + 4^2 + 5^2 = 55 using Explode and SpawnOp
    g = bf.Graph()
    g |= FromList([{"n":1}, {"n": 5}])
    g |= MapField(lambda x:list(range(1,1+x)), "n", "list")
    g1 = MapField(lambda x: x**2, "item")
    g |= ListParallel(g1, "list", "item")
    g |= MapField(sum, "list", "sum")
    g |= Sort("n")
    entries = g.execute(dispatch_brokers=False, mock=True)
    print(g)

    assert len(entries) == 2, f"Expected 2 entries, got {len(entries)}"
    assert entries[0].data["sum"] == 1, f"Expected 1, got {entries[0].data['sum']}"
    assert entries[1].data["sum"] == 55, f"Expected 55, got {entries[1].data['sum']}"

def test_Filter():
    # Lets test whether [3,8] < 5
    g = bf.Graph()
    g |= FromList([{"n": 3},{"n": 8}])
    g |= Filter(lambda data:data['n'] < 5)
    g |= Sort("n")
    entries = g.execute(dispatch_brokers=False, mock=True)
    print(g)

    assert len(entries) == 1, f"Expected 1 entry, got {len(entries)}"
    assert entries[0].data["n"] == 3, f"Expected 3, got {entries[0].data['n']}"
    
def test_Sort():
    # sort [3,5,4,2,1,6]
    g = bf.Graph()
    g |= FromList([3,5,4,2,1,6],output_key="n")
    g |= Sort("n")
    entries = g.execute(dispatch_brokers=False, mock=True)
    print(g)

    assert len(entries) == 6, f"Expected 6 entries, got {len(entries)}"
    new_list = [entry.data["n"] for entry in entries]
    assert new_list == [1, 2, 3, 4, 5, 6], f"Expected sorted list [1, 2, 3, 4, 5, 6], got {new_list}"
    
def test_Barrier(tmp_path):
    project = bf.ProjectFolder("test_barrier", 1, 0, 0, data_dir=tmp_path)
    # sort [3,5,4,2,1,6]
    g = bf.Graph()
    g |= FromList([3,5,4,2,1,6],output_key="n")
    g1 = CheckPoint(project["cache/checkpoint.jsonl"], barrier_level=1)
    g |= If(lambda data: data['n'] < 4, g1)
    g |= Sort("n",barrier_level=2)
    entries = g.execute(dispatch_brokers=False, mock=True)
    print(g)
    
    assert len(entries) == 6, f"Expected 6 entries, got {len(entries)}"
    new_list = [entry.data["n"] for entry in entries]
    assert new_list == [1, 2, 3, 4, 5, 6], f"Expected sorted list [1, 2, 3, 4, 5, 6], got {new_list}"


