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
    g |= ToList("prod")
    results = g.execute(dispatch_brokers=False, mock=True)
    assert len(results) == 2, f"Expected 2 products, got {len(results)}"
    assert results[0] == 1, f"Expected 1, got {results[0]}"
    assert results[1] == 120, f"Expected 120, got {results[1]}"

def test_If():
    # Lets test whether [3,8] < 5
    g = bf.Graph()
    g |= FromList([{"n": 3},{"n": 8}])
    g1 = SetField("result","less than 5")
    g2 = SetField("result","greater than or equal to 5")
    g |= If(lambda data:data['n'] < 5, g1, g2)
    g |= Sort("n")
    g |= ToList("result")
    results = g.execute(dispatch_brokers=False, mock=True)
    assert len(results) == 2, f"Expected 2 results, got {len(results)}"
    assert results[0] == "less than 5", f"Expected 'less than 5', got {results[0]}"
    assert results[1] == "greater than or equal to 5", f"Expected 'greater than or equal to 5', got {results[1]}"

def test_ListParallel():
    # Lets calculate 1^2 + 2^2 + 3^2  + 4^2 + 5^2 = 55 using Explode and SpawnOp
    g = bf.Graph()
    g |= FromList([{"n":1}, {"n": 5}])
    g |= MapField(lambda x:list(range(1,1+x)), "n", "list")
    g1 = MapField(lambda x: x**2, "item")
    g |= ListParallel(g1, "list", "item")
    g |= MapField(sum, "list", "sum")
    g |= Sort("n")
    g |= ToList("sum")
    results = g.execute(dispatch_brokers=False, mock=True)
    assert len(results) == 2, f"Expected 2 sums, got {len(results)}"
    assert results[0] == 1, f"Expected 1, got {results[0]}"
    assert results[1] == 55, f"Expected 55, got {results[1]}"

def test_Filter():
    # Lets test whether [3,8] < 5
    g = bf.Graph()
    g |= FromList([{"n": 3},{"n": 8}])
    g |= Filter(lambda data:data['n'] < 5)
    g |= Sort("n")
    g |= ToList("n")
    results = g.execute(dispatch_brokers=False, mock=True)
    assert len(results) == 1, f"Expected 1 result, got {len(results)}"
    assert results[0] == 3, f"Expected 3, got {results[0]}"
    
def test_Sort():
    # sort [3,5,4,2,1,6]
    g = bf.Graph()
    g |= FromList([3,5,4,2,1,6],output_key="n")
    g |= Sort("n")
    g |= ToList("n")
    results = g.execute(dispatch_brokers=False, mock=True)
    assert len(results) == 6, f"Expected 6 entries, got {len(results)}"
    assert results == [1, 2, 3, 4, 5, 6], f"Expected sorted list [1, 2, 3, 4, 5, 6], got {results}"
    
def test_Barrier(tmp_path):
    project = bf.ProjectFolder("test_barrier", 1, 0, 0, data_dir=tmp_path)
    # sort [3,5,4,2,1,6]
    g = bf.Graph()
    g |= FromList([3,5,4,2,1,6],output_key="n")
    g1 = CheckPoint(project["cache/checkpoint.jsonl"], barrier_level=1)
    g |= If(lambda data: data['n'] < 4, g1)
    g |= Sort("n",barrier_level=2)
    g |= ToList("n")
    results = g.execute(dispatch_brokers=False, mock=True)
    assert len(results) == 6, f"Expected 6 entries, got {len(results)}"
    assert results == [1, 2, 3, 4, 5, 6], f"Expected sorted list [1, 2, 3, 4, 5, 6], got {results}"
