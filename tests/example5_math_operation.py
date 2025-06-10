import batchfactory as bf
from batchfactory.op import *
import operator

def test_example_math_operation():
    # Lets calculate 5! = 120 using Repeat

    g = FromList([{"n": 5},{"n": 1}])
    g |= SetField({"prod":1})
    g1 = Apply(operator.mul, ["prod", "rounds"], ["prod"])
    g |= Repeat(g1, max_rounds_field="n")
    g2 = SetField("foo","bar")
    g |= If(lambda data:data['prod'] > 100, g2)
    output = ToList()
    g |= output

    g = g.compile()
    g.execute(dispatch_brokers=False, mock=False)
    entries = output.get_output()

    entry1 = next(filter(lambda e: e.data.get("n") == 1, entries), None)
    entry5 = next(filter(lambda e: e.data.get("n") == 5, entries), None)

    assert len(entries) == 2, f"Expected 2 entries, got {len(entries)}"
    assert entry1.data["prod"] == 1, f"Expected 1, got {entry1.data['prod']}"
    assert entry5.data["prod"] == 120, f"Expected 120, got {entry5.data['prod']}"

    assert "foo" not in entry1.data, "Expected 'foo' not to be in entry1"
    assert entry5.data["foo"] == "bar", f"Expected 'bar', got {entry5.data['foo']}"

    assert entry1.rev == 1, f"Expected revision 1 for entry1, got {entry1.rev}"
    assert entry5.rev == 5, f"Expected revision 5 for entry5, got {entry5.rev}"

    assert entry1.data['rounds'] == 1, f"Expected rounds 1 for entry1, got {entry1.data['rounds']}"
    assert entry5.data['rounds'] == 5, f"Expected rounds 5 for entry5, got {entry5.data['rounds']}"

    # next, lets try calculating 1^2 + 2^2 + 3^2  + 4^2 + 5^2 = 55 using Explode and SpawnOp

    g = FromList([{"n": 5},{"n": 1}])
    g |= Apply(lambda x:list(range(1,1+x)), "n", "list")
    node1 = SpawnFromList("list", "item")
    g |= node1
    g1 = Apply(lambda x: x**2, "item", "item")
    g1 = g1.to_segment()
    g.wire(node1,g1, 1, 0)
    node2 = CollectAllToList("item", "list")
    g |= node2
    g.wire(g1, node2, 0, 1)
    g |= Apply(sum, "list", "sum")
    output = ToList()
    g |= output
    g |= PrintEntry()
    g = g.compile()
    g.execute(dispatch_brokers=False, mock=False)
    entries = output.get_output()

    entry1 = next(filter(lambda e: e.data.get("n") == 1, entries), None)
    entry5 = next(filter(lambda e: e.data.get("n") == 5, entries), None)

    assert len(entries) == 2, f"Expected 2 entries, got {len(entries)}"
    assert entry1.data["sum"] == 1, f"Expected 1, got {entry1.data['sum']}"
    assert entry5.data["sum"] == 55, f"Expected 55, got {entry5.data['sum']}"


if __name__ == "__main__":
    test_example_math_operation()
    print("Example math operation executed successfully.")




