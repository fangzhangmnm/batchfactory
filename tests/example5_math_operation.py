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

if __name__ == "__main__":
    test_example_math_operation()
    print("Example math operation executed successfully.")




