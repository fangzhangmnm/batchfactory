from .broker_op import *
from .checkpoint_op import *
from .common_op import *
from .control_flow_op import *
from .llm_op import *
from .io_op import *




def get_all():
    from ..core import BaseOp
    import importlib, inspect
    op_module = importlib.import_module(__name__)

    all_ops = []
    for obj in vars(op_module).values():
        if (
            isinstance(obj, type) 
            and issubclass(obj, BaseOp)
            and not inspect.isabstract(obj)
        ):
            all_ops.append(obj)
    return all_ops
    
def print_all():
    print("Available Ops:")
    all_ops = sorted(get_all(), key=lambda x: x.__name__)
    for op in all_ops:
        name = op.__name__
        doc_header = op.__doc__.strip().splitlines()[0] if op.__doc__ else "No documentation available"
        if doc_header.startswith("- "):
            doc_header = doc_header[2:].strip()
        print(f"- {name}: {doc_header}")

def _generate_all_ops_md_str():
    md_lines = [
        "| Operation | Description |",
        "|-----------|-------------|",
    ]
    all_ops = sorted(get_all(), key=lambda x: x.__name__)
    for op in all_ops:
        name = op.__name__
        doc_header = op.__doc__.strip().splitlines()[0] if op.__doc__ else "No documentation available"
        if doc_header.startswith("- "):
            doc_header = doc_header[2:].strip()
        md_lines.append(f"| `{name}` | {doc_header} |")
    return "\n".join(md_lines)

