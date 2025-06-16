_op_list = []

def show_in_op_list(_func_or_class=None,highlight=False,is_functional=False):
    def decorator(func_or_class):
        global _op_list
        _op_list.append({"reference": func_or_class, "highlight": highlight, "is_functional": is_functional})
        return func_or_class
    return decorator if _func_or_class is None else decorator(_func_or_class)

def get_all_desc():
    def key(desc):
        return (not desc["highlight"], desc["reference"].__name__)
    global _op_list
    return list(sorted(_op_list, key=key))

def get_all():
    return [desc["reference"] for desc in get_all_desc()]

def generate_ops_md_str():
    # table_header = [
    #     "| Operation | Description |",
    #     "|-----------|-------------|",
    # ]
    md_lines = []
    all_desc = get_all_desc()
    md_lines.append("| Operation | Description |")
    md_lines.append("|-----------|-------------|")
    for desc in all_desc:
        op = desc["reference"]
        doc_header = op.__doc__.strip().splitlines()[0] if op.__doc__ else "No documentation available"
        doc_header = doc_header[2:].strip() if doc_header.startswith("- ") else doc_header.strip()
        name = op.__name__
        if desc["is_functional"]:
            # name = f"MapField({name},..."
            doc_header += f" Use `MapField({name},...).`"
        name = f"`{name}`"
        if desc["highlight"]:
            name = f"**{name}**"
        md_lines.append(f"| {name} | {doc_header} |")
    return "\n".join(md_lines)
