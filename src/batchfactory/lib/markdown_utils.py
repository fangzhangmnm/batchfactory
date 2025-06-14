import re
from .utils import _setdefault_hierarchy
from typing import Iterator, List, Tuple
from pathlib import Path


_MARKDOWN_HEADING_RE_WITH_INDENT = re.compile(r'^([ ]{0,3})(#+)([ \t]+)(.*)$') # indent, hashes, spacing, remaining
_CODE_FENCE_RE = re.compile(r'^([ ]{0,3}`{3,}|[ ]{0,3}~{3,})(.*)$') # fence_style, language

def detect_markdown_heading_line(line:str)-> Tuple[bool,int,str]:
    "returns is_heading, level, heading(stripped)"
    m = _MARKDOWN_HEADING_RE_WITH_INDENT.match(line)
    if not m:
        return False, 0, ''
    indent, hashes, spacing, remaining = m.groups()
    remaining = remaining or ''
    is_heading, level, heading = True, len(hashes), remaining.strip()
    return is_heading, level, heading

def escape_markdown_heading_line(line:str)->Tuple[bool,str]:
    "escape a markdown heading line, returns is_heading, escaped_line"
    m = _MARKDOWN_HEADING_RE_WITH_INDENT.match(line)
    if not m:
        return False, line
    indent, hashes, spacing, remaining = m.groups()
    remaining = remaining or ''
    return True, f"{indent}\\{hashes}{spacing}{remaining}"

def detect_code_fence(line:str)->Tuple[bool,str,str]:
    "detect a code fence line, returns is_fence, fence_style, language(stripped)"
    m = _CODE_FENCE_RE.match(line)
    if not m:
        return False, '', ''
    fence_style, language = m.groups()
    return True, fence_style, language.strip()

def update_markdown_headings(previous_headings:List[str], line:str, default="")-> Tuple[bool,List[str]|None]:
    is_heading, level, heading = detect_markdown_heading_line(line)
    if not is_heading:
        return False, previous_headings.copy()
    base = previous_headings[:level-1]
    pad = [default] * max(0, level - len(base) - 1)
    new_headings = base + pad + [heading]
    return True, new_headings

def escape_markdown_headings(text:str,fix_unclosed_code_fence=True)->str:
    in_fence, fence_style = False, ""
    out_lines = []
    for raw in text.splitlines(keepends=True):
        nl = "\n" if raw.endswith("\n") else ""
        line = raw.rstrip("\r\n")
        # detect fence open/close
        is_fence, new_fence_style, _ = detect_code_fence(line)
        if is_fence:
            if not in_fence:
                in_fence, fence_style = True, new_fence_style
            elif new_fence_style.strip() == fence_style.strip():
                in_fence, fence_style = False, ""
            out_lines.append(f"{line}{nl}")
            continue
        # outside a fence, escape true headings
        if not in_fence:
            is_heading, escaped = escape_markdown_heading_line(line)
            if is_heading:
                out_lines.append(f"{escaped}{nl}")
                continue
        # inside a fence, pass through as is
        out_lines.append(f"{line}{nl}")
    # close any unclosed fence
    if in_fence:
        if fix_unclosed_code_fence:
            out_lines.append(f"{fence_style}\n")
        else:
            raise ValueError("Unclosed code fence detected in text.")
    return ''.join(out_lines)


def generate_markdown_heading_lines(headings:List[str],previous_headings=None)->List[str]:
    """
    generate 0, 1 or more markdown heading lines from a list of headings
    will always create an entry, even it duplicates with a pervious parent heading.
    """
    if previous_headings is None: previous_headings = []
    changed = False
    lines = []
    for level, heading in enumerate(headings, start=1):
        if changed or level > len(previous_headings) or heading != previous_headings[level - 1]:
            lines.append(f"{'#' * level} {heading}\n")
            changed = True
    if not changed: # exception handling
        if headings:
            level, heading = len(headings), headings[-1]
            lines.append(f"{'#' * level} {heading}\n")
        elif previous_headings:
            raise ValueError("Entry with no headings can only be written at the beginning of a file.")
    return lines
        
def iter_markdown_lines(path:str|Path)->Iterator[Tuple[List[str],str]]:
    "yields (headings:list[str],keyword) for all nonempty lines in a markdown file. keyword are stripped"
    current_headings = []
    with Path(path).open('r', encoding='utf-8') as f:
        for raw in f:
            line = raw.rstrip("\r\n")
            is_heading, current_headings = update_markdown_headings(current_headings, line)
            if not is_heading:
                keyword = line.strip()
                if keyword:
                    yield current_headings, keyword

def write_markdown_lines(path:str|Path,entries:List[Tuple[List[str],str]]):
    """
    write a list of (headings:list[str],keyword) tuples to a markdown file.
    keyword are being automatically stripped, and must not be empty
    """
    with Path(path).open('w', encoding='utf-8', newline='\n') as f:
        previous_headings = []
        for headings, keyword in entries:
            headings = [h.strip() for h in headings]
            keyword = keyword.strip()
            if not keyword:
                raise ValueError("Keyword must not be empty.")
            lines = generate_markdown_heading_lines(headings, previous_headings)
            f.writelines(lines)
            f.write(f"{keyword}\n")
            previous_headings = list(headings)

def iter_markdown_entries(path:str|Path)->Iterator[Tuple[List[str],str]]:
    """
    yields (headings:list[str],text) for all markdown entries in a file
    - no stripping. empty entry also included
    """
    previous_headings = []
    previous_text = ""
    in_fence = False
    with Path(path).open('r', encoding='utf-8') as f:
        for raw in f:
            line = raw.rstrip("\r\n")
            if detect_code_fence(line)[0]:
                in_fence = not in_fence
            if in_fence:
                previous_text += raw
                continue
            is_heading, new_headings = update_markdown_headings(previous_headings, line)
            if is_heading:
                if previous_headings or previous_text:
                    yield previous_headings.copy(), previous_text
                previous_headings, previous_text = new_headings, ""
            else:
                previous_text += raw
        yield previous_headings.copy(), previous_text

def write_markdown_entries(path:str|Path,entries:List[Tuple[List[str],str]]):
    """
    write a list of (headings:list[str],text) tuples to a markdown file.
    texts are being kept as it is, and may contain empty lines.
    """
    with Path(path).open('w', encoding='utf-8', newline='\n') as f:
        previous_headings = []
        for headings, text in entries:
            headings = [h.strip() for h in headings]
            text = text.replace('\r\n', '\n').replace('\r', '\n')
            lines = generate_markdown_heading_lines(headings, previous_headings)
            f.writelines(lines)
            escaped_text = escape_markdown_headings(text)
            f.write(escaped_text)
            if not escaped_text.endswith('\n'):
                f.write('\n')
            previous_headings = list(headings)



def build_sort_key_from_text(s: str):
    m = re.search(r'(\d+)', s)
    num = int(m.group(1)) if m else 0
    return (num, s)

def build_sort_key_from_headings(headings: List[str]) -> List[Tuple[int, str]]:
    return [build_sort_key_from_text(h) for h in headings]




# def sort_markdown_entries(entry:Tuple[List[str],str])->Tuple[List[str],str]:
#     "sort markdown entry (headings:list, context)"




# def sort_markdown_entries(entry):
#     directory, keyword, _ = entry
#     directory = [_num_str_key(d) for d in directory]
#     keyword = _num_str_key(keyword)
#     return (directory, keyword)





# def nonempty_lines(text):
#     return [t for t in text.split('\n') if t.strip()]

# def iter_markdown_lines(markdown_path):
#     '''yields (directory,keyword,''), each line is a leaf'''
#     current_path=[]
#     def set_level(title,level):
#         nonlocal current_path
#         current_path=(current_path+['']*100)[:level]
#         current_path[level-1]=title
#     text=open(markdown_path, 'r', encoding='utf-8').read()
#     lines=text.split('\n')
#     for line in lines:
#         line=line.strip()
#         if re.match(r'^#+ ',line):
#             level=len(re.match(r'^(#+) ',line).group(1))
#             title=line[level+1:].strip()
#             set_level(title,level)
#         elif len(line)>0:
#             yield current_path[:],line,""


# def iter_markdown_entries(markdown_path):
#     '''yields (directory,keyword,context) for each subtitle leaf'''
#     current_path=[]
#     def set_level(title,level):
#         nonlocal current_path
#         current_path=(current_path+['']*100)[:level]
#         current_path[level-1]=title
#     text=open(markdown_path, 'r', encoding='utf-8').read()
#     current_context=None
#     def yieldQ():
#         # only yield if there is a context and path is not empty
#         # so the entry at root level is not yielded. e.g. prologue and information in a novel txt
#         return current_context and len(current_context.replace('\n','').strip())>0 and len(current_path)>0
#     for counter,line in enumerate(nonempty_lines(text)):
#         line_stripped=line.strip()
#         if re.match(r'^#+ ',line_stripped):
#             if yieldQ():
#                 yield current_path[:-1],current_path[-1],current_context
#             current_context=None
#             level=len(re.match(r'^(#+) ',line_stripped).group(1))
#             title=line_stripped[level+1:].strip()
#             set_level(title,level)
#         else:
#             if current_context is None:
#                 current_context=line
#             else:
#                 current_context+='\n'+line
#     if yieldQ():
#         yield current_path[:-1],current_path[-1],current_context


# def _num_str_key(s: str):
#     m = re.search(r'(\d+)', s)
#     num = int(m.group(1)) if m else 0
#     return (num, s)
# def markdown_sort_key(entry):
#     directory, keyword, _ = entry
#     directory = [_num_str_key(d) for d in directory]
#     keyword = _num_str_key(keyword)
#     return (directory, keyword)

# def write_markdown(entries:tuple[list,str,str],markdown_path,mode='w',sort=False):
#     '''entries:list of (directory,keyword,content) tuples
#     directory is a list of categories, not including keyword'''
#     old_directory=[]
#     def directory_change_iter(old_directory,new_directory):
#         # yield level,category for each level that changed
#         for i,new_category in enumerate(new_directory):
#             if i>=len(old_directory) or old_directory[i]!=new_category:
#                 yield i,new_category
#     if sort: entries= sorted(entries, key=markdown_sort_key)
#     with open(markdown_path,mode,encoding='utf-8') as f:
#         for directory,keyword,content in entries:
#             for level,category in directory_change_iter(old_directory,directory):
#                 f.write(f'{"#"*(level+1)} {category}\n\n')
#             keyword_level=len(directory)
#             f.write(f'{"#"*(keyword_level+1)} {keyword}\n\n')
#             f.write(content+'\n\n')
#             old_directory=directory

# def markdown_lines_to_dict(markdown_path):
#     '''returns a hierarchical dictionary of lists, where entries are non-empty lines, and keys are markdown headings'''
#     result = {}
#     for directory, keyword, _ in iter_markdown_entries(markdown_path):
#         _setdefault_hierarchy(result, directory, []).append(keyword)
#     return result

# def markdown_entries_to_dict(markdown_path):
#     '''returns a hierarchical dictionary of texts, where keys are markdown headings'''
#     result = {}
#     for directory, keyword, content in iter_markdown_entries(markdown_path):
#         _setdefault_hierarchy(result, directory, {})[keyword] = content
#     return result





__all__ = [
    'iter_markdown_lines',
    'iter_markdown_entries',
    'write_markdown_lines',
    'write_markdown_entries',
    'build_sort_key_from_headings',
    'escape_markdown_headings',
]