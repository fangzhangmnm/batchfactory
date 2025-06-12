import re
from .utils import _setdefault_hierarchy

def iter_markdown_lines(markdown_path):
    '''yields (directory,keyword,''), each line is a leaf'''
    current_path=[]
    def set_level(title,level):
        nonlocal current_path
        current_path=(current_path+['']*100)[:level]
        current_path[level-1]=title
    text=open(markdown_path, 'r', encoding='utf-8').read()
    lines=text.split('\n')
    for line in lines:
        line=line.strip()
        if re.match(r'^#+ ',line):
            level=len(re.match(r'^(#+) ',line).group(1))
            title=line[level+1:].strip()
            set_level(title,level)
        elif len(line)>0:
            yield current_path[:],line,""

def lines(text):
    return [t for t in text.split('\n') if t.strip()]

def iter_markdown_entries(markdown_path):
    '''yields (directory,keyword,context) for each subtitle leaf'''
    current_path=[]
    def set_level(title,level):
        nonlocal current_path
        current_path=(current_path+['']*100)[:level]
        current_path[level-1]=title
    text=open(markdown_path, 'r', encoding='utf-8').read()
    current_context=None
    def yieldQ():
        # only yield if there is a context and path is not empty
        # so the entry at root level is not yielded. e.g. prologue and information in a novel txt
        return current_context and len(current_context.replace('\n','').strip())>0 and len(current_path)>0
    for counter,line in enumerate(lines(text)):
        line_stripped=line.strip()
        if re.match(r'^#+ ',line_stripped):
            if yieldQ():
                yield current_path[:-1],current_path[-1],current_context
            current_context=None
            level=len(re.match(r'^(#+) ',line_stripped).group(1))
            title=line_stripped[level+1:].strip()
            set_level(title,level)
        else:
            if current_context is None:
                current_context=line
            else:
                current_context+='\n'+line
    if yieldQ():
        yield current_path[:-1],current_path[-1],current_context

def write_markdown(entries:tuple[list,str,str],markdown_path,mode='w',sort=False):
    '''entries:list of (directory,keyword,content) tuples
    directory is a list of categories, not including keyword'''
    old_directory=[]
    def directory_change_iter(old_directory,new_directory):
        # yield level,category for each level that changed
        for i,new_category in enumerate(new_directory):
            if i>=len(old_directory) or old_directory[i]!=new_category:
                yield i,new_category
    if sort: entries=sorted({(*x[0],x[1]):x for x in entries}.values())
    with open(markdown_path,mode,encoding='utf-8') as f:
        for directory,keyword,content in entries:
            for level,category in directory_change_iter(old_directory,directory):
                f.write(f'{"#"*(level+1)} {category}\n\n')
            keyword_level=len(directory)
            f.write(f'{"#"*(keyword_level+1)} {keyword}\n\n')
            f.write(content+'\n\n')
            old_directory=directory

def markdown_lines_to_dict(markdown_path):
    '''returns a hierarchical dictionary of lists, where entries are non-empty lines, and keys are markdown headings'''
    result = {}
    for directory, keyword, _ in iter_markdown_entries(markdown_path):
        _setdefault_hierarchy(result, directory, []).append(keyword)
    return result

def markdown_entries_to_dict(markdown_path):
    '''returns a hierarchical dictionary of texts, where keys are markdown headings'''
    result = {}
    for directory, keyword, content in iter_markdown_entries(markdown_path):
        _setdefault_hierarchy(result, directory, {})[keyword] = content
    return result





__all__ = [
    'iter_markdown_lines',
    'iter_markdown_entries',
    'write_markdown',
    'markdown_lines_to_dict',
    'markdown_entries_to_dict',
]