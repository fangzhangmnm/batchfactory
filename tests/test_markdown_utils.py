import pytest
import tempfile
from pathlib import Path

from batchfactory.lib.markdown_utils import (
    detect_markdown_heading_line,
    update_markdown_headings,
    iter_markdown_entries,
    generate_markdown_heading_lines,
    write_markdown_entries,
    iter_markdown_lines,
    write_markdown_lines,
    escape_markdown_heading_line,
    detect_code_fence,
    escape_markdown_headings,
)

def test_detect_markdown_headings_basic():
    ok, level, text = detect_markdown_heading_line('# Hello World')
    assert ok
    assert level == 1
    assert text == 'Hello World'


def test_detect_markdown_headings_leading_spaces():
    ok, level, text = detect_markdown_heading_line('   ## Indented')
    assert ok
    assert level == 2
    assert text == 'Indented'
    ok, level, text = detect_markdown_heading_line('    ## Not a heading')
    assert not ok
    assert level == 0
    assert text == ''

def test_detect_markdown_headings_indent_limit():
    ok, _, _ = detect_markdown_heading_line('    # NotHeading')
    assert not ok

def test_escape_markdown_heading_simple():
    is_h, escaped = escape_markdown_heading_line('# Title')
    assert is_h and escaped == '\\# Title'
    is_h2, esc2 = escape_markdown_heading_line('No heading')
    assert not is_h2 and esc2 == 'No heading'


def test_detect_code_fence_backticks():
    ok, style, lang = detect_code_fence('```')
    assert ok and style == '```' and lang == ''
    ok2, style2, lang2 = detect_code_fence('```python')
    assert ok2 and style2 == '```' and lang2 == 'python'


def test_detect_code_fence_tildes_with_indent():
    ok, style, lang = detect_code_fence('  ~~~ js')
    assert ok and style == '  ~~~' and lang == 'js'
    assert not detect_code_fence('# not a fence')[0]


def test_detect_markdown_headings_unlimited_hashes():
    ok, level, text = detect_markdown_heading_line('######## Title')
    assert ok
    assert level == 8
    assert text == 'Title'
def test_escape_headings_outside_and_inside_fence(tmp_path):
    text = (
        '# foo\n'
        '```python\n'
        '# code comment\n'
        '```\n'
        '# bar\n'
    )
    escaped = escape_markdown_headings(text)
    expected = (
        '\\# foo\n'
        '```python\n'
        '# code comment\n'
        '```\n'
        '\\# bar\n'
    )
    assert escaped == expected


def test_escape_headings_unclosed_fence_error():
    text = '```\n# inside\n'
    with pytest.raises(ValueError):
        escape_markdown_headings(text, fix_unclosed_code_fence=False)

def test_update_markdown_headings():
    ok, new = update_markdown_headings([], '# H1')
    assert ok
    assert new == ['H1']
    ok, new = update_markdown_headings(new, '## H2')
    assert ok
    assert new == ['H1', 'H2']
    ok, new = update_markdown_headings(['H1', 'H2'], '## H2')
    assert ok
    assert new == ['H1', 'H2']
    ok, new = update_markdown_headings(['H1', 'H2'], '# H1b')
    assert ok
    assert new == ['H1b']


def test_iter_markdown_entries_with_intro(tmp_path):
    content = 'Intro line\n# H1\nEntry text\n'
    path = tmp_path / 'test.md'
    path.write_text(content, encoding='utf-8')

    entries = list(iter_markdown_entries(path))
    assert entries == [([], 'Intro line\n'), (['H1'], 'Entry text\n')]


def test_iter_markdown_entries_starting_with_heading(tmp_path):
    content = '# H1\nFirst\n'
    path = tmp_path / 'test2.md'
    path.write_text(content, encoding='utf-8')

    entries = list(iter_markdown_entries(path))
    assert entries == [(['H1'], 'First\n')]


def test_generate_markdown_heading_lines_new_and_shallow():
    lines = generate_markdown_heading_lines(['A', 'B'], [])
    assert lines == ['# A\n', '## B\n']
    lines = generate_markdown_heading_lines(['A'], ['A', 'B'])
    assert lines == ['# A\n']


def test_generate_markdown_heading_lines_no_change():
    lines = generate_markdown_heading_lines(['A', 'B'], ['A', 'B'])
    assert lines == ['## B\n']


def test_write_markdown_entries_basic(tmp_path):
    entries = [(['H1'], 'Line1\n'), (['H1', 'H2'], 'Line2')]
    path = tmp_path / 'out.md'
    write_markdown_entries(path, entries)
    result = path.read_text(encoding='utf-8')
    expected = '# H1\nLine1\n## H2\nLine2\n'
    assert result == expected

def test_write_then_iter_entries_roundtrip(tmp_path):
    entries = [
        ([], 'Intro line\nSecond line\n'),
        (['H1'], 'Section one text\n'),
        (['H1', 'H2'], 'Deep section text\nLine2\n'),
    ]
    path = tmp_path / 'roundtrip_entries.md'
    write_markdown_entries(path, entries)
    result = list(iter_markdown_entries(path))
    assert result == entries


def test_write_then_iter_lines_roundtrip(tmp_path):
    entries = [
        ([], 'L1\nL2\n'),
        (['H1'], 'L3\n'),
        (['H1', 'H2'], 'L4\nL5\n'),
    ]
    path = tmp_path / 'roundtrip_lines.md'
    write_markdown_lines(path, entries)
    lines = list(iter_markdown_lines(path))
    expected = [
        ([], 'L1'),
        ([], 'L2'),
        (['H1'], 'L3'),
        (['H1', 'H2'], 'L4'),
        (['H1', 'H2'], 'L5'),
    ]
    assert lines == expected


def test_iter_markdown_entries_with_code_fence(tmp_path):
    content = (
        'Intro line\n'
        '```txt\n'
        '# comment in code\n'
        '```\n'
        '# Heading\n'
        'Body text\n'
    )
    path = tmp_path / 'test.md'
    path.write_text(content, encoding='utf-8')

    entries = list(iter_markdown_entries(path))
    assert entries == [
        ([], 'Intro line\n```txt\n# comment in code\n```\n'),
        (['Heading'], 'Body text\n')
    ]

def test_write_and_read_markdown_entries_with_fences(tmp_path):
    entries = [
        ([], 'Start\n```txt\n# code\n```\n'),
        (['H'], 'End\n')
    ]
    path = tmp_path / 'rt.md'
    write_markdown_entries(path, entries)
    result = list(iter_markdown_entries(path))
    # Note escape_headings should preserve code and escape no code lines
    assert result == [
        ([], 'Start\n```txt\n# code\n```\n'),
        (['H'], 'End\n')
    ]
def test_write_and_iter_lines_with_fences(tmp_path):
    entries = [
        ([], 'X1\n'),
        (['H1'], 'Y1\n```\n#Z\n```\nY2\n')
    ]
    path = tmp_path / 'rt2.md'
    write_markdown_entries(path, entries)
    lines = list(iter_markdown_lines(path))
    expected = [
        ([], 'X1'),
        (['H1'], 'Y1'),
        (['H1'], '```'),
        (['H1'], '#Z'),
        (['H1'], '```'),
        (['H1'], 'Y2'),
    ]
    assert lines == expected
