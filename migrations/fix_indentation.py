#!/usr/bin/env python3
"""Fix indentation errors left by executescript replacement."""
import re, glob

# Pattern: try:\n  # Schema already...\npass  # executescript
# -> try:\n  pass  # schema exists
pattern = re.compile(
    r'([ \t]*try:)(\n)([ \t]*)# Schema already exists in PostgreSQL[^\n]*\n[ \t]*pass  # executescript removed[^\n]*',
    re.MULTILINE
)

def fix_content(content):
    return pattern.sub(
        lambda m: m.group(1) + m.group(2) + m.group(3) + 'pass  # schema already in PostgreSQL',
        content
    )

files = (
    glob.glob('/opt/projects/helix/services/**/*.py', recursive=True) +
    glob.glob('/opt/projects/helix/routers/**/*.py', recursive=True) +
    glob.glob('/opt/projects/helix/services/*.py')
)

for f in sorted(set(files)):
    if '.bak' in f or '__pycache__' in f:
        continue
    content = open(f).read()
    if 'executescript removed' in content:
        fixed = fix_content(content)
        if fixed != content:
            open(f, 'w').write(fixed)
            print(f'FIXED: {f}')

# Also do a compile check on all py files
print('\nCompile check...')
import py_compile, os
errors = []
for f in sorted(set(files)):
    if '.bak' in f or '__pycache__' in f:
        continue
    try:
        py_compile.compile(f, doraise=True)
    except py_compile.PyCompileError as e:
        errors.append(str(e))
        print(f'  SYNTAX ERROR: {f}\n    {e}')

if not errors:
    print('  All files compile OK')
else:
    print(f'\n{len(errors)} syntax error(s) found')
