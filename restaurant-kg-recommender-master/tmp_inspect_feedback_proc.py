import json
from pathlib import Path

nb = json.loads(Path('graph_rag.ipynb').read_text(encoding='utf-8'))
ns = {'__name__': '__main__'}
for i, cell in enumerate(nb['cells'][:18], start=1):
    if cell.get('cell_type') != 'code':
        continue
    src = ''.join(cell.get('source', []))
    if src.lstrip().startswith('%') or src.lstrip().startswith('!'):
        continue
    exec(compile(src, f'cell-{i}', 'exec'), ns)

print('\n=== SUMMARY ===')
print('feedback rows:', len(ns['feedback']))
print('feedback_proc rows:', len(ns['feedback_proc']))
print('feedback_proc columns:', list(ns['feedback_proc'].columns))
print('\n=== HEAD ===')
print(ns['feedback_proc'].head(3).to_string())
