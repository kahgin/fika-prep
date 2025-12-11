import json
import sys

with open(sys.argv[1], 'r') as f:
    nb = json.load(f)

code = []
for cell in nb['cells']:
    if cell['cell_type'] == 'code':
        source = cell['source']
        if isinstance(source, list):
            source = ''.join(source)
        code.append(source)

output_file = sys.argv[1].replace('.ipynb', '.py')
with open(output_file, 'w') as f:
    f.write('\n\n'.join(code))

print(f"Converted to {output_file}")
