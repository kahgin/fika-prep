import os
from pathlib import Path

os.chdir('/home/kahgin/fika/fika-prep')

INPUT_DIR = Path("data/query")
OUTPUT_DIR = Path("data/query/batched")
NAME_MATCH = "*.txt"

def batch_files():
    """Read all NAME_MATCH pattern files from query/, deduplicate, and batch save."""
    
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    unique_lines = set()
    for file_path in INPUT_DIR.glob(NAME_MATCH):
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                unique_lines.update(line.strip() for line in f if line.strip())
        except FileNotFoundError:
            continue
    
    # Convert to list for batching
    lines = list(unique_lines)
    
    # Save in batches of 10 lines
    batch_size = 10
    for i in range(0, len(lines), batch_size):
        batch = lines[i:i + batch_size]
        output_file = OUTPUT_DIR / f"{NAME_MATCH.strip('*')}{i//batch_size}.txt"
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(batch))
    
    return len(lines), len(unique_lines)

if __name__ == "__main__":
    total_lines, unique_lines = batch_files()