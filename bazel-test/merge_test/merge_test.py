import glob
import sys
import os

print(os.getcwd(), sys.stderr)

total = 0
for filename in glob.iglob("merge_test/example_files/**/*.txt", recursive=True):
    print(filename)
    with open(filename, 'r') as f:
        total += int(f.read())
print(f"total {total}")

if total == 0:
    sys.exit(1)
sys.exit(0)