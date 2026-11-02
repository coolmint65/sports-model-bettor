import sys
path = sys.argv[1]
start = int(sys.argv[2]) if len(sys.argv) > 2 else 1
end = int(sys.argv[3]) if len(sys.argv) > 3 else 10**9
with open(path, encoding="utf-8") as f:
    lines = f.read().splitlines()
for i in range(start - 1, min(end, len(lines))):
    print(f"{i+1}\t{lines[i]}")
