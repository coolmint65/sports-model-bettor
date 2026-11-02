import sys, shutil, py_compile
sys.stdout.reconfigure(encoding='utf-8', errors='replace')

RM     = r'C:\sports-model-bettor\backend\server\routes_mlb.py'
BAK    = RM + '.bak-authrec-20261023'
HELPER = open(r'C:\sports-model-bettor\_mb_helper.txt', encoding='utf-8').read()
NEWFN  = open(r'C:\sports-model-bettor\_mb_newfunc.txt', encoding='utf-8').read()

START = '@router.get("/api/mlb/tracker/unified")'
END   = '@router.get("/api/tracker/paper")'

# Read raw to preserve the file's existing newline style.
raw = open(RM, 'rb').read()
src = raw.decode('utf-8')
crlf = ('\r\n' in src)
# Work in LF space, convert back at the end.
src_lf = src.replace('\r\n', '\n')

assert src_lf.count(START) == 1, f"START count={src_lf.count(START)}"
assert src_lf.count(END)   == 1, f"END count={src_lf.count(END)}"
assert '_mlb_recorded_authority_pick' not in src_lf, "helper already present — aborting"
i = src_lf.index(START)
j = src_lf.index(END)
assert i < j, "START must precede END"

helper = HELPER.replace('\r\n', '\n').rstrip('\n') + '\n'
newfn  = NEWFN.replace('\r\n', '\n').rstrip('\n') + '\n'
new_span = helper + '\n\n' + newfn + '\n\n'   # 2 blank lines around the new func

patched_lf = src_lf[:i] + new_span + src_lf[j:]

assert 'FROM paper_shadow ' not in new_span, "new span must not read paper_shadow"
assert 'MAX(created_at) c FROM picks' in patched_lf
assert '_mlb_recorded_authority_pick(gid, conn=conn)' in patched_lf
assert patched_lf.count(END) == 1
assert patched_lf.count(START) == 1

out = patched_lf.replace('\n', '\r\n') if crlf else patched_lf

shutil.copyfile(RM, BAK)
open(RM, 'wb').write(out.encode('utf-8'))
print("newline style:", "CRLF" if crlf else "LF")
print("backup  ->", BAK)
print("written ->", RM)
print("delta bytes:", len(out) - len(raw))

py_compile.compile(RM, doraise=True)
print("py_compile: OK")
