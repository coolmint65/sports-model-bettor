#!/usr/bin/env python3
"""
Delete the orphaned game-detail-view files left behind after
patch_rmdetail_edits.py removed the click-through wiring. Before
deleting ANY file, re-verifies (fresh grep over the live tree, run
from THIS directory) that no surviving file still imports it. Aborts
the whole run (deletes nothing) if any candidate is still referenced
from outside the candidate set itself.

Usage: python delete_orphans.py [--check] [--root PATH]
"""
import os
import re
import sys

CHECK_ONLY = "--check" in sys.argv
ROOT = "."
for i, a in enumerate(sys.argv):
    if a == "--root" and i + 1 < len(sys.argv):
        ROOT = sys.argv[i + 1]

CANDIDATES = [
    "components/GameDetail.jsx",
    "components/NHLGameDetail.jsx",
    "components/NBAGameDetail.jsx",
    "components/BasketballGameDetail.jsx",
    "components/tennis/MatchDetail.jsx",
    "components/PredictionResults.jsx",
    "components/primitives/GameDetailShell.jsx",
    "components/gameDetail/SharedGameHeader.jsx",
    "components/gameDetail/ProbHistogram.jsx",
    "components/gameDetail/ModelSignals.jsx",
    "components/gameDetail/EdgeCallout.jsx",
    "components/gameDetail/WhyThisPick.jsx",
    "components/gameDetail/RestBadges.jsx",
    "components/gameDetail/LineMovementBadge.jsx",
    "components/gameDetail/UnderdogNote.jsx",
    "components/gameDetail/curateReasoning.js",
    "components/gameDetail/kelly.js",
    "components/nba/detailHelpers.js",
    "components/nhl/detailHelpers.js",
]
candidate_set = set(os.path.normpath(c) for c in CANDIDATES)


def all_source_files():
    for dirpath, dirnames, filenames in os.walk(ROOT):
        # skip node_modules / dist just in case this is ever pointed too high
        dirnames[:] = [d for d in dirnames if d not in ("node_modules", "dist", ".git")]
        for fn in filenames:
            if fn.endswith(".jsx") or fn.endswith(".js"):
                if ".bak" in fn:
                    continue
                yield os.path.normpath(os.path.join(dirpath, fn))


# import-statement shaped reference: from '...NAME' / from "...NAME"
IMPORT_RE_TEMPLATE = r"""['"][^'"]*/{name}['"]"""

files = list(all_source_files())
print(f"Scanning {len(files)} source files under {os.path.abspath(ROOT)} ...")

blockers = {}
for rel in CANDIDATES:
    norm = os.path.normpath(rel)
    base = os.path.basename(norm)
    name = re.sub(r"\.(jsx|js)$", "", base)
    pattern = re.compile(IMPORT_RE_TEMPLATE.format(name=re.escape(name)))
    hits = []
    for f in files:
        if os.path.normpath(f) == norm:
            continue  # the file itself (self-reference inside comments etc. is fine)
        try:
            with open(f, "r", encoding="utf-8") as fh:
                text = fh.read()
        except UnicodeDecodeError:
            with open(f, "rb") as fh:
                text = fh.read().decode("utf-8", errors="replace")
        if pattern.search(text):
            # only a real blocker if the referencing file is NOT itself
            # one of the files we're about to delete
            if os.path.normpath(f) not in candidate_set:
                hits.append(f)
    if hits:
        blockers[rel] = hits

if blockers:
    print("\nABORT - the following candidates are still imported from OUTSIDE the orphan set:")
    for rel, hits in blockers.items():
        print(f"  {rel}  <-  {hits}")
    sys.exit(1)

print("Final check passed: none of the candidates are referenced from any surviving file.\n")

if CHECK_ONLY:
    print("CHECK ONLY — nothing deleted.")
    sys.exit(0)

for rel in CANDIDATES:
    path = os.path.join(ROOT, rel)
    if not os.path.exists(path):
        print(f"SKIP (already gone): {rel}")
        continue
    os.remove(path)
    print(f"DELETED: {rel}")

print(f"\n{len(CANDIDATES)} orphaned file(s) removed.")
