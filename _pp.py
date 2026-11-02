import shutil
p='engine/player_props_potd.py'
shutil.copy(p,p+'.bak-20261101-nolosing')
s=open(p,encoding='utf-8').read()
old="""        seasoned = [k for k in keys if (recs.get(k, {}).get("n") or 0) >= FALLBACK_MIN_N
                    and recs[k].get("margin_pp") is not None]
        if seasoned:
            mode = "best_unproven"
            k = max(seasoned, key=lambda k: (recs[k]["margin_pp"], recs[k]["n"]))
            top = _best_in(k)
        else:
            mode = "legacy"
            top = max(pool, key=_selection_score)
            k = _cell_key(top)"""
new="""        # Never pick from a cell that is BELOW its break-even (Austin 2026-11-01 "Yeah I like that
        # change"): a seasoned losing cell is excluded outright; untested cells (< FALLBACK_MIN_N
        # graded) beat it. Order: seasoned cell at/above break-even (best margin) -> best edge x
        # confidence among UNTESTED cells -> no POTD (every cell on the slate is a known loser).
        _n = lambda k: (recs.get(k, {}).get("n") or 0)
        seasoned = [k for k in keys if _n(k) >= FALLBACK_MIN_N
                    and recs[k].get("margin_pp") is not None and recs[k]["margin_pp"] >= 0]
        untested = [k for k in keys if _n(k) < FALLBACK_MIN_N]
        if seasoned:
            mode = "best_unproven"
            k = max(seasoned, key=lambda k: (recs[k]["margin_pp"], recs[k]["n"]))
            top = _best_in(k)
        elif untested:
            mode = "legacy"
            top = max((p for k in untested for p in by_cell[k]), key=_selection_score)
            k = _cell_key(top)
        else:
            logger.info("prop POTD %s: every cell on the slate is below break-even -> no POTD", sport)
            return None"""
assert s.count(old)==1; s=s.replace(old,new)
s=s.replace("""        head = (f"Not proven yet: no cell on the slate has {FALLBACK_MIN_N}+ graded picks"
                f"{since_s} ({core}). Fell back to edge x confidence.")""","""        head = (f"Not proven yet: untested cell ({core}; under {FALLBACK_MIN_N} graded{since_s}). "
                f"Cells below break-even are excluded; best edge x confidence among untested cells.")""")
open(p,'w',encoding='utf-8').write(s); print('ok')
