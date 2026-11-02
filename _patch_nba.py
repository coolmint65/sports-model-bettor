import shutil
p='backend/server/routes_nba.py'
shutil.copy(p,p+'.bak-20261101-cardhold')
s=open(p,encoding='utf-8').read()
old="""        best = (max(_mains, key=lambda p: float(p.get("edge") or 0) * float(p.get("prob") or p.get("model_prob") or 0))
                if _mains else None)
"""
new="""        best = (max(_mains, key=lambda p: float(p.get("edge") or 0) * float(p.get("prob") or p.get("model_prob") or 0))
                if _mains else None)
        # Card == tracker (Austin 2026-11-01 "cards have one pick but tracker has another"): the headline
        # HOLDS the game's RECORDED authority pick (first-recorded best_full/best_q1 rows in
        # nba_paper_picks, max by edge*prob -- exactly what /api/nba/tracker/unified grades) at its entry
        # price, instead of re-deriving a live pick every refresh as lines move. Live selection still
        # feeds best_full/best_q1 so the recorder keeps working; it only headlines until a pick is recorded.
        try:
            _rec_best = _nba_recorded_authority_best(game["id"])
        except Exception:
            _rec_best = None
        if _rec_best:
            from engine.config import EDGE_STRONG, EDGE_MODERATE
            _re = float(_rec_best.get("edge") or 0)
            best = {**_rec_best, "model_prob": _rec_best.get("prob"), "held": True,
                    "confidence": ("strong" if _re >= EDGE_STRONG else "moderate" if _re >= EDGE_MODERATE else "lean")}
"""
assert s.count(old)==1
s=s.replace(old,new)
open(p,'w',encoding='utf-8').write(s)
print('patched')
