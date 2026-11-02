import shutil
p='frontend/src/components/BetsView.jsx'
shutil.copy(p,p+'.bak-20261101-held')
s=open(p,encoding='utf-8').read()
old_nhl="""          const paper_pick = _pp
            ? { ..._pp, graduated: !!(chosen && chosen.type === _pp.type && chosen.pick === _pp.pick) }
            : (view !== 'p1' ? (b.card_lean || null) : null)   // lean fallback (Austin 2026-10-26)"""
new_nhl="""          // 'Best' view: the headline IS the held/recorded authority pick -> show it (graduated), not the
          // max-edge live candidate (Austin 2026-11-01: card pill disagreed with the tracker).
          const paper_pick = (merged && chosen) ? { ...chosen, graduated: true }
            : _pp
            ? { ..._pp, graduated: !!(chosen && chosen.type === _pp.type && chosen.pick === _pp.pick) }
            : (view !== 'p1' ? (b.card_lean || null) : null)   // lean fallback (Austin 2026-10-26)"""
old_nba="""          const paper_pick = _pp
            ? { ..._pp, graduated: !!(chosen && chosen.type === _pp.type && chosen.pick === _pp.pick) }
            : (view !== 'q1' ? (b.card_lean || null) : null)   // lean fallback (Austin 2026-10-26)"""
new_nba="""          // 'Best' view: the headline IS the held/recorded authority pick -> show it (graduated), not the
          // max-edge live candidate (Austin 2026-11-01: card pill disagreed with the tracker).
          const paper_pick = (merged && chosen) ? { ...chosen, graduated: true }
            : _pp
            ? { ..._pp, graduated: !!(chosen && chosen.type === _pp.type && chosen.pick === _pp.pick) }
            : (view !== 'q1' ? (b.card_lean || null) : null)   // lean fallback (Austin 2026-10-26)"""
assert s.count(old_nhl)==1 and s.count(old_nba)==1
s=s.replace(old_nhl,new_nhl).replace(old_nba,new_nba)
open(p,'w',encoding='utf-8').write(s); print('ok')
