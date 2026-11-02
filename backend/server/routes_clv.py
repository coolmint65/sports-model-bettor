"""CLV dashboard route (audit T2 -- the edge instrument).

Serves the Closing Line Value report as JSON (/api/clv/report) and a
self-contained HTML scoreboard (/clv). CLV = the one reliable edge signal:
do our picks get a better price than where the line closes? The `clean`
columns count only picks with a verified PRE-GAME close (the honest number);
they start empty and accumulate as the fixed capture runs forward.

Broadened 2026-09-28: the report now covers every sport x pick-type
(main / derivative / props / pick-of-day) — the HTML loop renders the
sport -> pick-type -> market nesting.
"""
from __future__ import annotations

import logging

from fastapi import APIRouter
from fastapi.responses import HTMLResponse

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/api/clv/report")
def api_clv_report() -> dict:
    """Segmented Closing Line Value across all sports x pick-types."""
    from engine.clv_report import clv_report
    return clv_report()


_PAGE = """<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>CLV Scoreboard</title>
<style>
:root{--bg:#0b0f15;--panel:#131a24;--bd:#233041;--tx:#e6edf3;--mut:#8ea0b6;
--good:#2ee6a6;--bad:#ff5b5b;--warn:#e6c34a}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--tx);
font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;line-height:1.5}
.wrap{max-width:1040px;margin:0 auto;padding:24px 16px 80px}
h1{font-size:26px;margin:0 0 4px}.sub{color:var(--mut);font-size:14px;margin-bottom:20px}
.hero{background:var(--panel);border:1px solid var(--bd);border-radius:14px;padding:22px;margin-bottom:22px}
.hero .big{font-size:40px;font-weight:750;line-height:1}
.hero .lbl{font-family:ui-monospace,monospace;font-size:11px;letter-spacing:.12em;
text-transform:uppercase;color:var(--mut);margin-bottom:6px}
.grid{display:flex;gap:14px;flex-wrap:wrap}.grid>div{flex:1;min-width:150px}
.verdict{margin-top:14px;font-size:15px;padding:12px 14px;border-radius:10px;border:1px solid}
h2{font-size:13px;font-family:ui-monospace,monospace;letter-spacing:.12em;text-transform:uppercase;
color:var(--mut);margin:26px 0 10px}
table{width:100%;border-collapse:collapse;font-size:13.5px}
th,td{text-align:right;padding:7px 10px;border-bottom:1px solid var(--bd)}
th:first-child,td:first-child{text-align:left}
th{color:var(--mut);font-weight:600;font-size:11px;letter-spacing:.06em;text-transform:uppercase}
tr.sport td{background:#0f161f;font-weight:700}
tr.ptype td{color:var(--mut);font-size:11px;letter-spacing:.05em;text-transform:uppercase}
tr.lown td{opacity:.5}
.good{color:var(--good)}.bad{color:var(--bad)}.mut{color:var(--mut)}
.clean{border-left:2px solid var(--warn)}
.foot{color:var(--mut);font-size:12px;margin-top:24px}
.tag{display:inline-block;font-family:ui-monospace,monospace;font-size:10px;padding:2px 6px;
border-radius:5px;border:1px solid var(--warn);color:var(--warn);margin-left:6px}
</style></head><body><div class="wrap">
<h1>CLV Scoreboard</h1>
<div class="sub">Do our picks beat the closing line? Positive CLV = we got a better price than the market's close = the one reliable edge signal. <b>Clean</b> = verified pre-game close only (the honest number).</div>
<div id="app">loading...</div>
<div class="foot" id="foot"></div></div>
<script>
const pc=(v,unit)=>v==null?'<span class="mut">--</span>':`<span class="${v>0?'good':(v<0?'bad':'mut')}">${v>0?'+':''}${v}${unit||''}</span>`;
const beat=(v)=>v==null?'<span class="mut">--</span>':`<span class="${v>=53?'good':(v<47?'bad':'mut')}">${v}%</span>`;
const cells=(c)=>`<td>${c.n}</td><td>${pc(c.avg_clv_pct,'%')}</td><td>${beat(c.beat_pct)}</td><td>${pc(c.roi_pct,'%')}</td><td class="clean">${c.clean_n}</td><td class="clean">${pc(c.clean_avg_clv_pct,'%')}</td><td class="clean">${c.clean_beat_pct==null?'<span class=mut>--</span>':beat(c.clean_beat_pct)}</td>`;
async function load(){
  const r=await fetch('/api/clv/report').then(x=>x.json());
  const o=r.overall;
  let cleanBadge = o.clean_n>0 ? '' : '<span class="tag">accumulating</span>';
  let heroClv = o.clean_n>0 ? o.clean_avg_clv_pct : o.avg_clv_pct;
  let heroBeat = o.clean_n>0 ? o.clean_beat_pct : o.beat_pct;
  let verdict, vcolor;
  if(o.clean_n<30){verdict=`Not enough clean pre-game data yet (${o.clean_n} verified). Numbers below lean on historical closes, which for near-start markets can be contaminated -- treat as directional until clean data builds.`;vcolor='var(--warn)';}
  else if(heroBeat>=53){verdict='Beating the close on verified data -- this is a real edge signal. Concentrate here.';vcolor='var(--good)';}
  else {verdict='Not beating the close overall. The market is sharper than the model on average -- hunt the specific rows below that are green on clean data.';vcolor='var(--bad)';}
  let h=`<div class="hero"><div class="grid">
    <div><div class="lbl">CLV ${o.clean_n>0?'(clean)':'(historical)'}</div><div class="big">${pc(heroClv,'%')}</div></div>
    <div><div class="lbl">Beat the close</div><div class="big">${beat(heroBeat)}</div></div>
    <div><div class="lbl">Clean sample</div><div class="big">${o.clean_n}${cleanBadge}</div></div>
    <div><div class="lbl">Total settled</div><div class="big">${o.n}</div></div></div>
    <div class="verdict" style="border-color:${vcolor};color:${vcolor}">${verdict}</div></div>`;
  h+='<h2>By sport, pick type &amp; market</h2><table><thead><tr><th>Market</th><th>n</th><th>CLV</th><th>Beat%</th><th>ROI</th><th class="clean">clean n</th><th class="clean">clean CLV</th><th class="clean">clean beat%</th></tr></thead><tbody>';
  for(const [sport,v] of Object.entries(r.by_sport)){
    h+=`<tr class="sport"><td>${sport.toUpperCase().replace('_',' ')}</td>${cells(v.overall)}</tr>`;
    const types=Object.entries(v.by_type);
    for(const [pt,tv] of types){
      if(types.length>1) h+=`<tr class="ptype"><td>&nbsp;&nbsp;${pt}</td>${cells(tv.overall)}</tr>`;
      for(const m of tv.by_market){
        h+=`<tr class="${m.n<20?'lown':''}"><td>&nbsp;&nbsp;&nbsp;&nbsp;${m.market}</td>${cells(m)}</tr>`;
      }
    }
  }
  h+='</tbody></table>';
  document.getElementById('app').innerHTML=h;
  document.getElementById('foot').textContent='Generated '+r.generated_at+' -- '+r.meta.sources_loaded+' tables loaded. A real edge needs beat% north of ~53% (to clear the vig) on clean data.';
}
load();
</script></body></html>"""


@router.get("/clv", response_class=HTMLResponse)
def clv_dashboard() -> str:
    return _PAGE
