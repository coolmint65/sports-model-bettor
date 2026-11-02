/**
 * MarketArmingView — per-market real-money arming for the auto-better, shown as
 * a section on the Bet Queue "Tracker" tab. Pick which (sport, bet_type) cells
 * actually place, from the ones that qualify. Armed = real money fires (still
 * gated by cell qualification + the calibrated-edge check). wnba defaults ON;
 * everything else is opt-in. Backed by /api/bet-queue/market-arming. 2026-09-02.
 */
import { useCallback, useEffect, useState } from 'react'
import { cn } from '../../lib/utils'
import { humanizeBetType } from '../../lib/betType'

const SPORT_LABEL = { wnba: 'WNBA', nba: 'NBA', mlb: 'MLB', nhl: 'NHL' }

export default function MarketArmingView({ api }) {
  const [data, setData] = useState(null)
  const [busy, setBusy] = useState(null)
  const [err, setErr] = useState(null)

  const refresh = useCallback(() => {
    api.get('/bet-queue/market-arming')
      .then((r) => setData(r.data))
      .catch((e) => setErr(e?.message || 'load failed'))
  }, [api])

  useEffect(() => { refresh() }, [refresh])

  const toggle = (m) => {
    const key = `${m.sport}:${m.bet_type}`
    setBusy(key); setErr(null)
    const url = `/bet-queue/market-arming?sport=${encodeURIComponent(m.sport)}`
      + `&bet_type=${encodeURIComponent(m.bet_type)}&on=${!m.armed}`
    api.post(url)
      .then(() => refresh())
      .catch((e) => setErr(e?.message || 'toggle failed'))
      .finally(() => setBusy(null))
  }

  const markets = (data && data.markets) || []
  const bySport = {}
  for (const m of markets) (bySport[m.sport] = bySport[m.sport] || []).push(m)
  const sports = Object.keys(bySport).sort()

  return (
    <div className="rounded-lg border border-border bg-card/40 overflow-hidden">
      <div className="border-b border-border px-4 py-3">
        <div className="text-sm font-semibold text-foreground">Arm markets</div>
        <div className="text-[11px] text-muted-foreground">
          Pick which markets the auto-better places, from the ones that qualify.
          <span className="text-positive"> Armed</span> = real money fires (still gated by
          cell qualification + the calibrated-edge check). WNBA is on by default; everything
          else is opt-in.
        </div>
      </div>
      <div className="p-3 space-y-3">
        {!data && <div className="text-[12px] text-muted-foreground">loading…</div>}
        {err && <div className="text-[11px] text-destructive">{err}</div>}
        {data && sports.length === 0 && (
          <div className="text-[12px] text-muted-foreground">no markets available</div>
        )}
        {sports.map((sp) => (
          <div key={sp}>
            <div className="mb-1.5 text-[10px] font-bold uppercase tracking-wider text-muted-foreground">
              {SPORT_LABEL[sp] || sp}
            </div>
            <div className="space-y-1">
              {bySport[sp].map((m) => {
                const key = `${m.sport}:${m.bet_type}`
                return (
                  <div
                    key={key}
                    className="flex items-center justify-between rounded-md border border-border bg-card px-3 py-1.5"
                  >
                    <span className="text-[13px] text-foreground">{humanizeBetType(m.bet_type)}</span>
                    <button
                      onClick={() => toggle(m)}
                      disabled={busy === key}
                      className={cn(
                        'inline-flex items-center rounded-full border px-2.5 py-0.5',
                        'text-[10px] font-bold uppercase tracking-wider',
                        'transition-colors hover:opacity-80 disabled:opacity-50',
                        m.armed
                          ? 'bg-positive/15 text-positive border-positive/40'
                          : 'bg-muted text-muted-foreground border-border',
                      )}
                      title={m.armed ? 'Armed — click to disarm' : 'Off — click to arm real-money placement'}
                    >
                      {m.armed ? '● Armed' : 'Off'}
                    </button>
                  </div>
                )
              })}
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
