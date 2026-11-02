import engine.queue_placer._hr_resolver as R
import engine.queue_placer._relay as rl
from scrapers.hardrock_odds import _fetch_sports_tree, _resolve_comp_ids, _fetch_events_for_comp

tree, _ = _fetch_sports_tree()
evs = []
for cid in _resolve_comp_ids('mlb', tree):
    e, _ = _fetch_events_for_comp(cid); evs.extend(e or [])

found = None
for e in evs:
    if e.get('inplay'):
        continue
    for m in (e.get('markets') or []):
        if m.get('type') == 'BASEBALL:FT:PROPSOMULTILINE':
            for s in (m.get('selection') or []):
                if (s.get('name') or '').lower().startswith('under'):
                    found = (e, m, s); break
        if found:
            break
    if found:
        break

if not found:
    print('no PROPSOMULTILINE Under on a prematch board — cannot test main market now'); raise SystemExit(0)

e, m, s = found
pname = (m.get('name') or '').split(' - ')[0].strip()
print('MAIN market test: %s | %s | %s @ %s | sel=%s (%s) odds=%s period=%s' % (
    pname, m.get('name'), R._participant_by_position(e, 0), R._participant_by_position(e, 1),
    s.get('name'), s.get('id'), s.get('odds'), m.get('period')))

# fake-resolve straight to this PROPSOMULTILINE selection (bypass matchup/abbr mapping)
def _fake(pick):
    return {'selection_id': str(s.get('id')), 'decimal': s.get('odds'),
            'decimal_float': float(s.get('odds')), 'period': str(m.get('period') or 'M'),
            'line': R._line_from_selection_name(s.get('name') or ''), 'market_id': str(m.get('id')),
            'market_type': m.get('type'), 'side': 'under', 'event_id': str(e.get('id')), 'root_idx': s.get('rootIdx')}
R.resolve = _fake
pick = {'sport': 'mlb', 'bet_type': 'Pitcher Ks O/U', 'stake_dollars': 1.0, 'odds': None,
        'matchup': 'X @ Y', 'pick': s.get('name'), 'player_name': pname}
try:
    resp = rl.place_bet(pick, dedup_key='propso-main:' + str(s.get('id')))
    print('RESPONSE: status=%s | ticket=%s | odds=%s | reject=%s' % (
        resp.get('status'), resp.get('hr_ticket_id'), resp.get('placed_odds'), resp.get('reject_reason')))
except Exception as ex:
    print('EXC:', type(ex).__name__, str(ex)[:150])
