#!/usr/bin/env python3
"""
Probe HR tennis odds fetch and match rate vs scheduled matches.
"""
import sys
import os
os.chdir('E:\sports-model-bettor')
sys.path.insert(0, 'E:\sports-model-bettor')

import sqlite3
from engine.tennis_odds import fetch_all, build_lookup, find_match, _normalize_name_key

# Connect to tennis DB
conn = sqlite3.connect('data\tennis.db')
conn.row_factory = sqlite3.Row
cursor = conn.cursor()

# ==================== SECTION 1: Fetch HR odds ====================
print("\n" + "="*70)
print("1. FETCHING HARD ROCK TENNIS ODDS")
print("="*70)

hr_events = fetch_all(force=True)
print(f"\nTotal HR events fetched: {len(hr_events)}")

if hr_events:
    # Show first 5 events
    for i, ev in enumerate(list(hr_events)[:5], 1):
        p1 = ev.get('p1_name', '?')
        p2 = ev.get('p2_name', '?')
        comp = ev.get('comp', '?')
        mid = ev.get('match_id', '?')
        print(f"  HR {i}. {p1} vs {p2} | {comp} (id: {mid})")

# Count "Winner of" events (bracket placeholders)
bracket_count = sum(1 for ev in hr_events if "winner of" in (ev.get('p1_name', '') + ev.get('p2_name', '')).lower())
real_name_count = len(hr_events) - bracket_count
print(f"\nHR event breakdown:")
print(f"  Total events: {len(hr_events)}")
print(f"  Real player names: {real_name_count}")
print(f"  Forward-bracket events (Winner of...): {bracket_count}")

# Build lookup index
hr_lookup = build_lookup(hr_events) if hr_events else {}
print(f"\nHR lookup index size: {len(hr_lookup)} unique player pairs")

# ==================== SECTION 2: Get scheduled matches ====================
print("\n" + "="*70)
print("2. SCHEDULED MATCHES (Today/Tomorrow: 2026-05-05, 2026-05-06)")
print("="*70)

cursor.execute("""
    SELECT date, tournament, p1_name, p2_name, tour, match_id
    FROM tennis_scheduled_matches 
    WHERE date IN ('2026-05-05', '2026-05-06')
    AND p1_id IS NOT NULL AND p2_id IS NOT NULL
    ORDER BY date, tournament, match_id
""")

scheduled = cursor.fetchall()
print(f"\nTotal scheduled matches: {len(scheduled)}")

# Show sample
print(f"\nFirst 10 scheduled matches:")
for i, row in enumerate(list(scheduled)[:10], 1):
    print(f"  {i}. {row['date']} | {row['tour'].upper()} {row['tournament']}: {row['p1_name']} vs {row['p2_name']}")

# ==================== SECTION 3: Check match rate ====================
print("\n" + "="*70)
print("3. MATCH RATE ANALYSIS")
print("="*70)

matched = 0
unmatched_samples = []

for row in scheduled:
    p1 = row['p1_name']
    p2 = row['p2_name']
    hr_match = find_match(hr_lookup, p1, p2)
    if hr_match:
        matched += 1
    else:
        if len(unmatched_samples) < 15:
            unmatched_samples.append({
                'date': row['date'],
                'tournament': row['tournament'],
                'p1': p1,
                'p2': p2,
                'tour': row['tour'],
                'match_id': row['match_id'],
            })

match_rate = (matched / len(scheduled) * 100) if scheduled else 0
print(f"\nMatched: {matched}/{len(scheduled)} ({match_rate:.1f}%)")
print(f"Unmatched: {len(scheduled) - matched}")

if unmatched_samples:
    print(f"\nFirst {len(unmatched_samples)} unmatched scheduled matches:")
    for i, u in enumerate(unmatched_samples, 1):
        print(f"  {i}. {u['date']} | {u['tour'].upper()} {u['tournament']}: {u['p1']} vs {u['p2']}")

# ==================== SECTION 4: Name divergence analysis ====================
print("\n" + "="*70)
print("4. NAME DIVERGENCE PATTERNS")
print("="*70)

print("\nNormalization demo:")
test_names = [
    "Jannik Sinner",
    "Jannick Sinner",
    "Jannic Sinner",
    "Ruud, C.",
    "Casper Ruud",
    "Rüd, C.",
    "Rud, C.",
    "Félix Auger-Aliassime",
    "Felix Auger-Aliassime",
]

for name in test_names:
    norm = _normalize_name_key(name)
    print(f"  '{name}' -> '{norm}'")

# Try to find HR events in all tournament names for pattern matching
print(f"\nUnique HR tournament names (first 15):")
hr_tournaments = set()
for ev in hr_events:
    if ev.get('comp'):
        hr_tournaments.add(ev.get('comp'))
for t in sorted(list(hr_tournaments)[:15]):
    print(f"  - {t}")

conn.close()
print("\n" + "="*70)
