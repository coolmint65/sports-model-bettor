"""One-shot sweep: replace datetime.now().strftime today-string with
et_today_str() across engine modules + insert the import. Generated
during the audit cleanup; safe to delete after verification."""
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent

FILES = [
    "engine/basketball/_euroleague_ingest.py",
    "engine/basketball/_predict.py",
    "engine/basketball/_realgm_ingest.py",
    "engine/derivative_tracker.py",
    "engine/gbm/predict.py",
    "engine/hockey/_tracker.py",
    "engine/lineup_refresh.py",
    "engine/live_tracker/_record.py",
    "engine/nba_tracker/_record.py",
    "engine/nhl_tracker/_record.py",
    "engine/pick_events.py",
    "engine/pick_of_day/_read.py",
    "engine/pick_of_day/_select.py",
    "engine/pick_provenance.py",
    "engine/picks.py",
    "engine/picks_cache.py",
    "engine/player_props_calibration.py",
    "engine/player_props_db.py",
    "engine/player_props_potd.py",
    "engine/sports/ahl/predict.py",
    "engine/sports/mlb/factors.py",
    "engine/sports/mlb/first_inning_features.py",
    "engine/sports/mlb/game_diagnose.py",
    "engine/sports/mlb/player_logs.py",
    "engine/sports/mlb/predict.py",
    "engine/sports/mlb/prop_features.py",
    "engine/sports/mlb/prop_features_v2.py",
    "engine/sports/mlb/prop_picks.py",
    "engine/sports/mlb/team_composer.py",
    "engine/sports/nba/diagnose.py",
    "engine/sports/nba/injury_refresh.py",
    "engine/sports/nba/minutes_projector.py",
    "engine/sports/nba/player_logs.py",
    "engine/sports/nba/prop_features.py",
    "engine/sports/nba/prop_picks.py",
    "engine/sports/nba/q1_composer.py",
    "engine/sports/nba/q1_predict.py",
    "engine/sports/nhl/goalie_refresh.py",
    "engine/sports/nhl/player_logs.py",
    "engine/sports/nhl/predict/__init__.py",
    "engine/sports/nhl/prop_features.py",
    "engine/sports/nhl/prop_picks.py",
    "engine/tennis_dist_gbm.py",
    "engine/tennis_line_movement.py",
    "engine/tennis_predict.py",
    "engine/tennis_schedule.py",
    "engine/tennis_tracker.py",
    "engine/tracker/_record.py",
    "engine/tracker/_settle.py",
]


def main():
    n_changed = 0
    for rel in FILES:
        p = ROOT.joinpath(*rel.split("/"))
        if not p.exists():
            print(f"missing: {rel}")
            continue
        src = p.read_text(encoding="utf-8")
        orig = src
        src = src.replace('datetime.now().strftime("%Y-%m-%d")', "et_today_str()")
        src = src.replace("datetime.now().strftime('%Y-%m-%d')", "et_today_str()")
        if src == orig:
            continue
        # Compute relative-import dots — depth from engine/ root.
        parts = rel.split("/")
        depth = len(parts) - 2  # path/within/engine: subdir count
        dots = "." * (depth + 1)
        import_line = f"from {dots}_tz import et_today_str"
        already = (import_line in src
                   or "from engine._tz import et_today_str" in src)
        if not already:
            lines = src.split("\n")
            insert_at = 0
            for i, line in enumerate(lines):
                s = line.lstrip()
                if s.startswith("from ") or s.startswith("import "):
                    if "TYPE_CHECKING" not in line:
                        insert_at = i + 1
                elif (insert_at and s
                       and not s.startswith("#")
                       and not s.startswith('"""')
                       and not s.startswith("'''")):
                    break
            if insert_at:
                lines.insert(insert_at, import_line)
                src = "\n".join(lines)
        p.write_text(src, encoding="utf-8")
        n_changed += 1
        print(f"  changed: {rel}")
    print(f"done: {n_changed} files changed")


if __name__ == "__main__":
    main()
