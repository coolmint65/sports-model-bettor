"""Tests for engine.tracker_core.

The deduplicated capture_closing_odds helper. The bug class this
replaces (per-sport drift — fix lands in NBA, nobody mirrors to NHL)
needs a guard to prevent regression.
"""
from engine.tracker_core import SportAdapter, core_capture_closing_odds


def test_adapter_dataclass_has_required_fields():
    """SportAdapter is a contract — caller code must pass all fields."""
    adapter = SportAdapter(
        name="test",
        get_conn=lambda: None,
        picks_table="picks",
        hr_fetch=lambda: {},
        extract_closing=lambda *a, **kw: None,
    )
    assert adapter.name == "test"
    assert adapter.picks_table == "picks"
    assert callable(adapter.get_conn)


def test_capture_returns_zero_when_no_pending():
    """No pending picks → zero updates, no exception."""
    import sqlite3
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE picks (
            id INTEGER PRIMARY KEY, matchup TEXT, bet_type TEXT,
            pick TEXT, closing_odds INTEGER, result TEXT
        );
    """)
    adapter = SportAdapter(
        name="test",
        get_conn=lambda: conn,
        picks_table="picks",
        hr_fetch=lambda: {},
        extract_closing=lambda *a, **kw: -110,
    )
    n = core_capture_closing_odds(adapter)
    assert n == 0


def test_capture_returns_zero_when_hr_empty():
    """HR fetch returns empty → no updates, no exception."""
    import sqlite3
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE picks (
            id INTEGER PRIMARY KEY, matchup TEXT, bet_type TEXT,
            pick TEXT, closing_odds INTEGER, result TEXT
        );
        INSERT INTO picks (matchup, bet_type, pick, closing_odds, result)
        VALUES ('NYY @ BOS', 'ML', 'NYY', NULL, NULL);
    """)
    adapter = SportAdapter(
        name="test",
        get_conn=lambda: conn,
        picks_table="picks",
        hr_fetch=lambda: {},
        extract_closing=lambda *a, **kw: -110,
    )
    n = core_capture_closing_odds(adapter)
    assert n == 0


def test_capture_writes_when_match_found():
    """Pending pick + matching HR feed + extractor returns int → row updated."""
    import sqlite3
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.executescript("""
        CREATE TABLE picks (
            id INTEGER PRIMARY KEY, matchup TEXT, bet_type TEXT,
            pick TEXT, closing_odds INTEGER, result TEXT
        );
        INSERT INTO picks (matchup, bet_type, pick, closing_odds, result)
        VALUES ('NYY @ BOS', 'ML', 'BOS', NULL, NULL);
    """)

    def fake_hr():
        return {"NYY@BOS": {"home_ml": -150, "away_ml": 130}}

    def fake_extract(bet_type, pick_text, home, game_odds):
        # return the home ML
        return game_odds.get("home_ml")

    adapter = SportAdapter(
        name="test",
        get_conn=lambda: conn,
        picks_table="picks",
        hr_fetch=fake_hr,
        extract_closing=fake_extract,
    )
    n = core_capture_closing_odds(adapter)
    assert n == 1
    # Verify the row was updated
    row = conn.execute("SELECT closing_odds FROM picks WHERE id = 1").fetchone()
    assert row["closing_odds"] == -150
