"""Tests for engine.pick_provenance.

The decision-trace recorder. Critical because every pick (accepted +
rejected) writes here, and a regression breaks debugging for every
losing-streak investigation downstream.
"""
import json
import gzip
import tempfile
import os
from pathlib import Path
from unittest.mock import patch


def test_record_and_get_roundtrip():
    """Insert + retrieve preserves the trace dict."""
    from engine import pick_provenance
    # Use a temp DB so we don't pollute production.
    with tempfile.TemporaryDirectory() as tmp:
        original_path = pick_provenance._DB_PATH
        try:
            pick_provenance._DB_PATH = Path(tmp) / "prov_test.db"
            trace = {
                "raw_prob": 0.65,
                "odds": -110,
                "implied": 0.524,
                "edge_pct": 6.0,
                "juice_wall_passed": True,
                "edge_floor_passed": True,
            }
            pick_provenance.record(
                sport="mlb",
                bet_type="ML",
                pick_text="NYY",
                trace=trace,
                accepted=True,
                final_prob=0.58,
                final_edge=6.0,
                game_id="test_001",
            )
            # Look up
            from datetime import datetime
            today = datetime.now().strftime("%Y-%m-%d")
            import hashlib
            key = hashlib.md5("test_001|ML|NYY".encode()).hexdigest()[:16]
            row = pick_provenance.get("mlb", today, key)
            assert row is not None
            assert row["bet_type"] == "ML"
            assert row["pick_text"] == "NYY"
            assert row["accepted"] is True
            assert row["final_prob"] == 0.58
            assert row["trace"]["raw_prob"] == 0.65
            assert row["trace"]["edge_floor_passed"] is True
        finally:
            pick_provenance._DB_PATH = original_path


def test_record_does_not_raise_on_bad_trace():
    """Recorder must not raise — pick generation must never fail
    because of a logging hiccup."""
    from engine import pick_provenance
    with tempfile.TemporaryDirectory() as tmp:
        original_path = pick_provenance._DB_PATH
        try:
            pick_provenance._DB_PATH = Path(tmp) / "prov_test.db"
            # Trace contains non-JSON-serializable object
            class Weird:
                pass
            trace = {"weird_obj": Weird()}
            # Should not raise
            pick_provenance.record(
                sport="mlb",
                bet_type="ML",
                pick_text="NYY",
                trace=trace,
                accepted=False,
                game_id="test_002",
            )
        finally:
            pick_provenance._DB_PATH = original_path


def test_pick_key_stable():
    """Same (game_id, bet_type, pick) → same key (idempotent UPSERT)."""
    from engine.pick_provenance import _make_pick_key
    k1 = _make_pick_key("g1", "ML", "NYY")
    k2 = _make_pick_key("g1", "ML", "NYY")
    k3 = _make_pick_key("g1", "ML", "BOS")
    assert k1 == k2
    assert k1 != k3


def test_pick_key_handles_none_game_id():
    from engine.pick_provenance import _make_pick_key
    # Should not raise on None game_id (some pick paths don't carry it)
    k = _make_pick_key(None, "ML", "X")
    assert k and len(k) == 16
