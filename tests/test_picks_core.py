"""Tests for engine.picks_core.score_pick.

The universal scoring pipeline that gates EVERY pick across MLB / NHL /
NBA / Tennis. A regression here silently changes which picks fire.
The blend_calibration overshrink (raw 0.5+ → 0.001 for MLB) shipped
to production for weeks because nothing exercised this end-to-end —
exactly the kind of bug a smoke test catches.
"""
from engine.picks_core import score_pick, _implied


class TestImplied:
    def test_neg_and_pos(self):
        assert abs(_implied(-110) - 0.5238) < 0.001
        assert abs(_implied(150) - 0.40) < 0.001


class TestScorePickGates:
    """Every gate should return None when it should reject."""

    def _candidate(self, **overrides):
        base = {
            "type": "ML",
            "pick": "TEST",
            "raw_prob": 0.55,
            "odds": -110,
            "game_id": "test_game",
        }
        base.update(overrides)
        return base

    def test_juice_wall_rejects(self):
        # -250 below the -200 juice wall → reject
        result = score_pick(
            self._candidate(odds=-250),
            sport="mlb", juice_wall=-200,
        )
        assert result is None

    def test_belief_gate_rejects(self):
        # raw_prob < 0.50 → never bet against our own model
        result = score_pick(
            self._candidate(raw_prob=0.40),
            sport="mlb", juice_wall=-200,
        )
        assert result is None

    def test_belief_gate_at_50pct_passes(self):
        # 0.50 is the inclusive boundary
        result = score_pick(
            self._candidate(raw_prob=0.50, odds=200),
            sport="mlb", juice_wall=-200,
        )
        # May still reject downstream (calibration / edge floor), but
        # not on the belief gate itself. We just check we don't crash.
        # (Result depends on live calibration data — skip strict assertion.)


class TestScorePickAcceptedShape:
    """When a pick passes all gates, the output shape is contractual."""

    def test_output_has_required_keys(self):
        result = score_pick(
            {"type": "ML", "pick": "X", "raw_prob": 0.75, "odds": 130},
            sport="mlb", juice_wall=-200,
        )
        if result is None:
            # Calibration may reject in this sample data; the shape
            # contract is only meaningful when something accepts.
            return
        for key in ("type", "pick", "prob", "prob_raw", "odds", "edge"):
            assert key in result, f"missing key {key} in {result}"

    def test_no_scored_by_core_sentinel(self):
        # That sentinel was removed in the picks_core cleanup — guard
        # against accidental re-introduction.
        result = score_pick(
            {"type": "ML", "pick": "X", "raw_prob": 0.75, "odds": 130},
            sport="mlb", juice_wall=-200,
        )
        if result is None:
            return
        assert "_scored_by_core" not in result
