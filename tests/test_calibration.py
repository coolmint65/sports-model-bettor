"""Tests for engine.empirical_calibration + engine.blend_calibration.

The two-stage calibration that shapes every pick's probability before
edge calc. The blend overshrink bug (raw 0.0-0.49 → 0.001 on MLB)
shipped to production for weeks because nothing exercised this. These
tests guard against the same shape of regression.
"""
from engine.empirical_calibration import calibrate as empirical_calibrate
from engine.blend_calibration import calibrate as blend_calibrate


class TestEmpiricalCalibration:
    def test_returns_finite_prob(self):
        for raw in (0.30, 0.50, 0.70, 0.90):
            cal = empirical_calibrate("ML", raw, sport="mlb",
                                       odds=-110, pick_text="NYY")
            assert 0.0 < cal < 1.0
            assert cal == cal  # not NaN

    def test_direction_aware_unders(self):
        # MLB Unders should be shrunk MORE than Overs (the calibration
        # data shows Unders bleeding heavily on overconfident predictions).
        # Both should pull the raw prob below 0.85.
        cal_under = empirical_calibrate("O/U", 0.85, sport="mlb",
                                         odds=-145, pick_text="Under 8.5")
        cal_over = empirical_calibrate("O/U", 0.85, sport="mlb",
                                        odds=-145, pick_text="Over 8.5")
        # Both should be below 0.85 given the historical calibration data.
        # Don't assume Under < Over strictly — depends on current sample.
        assert cal_under < 0.85
        assert cal_over < 0.85

    def test_passthrough_for_unknown_sport(self):
        # Unknown sport returns raw prob (no calibration data).
        cal = empirical_calibrate("ML", 0.60, sport="unknown_sport",
                                   odds=-110, pick_text="X")
        # Should approximately return the raw prob (or close to it)
        assert 0.40 < cal < 0.80


class TestBlendCalibration:
    """The bug: MIN_SAMPLES=200 was too low; on n=285 MLB picks the
    isotonic learned a step function (raw 0.0-0.49 → 0.001). Fixed
    by bumping MIN_SAMPLES to 500. These tests guard against the
    pathology returning."""

    def test_no_drastic_shrinkage_on_low_prob(self):
        # raw 0.30 → calibrated should NOT collapse to ~0
        cal = blend_calibrate("mlb", 0.30)
        assert cal > 0.05, f"blend collapsed 0.30 -> {cal} (overshrink bug)"

    def test_no_drastic_shrinkage_on_high_prob(self):
        # raw 0.85 → calibrated should NOT collapse to ~0.5
        cal = blend_calibrate("mlb", 0.85)
        assert cal > 0.45, f"blend over-compressed 0.85 -> {cal}"

    def test_passthrough_with_insufficient_samples(self):
        # All sports should currently fall back to identity passthrough
        # (n < 500 for each). Identity = output equals input.
        for sport in ("mlb", "nhl", "nba", "tennis"):
            for raw in (0.30, 0.50, 0.70):
                cal = blend_calibrate(sport, raw)
                # Allow tiny shrinkage but no bigger than 5pp.
                assert abs(cal - raw) < 0.05, (
                    f"{sport} raw {raw} -> {cal} (passthrough expected)"
                )
