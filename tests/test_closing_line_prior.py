"""Tests for engine.closing_line_prior.

Pure-math module — log-odds linear pool of model_prob and market
implied. Critical because the prior gates EVERY MLB pick today (and
will gate other sports as their weights fit). A regression here
silently shifts every probability the model produces.
"""
from engine.closing_line_prior import (
    posterior_prob, _logit, _sigmoid, _implied_from_american,
)


class TestImplied:
    def test_neg_odds(self):
        # -200 → 200/(200+100) = 0.667
        assert abs(_implied_from_american(-200) - 0.6667) < 0.001

    def test_pos_odds(self):
        # +150 → 100/(150+100) = 0.40
        assert abs(_implied_from_american(150) - 0.40) < 0.001

    def test_even(self):
        assert abs(_implied_from_american(100) - 0.50) < 0.001


class TestSigmoidLogitRoundtrip:
    def test_roundtrip(self):
        for p in (0.1, 0.3, 0.5, 0.7, 0.9):
            assert abs(_sigmoid(_logit(p)) - p) < 1e-9


class TestPosteriorProb:
    def test_w1_returns_model(self):
        # weight=1 → pure model
        assert abs(posterior_prob(0.7, 0.4, 1.0) - 0.7) < 1e-3

    def test_w0_returns_market(self):
        # weight=0 → pure market
        assert abs(posterior_prob(0.7, 0.4, 0.0) - 0.4) < 1e-3

    def test_w_half_blends(self):
        # weight=0.5 → log-odds midpoint
        post = posterior_prob(0.7, 0.4, 0.5)
        # logit(0.7)=0.847, logit(0.4)=-0.405, mean=0.221, sigmoid=0.555
        assert 0.54 < post < 0.57

    def test_market_dominates_at_low_w(self):
        # w=0.1 → posterior should pull strongly toward market
        post = posterior_prob(0.85, 0.50, 0.10)
        # Should be much closer to 0.50 than to 0.85
        assert abs(post - 0.50) < abs(post - 0.85)

    def test_extremes_dont_overflow(self):
        # Numeric stability — logit(0.999...) is huge but should clip
        for p in (0.0, 1.0):
            for m in (0.0, 0.5, 1.0):
                post = posterior_prob(p, m, 0.5)
                assert 0.0 < post < 1.0  # never exactly 0 or 1
