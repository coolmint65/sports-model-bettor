"""The pick-selection authority (Austin 2026-10-14, Workstream #1).

`apply_selected` takes a sport's already-chosen held lean (a card-chip dict)
and returns THE selected pick for that game: the same object every surface
should show -- edge calibrated through `calibration_gate.cal_edge` (the
paper-POTD method: edge, calibrated) + a confidence label. Sport-agnostic:
accepts either field convention (type/bet_type, prob/model_prob) so each
sport's recorded chip works unchanged; the ranking + calibration live here.
"""
from __future__ import annotations


def apply_selected(sport: str, chip: dict | None) -> dict | None:
    """Return ``chip`` as the single selected pick with a CALIBRATED edge and a
    confidence label, or None if there is no chip. Behaviour-preserving vs. the
    inline per-sport paths it replaces."""
    if not chip:
        return None
    out = dict(chip)
    _type = out.get("type") or out.get("bet_type")
    _prob = out.get("prob") if out.get("prob") is not None else out.get("model_prob")
    _pick = out.get("pick") or out.get("display")   # NBA recorded chips use "display"
    if _type is not None:
        out["type"] = _type                     # normalize so consumers find "type"
    try:
        from ..calibration_gate import cal_edge as _cal_edge
        _ce = _cal_edge(sport, _type, _prob, out.get("odds"),
                        pick_text=_pick, raw_edge=out.get("edge"))
        if _ce is not None:
            out["edge"] = round(float(_ce), 2)
    except Exception:
        pass
    try:
        from ..config import EDGE_STRONG, EDGE_MODERATE
        _e = float(out.get("edge") or 0)
        out["confidence"] = ("strong" if _e >= EDGE_STRONG
                             else "moderate" if _e >= EDGE_MODERATE else "lean")
    except Exception:
        pass
    return out
