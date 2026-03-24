"""
Output formatting for matchup predictions.
Renders a full prediction to a clean terminal display.
"""


def format_prediction(pred: dict) -> str:
    """Format a prediction dict into a readable string."""
    if "error" in pred:
        return f"ERROR: {', '.join(pred['error'])}"

    lines = []
    lines.append("")
    lines.append("=" * 64)
    lines.append(f"  {pred['league_name']} ({pred['league']}) MATCHUP PREDICTION")
    lines.append("=" * 64)

    home = pred["home"]
    away = pred["away"]

    # Teams
    lines.append("")
    record_h = f"  ({home['record']})" if home.get("record") else ""
    record_a = f"  ({away['record']})" if away.get("record") else ""
    lines.append(f"  HOME: {home['name']}{record_h}")
    lines.append(f"  AWAY: {away['name']}{record_a}")

    # Expected score
    lines.append("")
    lines.append("-" * 64)
    es = pred["expected_score"]
    lines.append(f"  PROJECTED SCORE: {home['name']} {es['home']}  -  {es['away']} {away['name']}")
    lines.append(f"  SPREAD: {home['name']} {-pred['spread']:+.1f}   |   TOTAL: {pred['total']}")
    lines.append("-" * 64)

    # Win probabilities
    lines.append("")
    wp = pred["win_prob"]
    lines.append("  WIN PROBABILITY:")
    if "draw" in wp:
        lines.append(f"    {home['name']:30s} {wp['home']:.1%}")
        lines.append(f"    {'Draw':30s} {wp['draw']:.1%}")
        lines.append(f"    {away['name']:30s} {wp['away']:.1%}")
    else:
        lines.append(f"    {home['name']:30s} {wp['home']:.1%}")
        lines.append(f"    {away['name']:30s} {wp['away']:.1%}")

    if pred.get("regulation_draw_prob"):
        lines.append(f"    {'Regulation Draw (→ OT)':30s} {pred['regulation_draw_prob']:.1%}")

    # BTTS (soccer)
    if pred.get("btts") is not None:
        lines.append("")
        lines.append(f"  BTTS: Yes {pred['btts']:.1%}  |  No {1 - pred['btts']:.1%}")

    # Over/Under
    if pred.get("over_under"):
        lines.append("")
        lines.append("  OVER/UNDER:")
        for line, probs in pred["over_under"].items():
            lines.append(f"    {line:>6s}   Over {probs['over']:.1%}  |  Under {probs['under']:.1%}")

    # Half breakdown
    if pred.get("halves"):
        lines.append("")
        lines.append("  HALF BREAKDOWN:")
        lines.append(f"    {'':8s} {'Home':>8s} {'Away':>8s} {'Total':>8s}")
        for h in pred["halves"]:
            lines.append(f"    {h['period']:8s} {h['home']:8} {h['away']:8} {h['total']:8}")

    # Period breakdown
    if pred.get("periods"):
        lines.append("")
        lines.append("  PERIOD BREAKDOWN:")
        lines.append(f"    {'':8s} {'Home':>8s} {'Away':>8s} {'Total':>8s}")
        for p in pred["periods"]:
            lines.append(f"    {p['period']:8s} {p['home']:8} {p['away']:8} {p['total']:8}")

    # Correct scores (soccer/hockey)
    if pred.get("correct_scores"):
        lines.append("")
        lines.append("  MOST LIKELY CORRECT SCORES:")
        for cs in pred["correct_scores"]:
            lines.append(f"    {cs['home']}-{cs['away']}  ({cs['prob']:.1%})")

    # Reasoning
    if pred.get("reasoning"):
        lines.append("")
        lines.append("  ANALYSIS:")
        for r in pred["reasoning"]:
            lines.append(f"    - {r}")

    lines.append("")
    lines.append("=" * 64)
    lines.append("")

    return "\n".join(lines)
