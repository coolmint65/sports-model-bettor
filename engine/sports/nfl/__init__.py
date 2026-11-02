"""NFL player-props PAPER model.

A NEW, isolated paper model that EXTENDS the shared player-props
framework (``engine.player_props_db`` / ``player_props_tracker`` /
``player_props_potd``) with football/NFL player-prop markets
(passing / rushing / receiving yards + receptions).

It is deliberately isolated from:
  * ``engine.football`` — the team-level NFL GAME model (Elo/GBM/MC).
    We only ever READ ``engine.football._db`` (game status + the
    upcoming slate) and ``engine.football._odds`` (implied team
    totals) — never write to nfl.db or touch the game model's logic.
  * real-money placement (``engine.bet_queue`` / queue_placer). Prop
    picks live in their own ``player_props_picks`` table and are
    never placed.

Storage lives in a DEDICATED sqlite file (``data/football/
nfl_player.db``, see ``_propdb``) so prop tables never co-mingle with
the game model's tables.

Modules:
    _propdb       dedicated sqlite connection for the prop tables
    player_logs   ESPN NFL per-player game-log ingest
    player_props  passing/rushing/receiving yards + receptions model
"""
