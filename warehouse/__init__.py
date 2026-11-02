"""PickMint odds warehouse + replay lab (MintyBot 2026-11-01).

Kept import-light on purpose: ``scrapers.hardrock_odds`` imports ``warehouse.hr_tee`` from inside
the worker/backend, so this package __init__ must not open databases or import heavy modules.
The public API is loaded lazily:

    from warehouse import wh            # accessors: wh.events / wh.price_at / wh.open / wh.close ...
    from warehouse.replay import replay # walk-forward replay with enforced no-peeking

See ``warehouse/api.py`` (accessors) and ``warehouse/replay.py`` (replay engine) docstrings.
"""


def __getattr__(name):  # PEP 562 lazy attributes
    if name == "wh":
        from . import api as _api
        return _api
    raise AttributeError(name)
