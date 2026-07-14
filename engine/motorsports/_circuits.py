"""Curated F1 circuit info — track length, race laps, lap record.

Ergast doesn't expose these structured fields, and the Wikipedia
infobox is unreliable to parse (changes formatting per circuit).
Hand-curated table is the right tradeoff: 22 circuits on the 2026
calendar, stable data, single source of truth.

Update procedure when the calendar changes:
  - Add the new circuit's ergast_id key with the official length / laps
  - Lap-record updates land here too, sourced from formula1.com or
    each race's Wikipedia race-report page

Race distance (km) = length_km * laps. The frontend computes this on
the fly so we only persist length + laps.
"""
from __future__ import annotations

# circuit_id (Ergast) → {length_km, laps, lap_record_time, lap_record_holder, lap_record_year, type}
CIRCUIT_INFO: dict[str, dict] = {
    "albert_park": {
        "length_km": 5.278, "laps": 58,
        "lap_record_time": "1:19.813",
        "lap_record_holder": "Charles Leclerc",
        "lap_record_year": 2024,
        "type": "Street",
    },
    "shanghai": {
        "length_km": 5.451, "laps": 56,
        "lap_record_time": "1:32.238",
        "lap_record_holder": "Michael Schumacher",
        "lap_record_year": 2004,
        "type": "Permanent",
    },
    "suzuka": {
        "length_km": 5.807, "laps": 53,
        "lap_record_time": "1:30.983",
        "lap_record_holder": "Lewis Hamilton",
        "lap_record_year": 2019,
        "type": "Permanent",
    },
    "miami": {
        "length_km": 5.412, "laps": 57,
        "lap_record_time": "1:29.708",
        "lap_record_holder": "Max Verstappen",
        "lap_record_year": 2023,
        "type": "Street",
    },
    "villeneuve": {
        "length_km": 4.361, "laps": 70,
        "lap_record_time": "1:13.078",
        "lap_record_holder": "Valtteri Bottas",
        "lap_record_year": 2019,
        "type": "Semi-Permanent",
    },
    "monaco": {
        "length_km": 3.337, "laps": 78,
        "lap_record_time": "1:12.909",
        "lap_record_holder": "Lewis Hamilton",
        "lap_record_year": 2021,
        "type": "Street",
    },
    "catalunya": {
        "length_km": 4.657, "laps": 66,
        "lap_record_time": "1:16.330",
        "lap_record_holder": "Max Verstappen",
        "lap_record_year": 2023,
        "type": "Permanent",
    },
    "red_bull_ring": {
        "length_km": 4.318, "laps": 71,
        "lap_record_time": "1:05.619",
        "lap_record_holder": "Carlos Sainz",
        "lap_record_year": 2020,
        "type": "Permanent",
    },
    "silverstone": {
        "length_km": 5.891, "laps": 52,
        "lap_record_time": "1:27.097",
        "lap_record_holder": "Max Verstappen",
        "lap_record_year": 2020,
        "type": "Permanent",
    },
    "spa": {
        "length_km": 7.004, "laps": 44,
        "lap_record_time": "1:46.286",
        "lap_record_holder": "Valtteri Bottas",
        "lap_record_year": 2018,
        "type": "Permanent",
    },
    "hungaroring": {
        "length_km": 4.381, "laps": 70,
        "lap_record_time": "1:16.627",
        "lap_record_holder": "Lewis Hamilton",
        "lap_record_year": 2020,
        "type": "Permanent",
    },
    "zandvoort": {
        "length_km": 4.259, "laps": 72,
        "lap_record_time": "1:11.097",
        "lap_record_holder": "Lewis Hamilton",
        "lap_record_year": 2021,
        "type": "Permanent",
    },
    "monza": {
        "length_km": 5.793, "laps": 53,
        "lap_record_time": "1:21.046",
        "lap_record_holder": "Rubens Barrichello",
        "lap_record_year": 2004,
        "type": "Permanent",
    },
    "madring": {
        # New 2026 venue — Madrid street circuit. Lap record will populate
        # after the inaugural running.
        "length_km": 5.474, "laps": 57,
        "lap_record_time": None,
        "lap_record_holder": None,
        "lap_record_year": None,
        "type": "Street",
    },
    "baku": {
        "length_km": 6.003, "laps": 51,
        "lap_record_time": "1:43.009",
        "lap_record_holder": "Charles Leclerc",
        "lap_record_year": 2019,
        "type": "Street",
    },
    "marina_bay": {
        "length_km": 4.940, "laps": 62,
        "lap_record_time": "1:34.486",
        "lap_record_holder": "Lando Norris",
        "lap_record_year": 2024,
        "type": "Street",
    },
    "americas": {
        "length_km": 5.513, "laps": 56,
        "lap_record_time": "1:36.169",
        "lap_record_holder": "Charles Leclerc",
        "lap_record_year": 2019,
        "type": "Permanent",
    },
    "rodriguez": {
        "length_km": 4.304, "laps": 71,
        "lap_record_time": "1:17.774",
        "lap_record_holder": "Valtteri Bottas",
        "lap_record_year": 2021,
        "type": "Permanent",
    },
    "interlagos": {
        "length_km": 4.309, "laps": 71,
        "lap_record_time": "1:10.540",
        "lap_record_holder": "Valtteri Bottas",
        "lap_record_year": 2018,
        "type": "Permanent",
    },
    "vegas": {
        "length_km": 6.201, "laps": 50,
        "lap_record_time": "1:34.876",
        "lap_record_holder": "Oscar Piastri",
        "lap_record_year": 2024,
        "type": "Street",
    },
    "losail": {
        "length_km": 5.419, "laps": 57,
        "lap_record_time": "1:24.319",
        "lap_record_holder": "Lando Norris",
        "lap_record_year": 2024,
        "type": "Permanent",
    },
    "yas_marina": {
        "length_km": 5.281, "laps": 58,
        "lap_record_time": "1:25.637",
        "lap_record_holder": "Kevin Magnussen",
        "lap_record_year": 2024,
        "type": "Permanent",
    },
}


def circuit_info(ergast_id: str | None) -> dict:
    """Return curated info for a circuit, or an empty dict if unknown.
    Empty-dict-on-miss lets the route response include an `info` field
    unconditionally without sentinel-checks downstream."""
    if not ergast_id:
        return {}
    info = CIRCUIT_INFO.get(ergast_id) or {}
    if not info:
        return {}
    out = dict(info)
    if out.get("length_km") and out.get("laps"):
        out["distance_km"] = round(out["length_km"] * out["laps"], 3)
    return out
