"""
02_fetch_seriescalc.py
======================
Phase 2: Abruf stündlicher Zeitreihendaten über die PVGIS seriescalc API
für alle 703 Tilt/Azimuth-Kombinationen und einen mehrjährigen Zeitraum.

Ergebnis:
  - data/processed/{NAME}/seriescalc_{lat}_{lon}_{startyear}_{endyear}.duckdb
    Tabelle: hourly_data
    Spalten: time (TIMESTAMP), tilt (INT), azimuth (INT),
             P (DOUBLE),    ← Leistung in W/kWp
             Gb_i, Gd_i, Gr_i (DOUBLE),  ← Strahlungskomponenten W/m²
             H_sun (DOUBLE), T2m (DOUBLE), WS10m (DOUBLE)

Datenmenge:  703 Kombinationen × 10 Jahre × 8760 h ≈ 61 Mio. Zeilen
Laufzeit:    ca. 1–2 Stunden (703 Requests, große Responses)

Aufruf:
  python scripts/02_fetch_seriescalc.py
  python scripts/02_fetch_seriescalc.py --startyear 2010 --endyear 2020
  python scripts/02_fetch_seriescalc.py --endyear 2015          ← kürzerer Zeitraum zum Testen
"""

import argparse
import os
import sys
import time
from datetime import datetime

import duckdb
import pandas as pd
import requests
from tqdm import tqdm

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


# =============================================================================
# Datenbankschema
# =============================================================================

TABELLE = "hourly_data"

# Index für schnelle Abfragen nach Tilt/Azimuth
CREATE_INDEX_SQL = f"""
CREATE INDEX IF NOT EXISTS idx_tilt_azi ON {TABELLE} (tilt, azimuth);
"""

FORTSCHRITT_TABELLE = "fetch_log"

CREATE_LOG_SQL = f"""
CREATE TABLE IF NOT EXISTS {FORTSCHRITT_TABELLE} (
    tilt      INTEGER NOT NULL,
    azimuth   INTEGER NOT NULL,
    fetched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (tilt, azimuth)
);
"""


# =============================================================================
# Hilfsfunktionen
# =============================================================================

def db_verbinden(db_pfad: str) -> duckdb.DuckDBPyConnection:
    """Öffnet die DuckDB-Datenbank und erstellt fetch_log falls nötig."""
    os.makedirs(os.path.dirname(db_pfad), exist_ok=True)
    con = duckdb.connect(db_pfad)
    con.execute(CREATE_LOG_SQL)
    return con


def tabelle_erstellen(con: duckdb.DuckDBPyConnection, df: pd.DataFrame) -> None:
    """Erstellt hourly_data dynamisch aus dem ersten API-Response-DataFrame."""
    tabellen = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
    if TABELLE not in tabellen:
        con.execute(f"CREATE TABLE {TABELLE} AS SELECT * FROM df WHERE 1=0")
        print(f"  Tabellenschema erstellt: {list(df.columns)}")


def bereits_abgerufen(con: duckdb.DuckDBPyConnection, tilt: int, azimuth: int) -> bool:
    """Prüft ob eine Kombination bereits in der Datenbank vorhanden ist."""
    result = con.execute(
        f"SELECT 1 FROM {FORTSCHRITT_TABELLE} WHERE tilt=? AND azimuth=?",
        [tilt, azimuth]
    ).fetchone()
    return result is not None


def als_abgerufen_markieren(con: duckdb.DuckDBPyConnection, tilt: int, azimuth: int) -> None:
    con.execute(
        f"INSERT OR REPLACE INTO {FORTSCHRITT_TABELLE} (tilt, azimuth) VALUES (?, ?)",
        [tilt, azimuth]
    )


def seriescalc_abrufen(tilt: int, azimuth: int, startyear: int, endyear: int):
    """
    Ruft die PVGIS seriescalc API ab.
    Gibt DataFrame mit stündlichen Werten zurück oder None bei Fehler.
    """
    params = {
        "lat":          config.LAT,
        "lon":          config.LON,
        "peakpower":    config.PEAKPOWER,
        "loss":         config.LOSS,
        "angle":        tilt,
        "aspect":       azimuth,
        "startyear":    startyear,
        "endyear":      endyear,
        "outputformat": "json",
        "pvcalculation": 1,
    }

    for versuch in range(1, 4):
        try:
            antwort = requests.get(
                config.SERIES_ENDPOINT,
                params=params,
                timeout=120,  # größere Responses brauchen länger
            )
            antwort.raise_for_status()
            daten = antwort.json()

            stunden = daten["outputs"]["hourly"]
            df = pd.DataFrame(stunden)

            # Zeitstempel parsen
            df["time"] = pd.to_datetime(df["time"], format="%Y%m%d:%H%M")

            # Tilt/Azimuth hinzufügen
            df["tilt"]    = tilt
            df["azimuth"] = azimuth

            # Nur relevante Spalten behalten
            spalten = ["time", "tilt", "azimuth", "P", "Gb_i", "Gd_i", "Gr_i",
                       "H_sun", "T2m", "WS10m"]
            df = df[[s for s in spalten if s in df.columns]]

            time.sleep(config.REQUEST_DELAY)
            return df

        except requests.RequestException as fehler:
            if versuch == 3:
                print(f"\n  FEHLER bei Tilt={tilt}°, Azi={azimuth}°: {fehler}")
                return None
            time.sleep(config.REQUEST_DELAY * (versuch + 2))

    return None


# =============================================================================
# Hauptabruf
# =============================================================================

def daten_abrufen(db_pfad: str, startyear: int, endyear: int) -> None:
    """Iteriert über alle 703 Kombinationen und schreibt Daten in DuckDB."""
    con = db_verbinden(db_pfad)

    kombinationen = [
        (tilt, azi)
        for tilt in config.TILTS
        for azi  in config.AZIMUTHS
    ]
    gesamt = len(kombinationen)

    # Bereits vorhandene Kombinationen zählen
    vorhanden = con.execute(f"SELECT COUNT(*) FROM {FORTSCHRITT_TABELLE}").fetchone()[0]

    jahre = endyear - startyear + 1
    print(f"\nStarte seriescalc-Abfragen: {gesamt} Kombinationen")
    print(f"Standort:  {config.NAME}  |  {config.LAT}°N, {config.LON}°E")
    print(f"Zeitraum:  {startyear}–{endyear}  ({jahre} Jahre, ~{jahre * 8760:,} h/Kombination)")
    print(f"Datenbank: {db_pfad}")
    print(f"Bereits vorhanden: {vorhanden}/{gesamt} Kombinationen\n")

    neu = 0
    fehler = 0

    with tqdm(total=gesamt, unit="Req", ncols=80, initial=vorhanden) as fortschritt:
        for tilt, azi in kombinationen:

            if bereits_abgerufen(con, tilt, azi):
                continue

            df = seriescalc_abrufen(tilt, azi, startyear, endyear)

            if df is not None:
                # Tabelle beim ersten Datensatz dynamisch erstellen
                tabelle_erstellen(con, df)
                con.execute(f"INSERT INTO {TABELLE} SELECT * FROM df")
                als_abgerufen_markieren(con, tilt, azi)
                # Expliziter Commit nach jedem Insert (wichtig auf Netzlaufwerken)
                con.commit()
                neu += 1
            else:
                fehler += 1

            fortschritt.update(1)

    # Index erstellen (nur wenn Tabelle vorhanden)
    tabellen = con.execute("SHOW TABLES").fetchdf()["name"].tolist()
    if TABELLE in tabellen:
        print("\nErstelle Index ...")
        con.execute(CREATE_INDEX_SQL)
        con.commit()
    else:
        print("\nWarnung: Keine Daten in Datenbank — Index wird übersprungen.")

    con.close()

    print(f"\nFertig: {neu} neu abgerufen, {fehler} Fehler")
    print(f"Datenbank: {db_pfad}")

    # Datenbankgröße ausgeben
    groesse_mb = os.path.getsize(db_pfad) / 1024 / 1024
    print(f"Dateigröße: {groesse_mb:.1f} MB")


# =============================================================================
# Hauptprogramm
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="PVGIS seriescalc-Daten für Tilt/Azimuth-Raster in DuckDB speichern"
    )
    parser.add_argument(
        "--startyear",
        type=int,
        default=config.SERIES_STARTYEAR,
        help=f"Startjahr (Standard: {config.SERIES_STARTYEAR})",
    )
    parser.add_argument(
        "--endyear",
        type=int,
        default=config.SERIES_ENDYEAR,
        help=f"Endjahr (Standard: {config.SERIES_ENDYEAR})",
    )
    args = parser.parse_args()

    dateiname = (f"seriescalc_{config.LAT}_{config.LON}"
                 f"_{args.startyear}_{args.endyear}.duckdb")
    db_pfad = os.path.join(config.DATA_PROCESSED_DIR, dateiname)

    daten_abrufen(db_pfad, args.startyear, args.endyear)

    print(f"\n{'='*60}")
    print("Nächste Schritte:")
    print(f"  → Statistiken berechnen: python scripts/03_analyze.py")
    print(f"  → Datenbank direkt abfragen:")
    print(f"    import duckdb")
    print(f"    con = duckdb.connect('{db_pfad}')")
    print(f"    con.execute(\"SELECT COUNT(*) FROM hourly_data\").fetchone()")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
