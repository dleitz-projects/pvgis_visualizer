"""
03_analyze.py
=============
Statistische Auswertungen aus der seriescalc DuckDB-Datenbank.

Alle Aggregationen laufen direkt in DuckDB (SQL) — sehr schnell auch bei 61 Mio. Zeilen.
Ergebnisse werden als CSV gespeichert (long-format + Pivot für Heatmaps).

Erzeugte Dateien unter data/processed/{NAME}/stats/:
  jahresertraege.csv          ← E_y mean/std pro Tilt/Azimuth
  monatsertraege.csv          ← E_m mean/std pro Tilt/Azimuth/Monat
  tagesverlauf_gesamt.csv     ← P mean/std pro Tilt/Azimuth/Stunde (ganzes Jahr)
  tagesverlauf_monat_{01-12}.csv ← P mean/std pro Stunde, je Monat

  pivot/E_y_mean.csv, pivot/E_y_std.csv
  pivot/E_m{01-12}_mean.csv,  pivot/E_m{01-12}_std.csv

Aufruf:
  python scripts/03_analyze.py
  python scripts/03_analyze.py --db data/processed/Standort_2/seriescalc_...duckdb
"""

import argparse
import os
import sys
from pathlib import Path

import duckdb
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

MONATSNAMEN_KURZ = ["Jan","Feb","Mär","Apr","Mai","Jun",
                    "Jul","Aug","Sep","Okt","Nov","Dez"]


# =============================================================================
# Hilfsfunktionen
# =============================================================================

def db_finden(ordner: str) -> str:
    """Sucht die größte (vollständigste) seriescalc-Datenbank im Verzeichnis."""
    kandidaten = sorted(Path(ordner).glob("seriescalc_*.duckdb"),
                        key=lambda p: p.stat().st_size, reverse=True)
    if not kandidaten:
        print(f"  FEHLER: Keine seriescalc_*.duckdb in '{ordner}' gefunden.")
        print("  Bitte zuerst 02_fetch_seriescalc.py ausführen.")
        sys.exit(1)
    return str(kandidaten[0])


def pivot_erstellen(df: pd.DataFrame, wert_spalte: str) -> pd.DataFrame:
    """Pivot: Index=Tilt (absteigend), Spalten=Azimuth."""
    return (df.pivot(index="tilt", columns="azimuth", values=wert_spalte)
              .sort_index(ascending=False))


def csv_speichern(df: pd.DataFrame, pfad: str) -> None:
    os.makedirs(os.path.dirname(pfad), exist_ok=True)
    df.to_csv(pfad, index=False)


def pivot_speichern(df: pd.DataFrame, pfad: str) -> None:
    os.makedirs(os.path.dirname(pfad), exist_ok=True)
    df.to_csv(pfad)


# =============================================================================
# Analyse 1: Jahreserträge (mean + std über alle Jahre)
# =============================================================================

def analyse_jahresertraege(con: duckdb.DuckDBPyConnection, stats_dir: str) -> pd.DataFrame:
    """
    Summiert P pro Jahr/Tilt/Azimuth → E_y in kWh/kWp.
    Berechnet mean und std über alle Jahre.
    """
    print("  [1/4] Jahreserträge ...")

    df = con.execute("""
        SELECT
            tilt,
            azimuth,
            YEAR(time)          AS jahr,
            SUM(P) / 1000.0     AS E_y
        FROM hourly_data
        GROUP BY tilt, azimuth, YEAR(time)
    """).fetchdf()

    stats = (df.groupby(["tilt", "azimuth"])["E_y"]
               .agg(jahre="count", E_y_mean="mean", E_y_std="std")
               .reset_index())

    # std=NaN wenn nur 1 Jahr → 0 setzen
    stats["E_y_std"] = stats["E_y_std"].fillna(0)

    csv_speichern(stats, os.path.join(stats_dir, "jahresertraege.csv"))

    pivot_speichern(pivot_erstellen(stats, "E_y_mean"),
                    os.path.join(stats_dir, "pivot", "E_y_mean.csv"))
    pivot_speichern(pivot_erstellen(stats, "E_y_std"),
                    os.path.join(stats_dir, "pivot", "E_y_std.csv"))

    jahre = stats["jahre"].iloc[0]
    e_max = stats["E_y_mean"].max()
    print(f"     {jahre} Jahr/e  |  max. Jahresertrag: {e_max:.0f} kWh/kWp")
    return stats


# =============================================================================
# Analyse 2: Monatserträge (mean + std je Monat)
# =============================================================================

def analyse_monatsertraege(con: duckdb.DuckDBPyConnection, stats_dir: str) -> pd.DataFrame:
    """
    Summiert P pro Jahr/Monat/Tilt/Azimuth → E_m in kWh/kWp.
    Berechnet mean und std über alle Jahre je Monat.
    """
    print("  [2/4] Monatserträge ...")

    df = con.execute("""
        SELECT
            tilt,
            azimuth,
            YEAR(time)          AS jahr,
            MONTH(time)         AS monat,
            SUM(P) / 1000.0     AS E_m
        FROM hourly_data
        GROUP BY tilt, azimuth, YEAR(time), MONTH(time)
    """).fetchdf()

    stats = (df.groupby(["tilt", "azimuth", "monat"])["E_m"]
               .agg(E_m_mean="mean", E_m_std="std")
               .reset_index())

    stats["E_m_std"] = stats["E_m_std"].fillna(0)

    csv_speichern(stats, os.path.join(stats_dir, "monatsertraege.csv"))

    # Pivot pro Monat
    for m in range(1, 13):
        sub = stats[stats["monat"] == m]
        pivot_speichern(pivot_erstellen(sub, "E_m_mean"),
                        os.path.join(stats_dir, "pivot", f"E_m{m:02d}_mean.csv"))
        pivot_speichern(pivot_erstellen(sub, "E_m_std"),
                        os.path.join(stats_dir, "pivot", f"E_m{m:02d}_std.csv"))

    print(f"     12 Monate  |  Pivot-CSVs für mean + std erstellt")
    return stats


# =============================================================================
# Analyse 3: Tagesverlauf gesamt (Stundenmittel über das ganze Jahr)
# =============================================================================

def analyse_tagesverlauf_gesamt(con: duckdb.DuckDBPyConnection, stats_dir: str) -> pd.DataFrame:
    """
    Mittlerer Tagesverlauf (Stundenmittel der Leistung in W/kWp) über alle Tage/Jahre.
    """
    print("  [3/4] Tagesverlauf gesamt ...")

    stats = con.execute("""
        SELECT
            tilt,
            azimuth,
            HOUR(time)      AS stunde,
            AVG(P)          AS P_mean,
            STDDEV_SAMP(P)  AS P_std,
            MIN(P)          AS P_min,
            MAX(P)          AS P_max
        FROM hourly_data
        GROUP BY tilt, azimuth, HOUR(time)
        ORDER BY tilt, azimuth, stunde
    """).fetchdf()

    stats["P_std"] = stats["P_std"].fillna(0)

    csv_speichern(stats, os.path.join(stats_dir, "tagesverlauf_gesamt.csv"))
    print(f"     {len(stats)} Zeilen (703 Kombinationen × 24 Stunden)")
    return stats


# =============================================================================
# Analyse 4: Tagesverlauf je Monat
# =============================================================================

def analyse_tagesverlauf_monate(con: duckdb.DuckDBPyConnection, stats_dir: str) -> None:
    """
    Mittlerer Tagesverlauf pro Monat — zeigt saisonale Unterschiede.
    """
    print("  [4/4] Tagesverlauf je Monat ...")

    stats = con.execute("""
        SELECT
            tilt,
            azimuth,
            MONTH(time)     AS monat,
            HOUR(time)      AS stunde,
            AVG(P)          AS P_mean,
            STDDEV_SAMP(P)  AS P_std,
            MIN(P)          AS P_min,
            MAX(P)          AS P_max
        FROM hourly_data
        GROUP BY tilt, azimuth, MONTH(time), HOUR(time)
        ORDER BY tilt, azimuth, monat, stunde
    """).fetchdf()

    stats["P_std"] = stats["P_std"].fillna(0)

    # Eine CSV pro Monat
    for m in range(1, 13):
        sub = stats[stats["monat"] == m].drop(columns="monat")
        csv_speichern(sub, os.path.join(stats_dir, f"tagesverlauf_monat_{m:02d}.csv"))

    print(f"     12 Monatsdateien erstellt")


# =============================================================================
# Analyse 5: Tageserträge (E_d mean + std pro Tag des Jahres)
# =============================================================================

def analyse_tagesertraege(con: duckdb.DuckDBPyConnection, stats_dir: str) -> pd.DataFrame:
    """
    Summiert P pro Tag/Jahr/Tilt/Azimuth → E_d in kWh/kWp.
    Berechnet mean und std über alle Jahre je Kalendertag (1–365).
    Tag 366 (Schaltjahre) wird verworfen.
    """
    print("  [5/5] Tageserträge (365 Tage) ...")

    df = con.execute("""
        SELECT
            tilt,
            azimuth,
            YEAR(time)          AS jahr,
            DAYOFYEAR(time)     AS tag,
            SUM(P) / 1000.0     AS E_d
        FROM hourly_data
        WHERE DAYOFYEAR(time) <= 365
        GROUP BY tilt, azimuth, YEAR(time), DAYOFYEAR(time)
    """).fetchdf()

    stats = (df.groupby(["tilt", "azimuth", "tag"])["E_d"]
               .agg(E_d_mean="mean", E_d_std="std")
               .reset_index())

    stats["E_d_std"] = stats["E_d_std"].fillna(0)

    csv_speichern(stats, os.path.join(stats_dir, "tagesertraege.csv"))

    n_kombi = len(stats["tag"].unique())
    print(f"     365 Tage  |  {len(stats)} Zeilen ({len(stats)//365} Kombinationen × 365 Tage)")
    return stats


# =============================================================================
# Hauptprogramm
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Statistische Auswertungen aus seriescalc DuckDB"
    )
    parser.add_argument(
        "--db",
        default=None,
        help="Pfad zur DuckDB-Datei (Standard: neueste in data/processed/{NAME}/)",
    )
    args = parser.parse_args()

    db_pfad = args.db or db_finden(config.DATA_PROCESSED_DIR)
    stats_dir = os.path.join(config.DATA_PROCESSED_DIR, "stats")

    print(f"\nPVGIS Analyzer")
    print(f"Datenbank: {db_pfad}")
    print(f"Ausgabe:   {stats_dir}/\n")

    con = duckdb.connect(db_pfad, read_only=True)

    # Überblick
    zeilen = con.execute("SELECT COUNT(*) FROM hourly_data").fetchone()[0]
    jahre  = con.execute("SELECT COUNT(DISTINCT YEAR(time)) FROM hourly_data").fetchone()[0]
    print(f"Datenbankinhalt: {zeilen:,} Zeilen  |  {jahre} Jahr/e\n")
    print("Starte Analysen ...")

    analyse_jahresertraege(con, stats_dir)
    analyse_monatsertraege(con, stats_dir)
    analyse_tagesverlauf_gesamt(con, stats_dir)
    analyse_tagesverlauf_monate(con, stats_dir)
    analyse_tagesertraege(con, stats_dir)

    con.close()

    # Übersicht der erzeugten Dateien
    alle_csvs = list(Path(stats_dir).rglob("*.csv"))
    print(f"\n{'='*60}")
    print(f"Fertig. {len(alle_csvs)} CSV-Dateien unter {stats_dir}/")
    print(f"\nNächste Schritte:")
    print(f"  → Heatmaps aus Statistiken: python scripts/04_visualize.py --modus stats")
    print(f"  → 10-Jahres-Daten abrufen:  python scripts/02_fetch_seriescalc.py")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
