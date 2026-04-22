"""
01_fetch_pvcalc.py
==================
Phase 1: Abruf von Jahres- und Monatserträgen (kWh/kWp) über die PVGIS PVcalc API
für alle 703 Tilt/Azimuth-Kombinationen des konfigurierten Rasters.

Ergebnis:
  - data/raw/pvcalc_{lat}_{lon}_{year}_{timestamp}.json   ← Rohdaten-Cache
  - data/processed/pvcalc_{lat}_{lon}_{year}_{timestamp}_long.csv    ← Long-Format
  - data/processed/pvcalc_{lat}_{lon}_{year}_{timestamp}_pivot_E_y.csv
  - data/processed/pvcalc_{lat}_{lon}_{year}_{timestamp}_pivot_monat_{01-12}.csv

Aufruf:
  python scripts/01_fetch_pvcalc.py
  python scripts/01_fetch_pvcalc.py --cache data/raw/pvcalc_52.772_9.825_2023_....json
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime

import pandas as pd
import requests
from tqdm import tqdm

# Projektverzeichnis zum Suchpfad hinzufügen (für config.py)
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config


# =============================================================================
# Hilfsfunktionen
# =============================================================================

def cache_laden(pfad: str) -> dict:
    """Lädt einen vorhandenen JSON-Cache oder gibt eine leere Struktur zurück."""
    if os.path.exists(pfad):
        with open(pfad, encoding="utf-8") as f:
            cache = json.load(f)
        print(f"  Cache geladen: {len(cache['data'])} vorhandene Einträge in '{pfad}'")
        return cache
    return {"meta": {}, "data": {}}


def cache_speichern(pfad: str, cache: dict) -> None:
    """Speichert den Cache als JSON-Datei."""
    os.makedirs(os.path.dirname(pfad), exist_ok=True)
    with open(pfad, "w", encoding="utf-8") as f:
        json.dump(cache, f, indent=2, ensure_ascii=False)


def pvcalc_abrufen(tilt: int, azimuth: int, jahr: int):
    """
    Ruft die PVGIS PVcalc API für eine Tilt/Azimuth-Kombination ab.
    Gibt E_y (float) und E_m (Liste mit 12 Werten) zurück oder None bei Fehler.
    """
    params = {
        "lat":          config.LAT,
        "lon":          config.LON,
        "peakpower":    config.PEAKPOWER,
        "loss":         config.LOSS,
        "angle":        tilt,
        "aspect":       azimuth,
        "startyear":    jahr,
        "endyear":      jahr,
        "outputformat": "json",
    }

    for versuch in range(1, 4):  # max. 3 Versuche
        try:
            antwort = requests.get(
                config.PVCALC_ENDPOINT,
                params=params,
                timeout=30,
            )
            antwort.raise_for_status()
            daten = antwort.json()

            e_y = daten["outputs"]["totals"]["fixed"]["E_y"]
            e_m = [monat["E_m"] for monat in daten["outputs"]["monthly"]["fixed"]]

            time.sleep(config.REQUEST_DELAY)
            return {"E_y": e_y, "E_m": e_m}

        except requests.RequestException as fehler:
            if versuch == 3:
                print(f"\n  FEHLER bei Tilt={tilt}°, Azi={azimuth}°: {fehler}")
                return None
            time.sleep(config.REQUEST_DELAY * (versuch + 1))  # Exponentielles Backoff

    return None


# =============================================================================
# Daten abrufen
# =============================================================================

def daten_abrufen(cache_pfad: str, jahr: int) -> dict:
    """
    Iteriert über alle 703 Tilt/Azimuth-Kombinationen und füllt den Cache.
    Überspringt bereits vorhandene Einträge.
    """
    cache = cache_laden(cache_pfad)

    # Meta-Informationen setzen (beim ersten Lauf)
    if not cache["meta"]:
        cache["meta"] = {
            "lat":        config.LAT,
            "lon":        config.LON,
            "year":       jahr,
            "peakpower":  config.PEAKPOWER,
            "loss":       config.LOSS,
            "fetched_at": datetime.now().isoformat(timespec="seconds"),
        }

    kombinationen = [
        (tilt, azi)
        for tilt in config.TILTS
        for azi  in config.AZIMUTHS
    ]
    gesamt     = len(kombinationen)
    neu        = 0
    fehler     = 0
    uebersprungen = 0

    print(f"\nStarte PVcalc-Abfragen: {gesamt} Kombinationen")
    print(f"Standort: {config.LAT}°N, {config.LON}°E  |  Jahr: {jahr}")
    print(f"Raster: Tilt {config.TILT_MIN}–{config.TILT_MAX}° ({config.TILT_STEP}°-Schritte), "
          f"Azimuth {config.AZI_MIN}–{config.AZI_MAX}° ({config.AZI_STEP}°-Schritte)\n")

    with tqdm(total=gesamt, unit="Req", ncols=80) as fortschritt:
        for tilt, azi in kombinationen:
            schluessel = f"tilt_{tilt}_azi_{azi}"

            # Überspringen wenn bereits im Cache
            if schluessel in cache["data"]:
                uebersprungen += 1
                fortschritt.update(1)
                continue

            ergebnis = pvcalc_abrufen(tilt, azi, jahr)

            if ergebnis:
                cache["data"][schluessel] = ergebnis
                neu += 1
            else:
                fehler += 1

            # Cache alle 50 neuen Einträge zwischenspeichern
            if neu % 50 == 0 and neu > 0:
                cache_speichern(cache_pfad, cache)

            fortschritt.update(1)

    # Finalen Cache speichern
    cache_speichern(cache_pfad, cache)

    print(f"\nFertig: {neu} neu abgerufen, {uebersprungen} übersprungen, {fehler} Fehler")
    print(f"Cache gespeichert: {cache_pfad}")
    return cache


# =============================================================================
# Daten verarbeiten und speichern
# =============================================================================

def cache_zu_dataframe(cache: dict) -> pd.DataFrame:
    """Wandelt den JSON-Cache in ein Long-Format DataFrame um."""
    zeilen = []
    for schluessel, werte in cache["data"].items():
        teile   = schluessel.split("_")
        tilt    = int(teile[1])
        azimuth = int(teile[3])
        zeile   = {"tilt": tilt, "azimuth": azimuth, "E_y": werte["E_y"]}
        for i, em in enumerate(werte["E_m"], 1):
            zeile[f"E_m{i:02d}"] = em
        zeilen.append(zeile)

    df = pd.DataFrame(zeilen).sort_values(["tilt", "azimuth"]).reset_index(drop=True)
    return df


def pivot_erstellen(df: pd.DataFrame, spalte: str) -> pd.DataFrame:
    """Erstellt Pivot-Tabelle: Index=Tilt (absteigend), Spalten=Azimuth."""
    return (df.pivot(index="tilt", columns="azimuth", values=spalte)
              .sort_index(ascending=False))


def ergebnisse_speichern(df: pd.DataFrame, basisname: str) -> None:
    """Speichert Long-Format und alle Pivot-Tabellen als CSV."""
    os.makedirs(config.DATA_PROCESSED_DIR, exist_ok=True)

    # Long-Format
    long_pfad = os.path.join(config.DATA_PROCESSED_DIR, f"{basisname}_long.csv")
    df.to_csv(long_pfad, index=False)
    print(f"  Long-Format:  {long_pfad}")

    # Jahres-Pivot
    pivot = pivot_erstellen(df, "E_y")
    pivot_pfad = os.path.join(config.DATA_PROCESSED_DIR, f"{basisname}_pivot_E_y.csv")
    pivot.to_csv(pivot_pfad)
    print(f"  Jahres-Pivot: {pivot_pfad}")

    # Monats-Pivots
    for m in range(1, 13):
        spalte = f"E_m{m:02d}"
        pivot  = pivot_erstellen(df, spalte)
        pfad   = os.path.join(config.DATA_PROCESSED_DIR, f"{basisname}_pivot_monat_{m:02d}.csv")
        pivot.to_csv(pfad)
    print(f"  Monats-Pivots: {basisname}_pivot_monat_01.csv bis _monat_12.csv")


# =============================================================================
# Hauptprogramm
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="PVGIS PVcalc-Daten für Tilt/Azimuth-Raster abrufen"
    )
    parser.add_argument(
        "--cache",
        help="Pfad zu einem vorhandenen JSON-Cache (setzt Download fort)",
        default=None,
    )
    parser.add_argument(
        "--jahr",
        type=int,
        default=config.PVCALC_YEAR,
        help=f"Abfragejahr (Standard: {config.PVCALC_YEAR})",
    )
    args = parser.parse_args()

    # Cache-Pfad bestimmen
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    if args.cache:
        cache_pfad = args.cache
    else:
        dateiname  = f"pvcalc_{config.LAT}_{config.LON}_{args.jahr}_{timestamp}.json"
        cache_pfad = os.path.join(config.DATA_RAW_DIR, dateiname)

    # Daten abrufen
    cache = daten_abrufen(cache_pfad, args.jahr)

    # Verarbeiten und speichern
    print("\nVerarbeite Daten ...")
    df = cache_zu_dataframe(cache)
    basisname = f"pvcalc_{config.LAT}_{config.LON}_{args.jahr}_{timestamp}"
    ergebnisse_speichern(df, basisname)

    # Abschlussmeldung
    print(f"\n{'='*60}")
    print("Alle Daten gespeichert.")
    print(f"\nNächste Schritte:")
    print(f"  → Heatmaps erstellen:  python scripts/04_visualize.py")
    print(f"  → Pivot-CSV liegt in:  {config.DATA_PROCESSED_DIR}/")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
