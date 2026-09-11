"""
10_fetch_strommarkt.py
======================
Lädt historische Strommarktdaten für Deutschland in eine DuckDB-Datenbank.

Quelle: Energy-Charts API des Fraunhofer ISE
        (https://api.energy-charts.info, Datenbasis SMARD/Bundesnetzagentur,
        Lizenz CC BY 4.0, kein API-Schlüssel nötig)

Datensätze:
  spotpreise   ← Day-Ahead-Auktionspreise EUR/MWh
                 2015 bis 30.09.2018 Gebotszone DE-AT-LU, danach DE-LU
                 stündlich, seit der Umstellung der Auktion viertelstündlich
  erzeugung    ← Öffentliche Netzeinspeisung je Energieträger in MW
                 (Solar, Wind onshore/offshore, Last) — Basis für den
                 selbst berechneten Marktwert Solar

Die Datenbank wird bewusst vollständig heruntergeladen (2015 bis heute), damit
spätere Auswertungen beliebige Zeitfenster verknüpfen können. Der Abruf ist
monatsweise und wiederaufnehmbar: bereits geladene Monate werden übersprungen.

Aufruf:
  python wirtschaftlichkeit/10_fetch_strommarkt.py
  python wirtschaftlichkeit/10_fetch_strommarkt.py --ab 2020-01-01
  python wirtschaftlichkeit/10_fetch_strommarkt.py --nur preise
  python wirtschaftlichkeit/10_fetch_strommarkt.py --neu-laden 2026-08
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import date, datetime, timedelta

import duckdb
import pandas as pd
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# =============================================================================
# Konstanten
# =============================================================================

# Erzeugungsarten, die gespeichert werden (die API liefert deutlich mehr;
# diese vier reichen für Marktwerte und Einordnung und halten die DB schlank).
ERZEUGUNGSARTEN = ["Solar", "Wind onshore", "Wind offshore", "Load"]

REQUEST_DELAY = 1.0   # Sekunden zwischen Requests
MAX_VERSUCHE  = 5

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS spotpreise (
    ts_utc          TIMESTAMP NOT NULL,
    zone            VARCHAR   NOT NULL,
    preis_eur_mwh   DOUBLE,
    aufloesung_min  INTEGER,
    PRIMARY KEY (ts_utc, zone)
);

CREATE TABLE IF NOT EXISTS erzeugung (
    ts_utc          TIMESTAMP NOT NULL,
    land            VARCHAR   NOT NULL,
    art             VARCHAR   NOT NULL,
    leistung_mw     DOUBLE,
    aufloesung_min  INTEGER,
    PRIMARY KEY (ts_utc, land, art)
);

CREATE TABLE IF NOT EXISTS fetch_log_markt (
    datensatz   VARCHAR NOT NULL,
    schluessel  VARCHAR NOT NULL,   -- Gebotszone bzw. Land
    monat       VARCHAR NOT NULL,   -- YYYY-MM
    zeilen      INTEGER,
    geladen_am  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (datensatz, schluessel, monat)
);
"""


# =============================================================================
# Hilfsfunktionen
# =============================================================================

def db_verbinden() -> duckdb.DuckDBPyConnection:
    os.makedirs(config.MARKT_DIR, exist_ok=True)
    con = duckdb.connect(config.MARKT_DB)
    con.execute(SCHEMA_SQL)
    return con


def monate(von: date, bis: date):
    """Erzeugt (monatsschluessel, erster_tag, letzter_tag) für jeden Monat."""
    aktuell = date(von.year, von.month, 1)
    while aktuell <= bis:
        if aktuell.month == 12:
            naechster = date(aktuell.year + 1, 1, 1)
        else:
            naechster = date(aktuell.year, aktuell.month + 1, 1)
        start = max(aktuell, von)
        ende  = min(naechster - timedelta(days=1), bis)
        yield f"{aktuell.year}-{aktuell.month:02d}", start, ende
        aktuell = naechster


def api_abrufen(pfad: str, params: dict) -> dict | None:
    """GET auf die Energy-Charts API mit Wiederholung bei Fehlern und 429."""
    url = f"{config.ENERGY_CHARTS_BASE}/{pfad}"
    for versuch in range(1, MAX_VERSUCHE + 1):
        try:
            antwort = requests.get(url, params=params, timeout=90)
            if antwort.status_code == 429:          # Too Many Requests
                time.sleep(5 * versuch)
                continue
            antwort.raise_for_status()
            if not antwort.text.strip().startswith("{"):
                return None                          # z. B. "no content available"
            return antwort.json()
        except requests.RequestException as fehler:
            if versuch == MAX_VERSUCHE:
                print(f"    FEHLER {pfad} {params}: {fehler}")
                return None
            time.sleep(3 * versuch)
    return None


def aufloesung_min(unix_sekunden: list) -> int:
    """Zeitliche Auflösung der Reihe in Minuten (Median der Abstände)."""
    if len(unix_sekunden) < 2:
        return 60
    diffs = pd.Series(unix_sekunden).diff().dropna()
    return int(diffs.median() // 60)


def log_eintragen(con, datensatz: str, schluessel: str, monat: str, zeilen: int) -> None:
    con.execute(
        """INSERT OR REPLACE INTO fetch_log_markt
           (datensatz, schluessel, monat, zeilen, geladen_am)
           VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP)""",
        [datensatz, schluessel, monat, zeilen])


def bereits_geladen(con, datensatz: str, schluessel: str, monat: str) -> bool:
    treffer = con.execute(
        """SELECT zeilen FROM fetch_log_markt
           WHERE datensatz=? AND schluessel=? AND monat=?""",
        [datensatz, schluessel, monat]).fetchone()
    return treffer is not None and treffer[0] > 0


# =============================================================================
# Spotpreise
# =============================================================================

def preise_laden(con, ab: date, bis: date, neu_laden: set) -> None:
    print("\nSpotpreise (Day-Ahead)")
    for zone, zone_von, zone_bis in config.PREIS_ZONEN:
        von = max(ab, datetime.strptime(zone_von, "%Y-%m-%d").date())
        ende = bis if zone_bis is None else min(
            bis, datetime.strptime(zone_bis, "%Y-%m-%d").date())
        if von > ende:
            continue

        print(f"  Gebotszone {zone}: {von} bis {ende}")
        neu = uebersprungen = 0

        for monat, start, stop in monate(von, ende):
            if bereits_geladen(con, "spotpreise", zone, monat) and monat not in neu_laden:
                uebersprungen += 1
                continue

            daten = api_abrufen("price", {"bzn": zone,
                                          "start": start.isoformat(),
                                          "end":   stop.isoformat()})
            time.sleep(REQUEST_DELAY)
            if not daten or not daten.get("unix_seconds"):
                log_eintragen(con, "spotpreise", zone, monat, 0)
                continue

            df = pd.DataFrame({
                "ts_utc": pd.to_datetime(daten["unix_seconds"], unit="s"),
                "zone":   zone,
                "preis_eur_mwh":  daten["price"],
                "aufloesung_min": aufloesung_min(daten["unix_seconds"]),
            }).dropna(subset=["preis_eur_mwh"])

            con.execute("""INSERT OR REPLACE INTO spotpreise
                           SELECT ts_utc, zone, preis_eur_mwh, aufloesung_min FROM df""")
            log_eintragen(con, "spotpreise", zone, monat, len(df))
            con.commit()
            neu += 1
            print(f"    {monat}: {len(df):>5} Werte", end="\r")

        print(f"    neu geladen: {neu} Monate, übersprungen: {uebersprungen}     ")


# =============================================================================
# Erzeugung
# =============================================================================

def erzeugung_laden(con, ab: date, bis: date, neu_laden: set) -> None:
    land = config.ERZEUGUNG_ZONE
    print(f"\nErzeugung und Last ({land}): {', '.join(ERZEUGUNGSARTEN)}")
    neu = uebersprungen = 0

    for monat, start, stop in monate(ab, bis):
        if bereits_geladen(con, "erzeugung", land, monat) and monat not in neu_laden:
            uebersprungen += 1
            continue

        daten = api_abrufen("public_power", {"country": land.lower(),
                                             "start": start.isoformat(),
                                             "end":   stop.isoformat()})
        time.sleep(REQUEST_DELAY)
        if not daten or not daten.get("unix_seconds"):
            log_eintragen(con, "erzeugung", land, monat, 0)
            continue

        ts   = pd.to_datetime(daten["unix_seconds"], unit="s")
        aufl = aufloesung_min(daten["unix_seconds"])
        teile = []
        for eintrag in daten["production_types"]:
            if eintrag["name"] not in ERZEUGUNGSARTEN:
                continue
            teile.append(pd.DataFrame({
                "ts_utc": ts,
                "land":   land,
                "art":    eintrag["name"],
                "leistung_mw":    eintrag["data"],
                "aufloesung_min": aufl,
            }))
        if not teile:
            log_eintragen(con, "erzeugung", land, monat, 0)
            continue

        df = pd.concat(teile, ignore_index=True).dropna(subset=["leistung_mw"])
        con.execute("""INSERT OR REPLACE INTO erzeugung
                       SELECT ts_utc, land, art, leistung_mw, aufloesung_min FROM df""")
        log_eintragen(con, "erzeugung", land, monat, len(df))
        con.commit()
        neu += 1
        print(f"    {monat}: {len(df):>6} Werte", end="\r")

    print(f"    neu geladen: {neu} Monate, übersprungen: {uebersprungen}     ")


# =============================================================================
# Übersicht
# =============================================================================

def uebersicht(con) -> None:
    print(f"\n{'='*64}\nDatenbank: {config.MARKT_DB}")
    for tabelle, zeit in [("spotpreise", "ts_utc"), ("erzeugung", "ts_utc")]:
        zeile = con.execute(f"""SELECT COUNT(*), MIN({zeit}), MAX({zeit})
                                FROM {tabelle}""").fetchone()
        print(f"  {tabelle:<12} {zeile[0]:>10,} Zeilen   {zeile[1]}  bis  {zeile[2]}")
    print(f"{'='*64}\n")


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--ab", default="2015-01-01",
                        help="Startdatum (Standard: 2015-01-01, Beginn der SMARD-Daten)")
    parser.add_argument("--bis", default=None,
                        help="Enddatum (Standard: gestern)")
    parser.add_argument("--nur", choices=["preise", "erzeugung"], default=None,
                        help="Nur einen Datensatz laden")
    parser.add_argument("--neu-laden", nargs="*", default=[], metavar="YYYY-MM",
                        help="Diese Monate erneut laden (z. B. der laufende Monat)")
    args = parser.parse_args()

    ab  = datetime.strptime(args.ab, "%Y-%m-%d").date()
    bis = (datetime.strptime(args.bis, "%Y-%m-%d").date() if args.bis
           else date.today() - timedelta(days=1))
    neu_laden = set(args.neu_laden)

    con = db_verbinden()
    print(f"Strommarktdaten {ab} bis {bis}  →  {config.MARKT_DB}")

    if args.nur in (None, "preise"):
        preise_laden(con, ab, bis, neu_laden)
    if args.nur in (None, "erzeugung"):
        erzeugung_laden(con, ab, bis, neu_laden)

    uebersicht(con)
    con.close()


if __name__ == "__main__":
    main()
