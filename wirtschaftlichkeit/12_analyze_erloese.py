"""
12_analyze_erloese.py
=====================
Verknüpft die stündlichen PV-Zeitreihen mit den historischen Spotpreisen und
berechnet je Tilt/Azimuth-Kombination den Einspeiseerlös bei Direktvermarktung.

Methodik
--------
1. Typjahr-Profil: aus allen verfügbaren PVGIS-Jahren wird je Kombination und
   je Stunde des Jahres (Monat/Tag/Stunde, UTC) der Mittelwert der Leistung
   gebildet. Das glättet einzelne Wetterjahre heraus.
2. Preisjahre: die Erlösrechnung nutzt nur die letzten vollständigen
   Kalenderjahre (Standard 3), weil das aktuelle Preisniveau und die
   Mittagsdelle der Preise das Ergebnis dominieren.
3. Je Kombination und Preisjahr:
       Erlös      = Σ (P_t/1000 · Preis_t/1000)            EUR/kWp
       Erlöspreis = Erlös / Jahresertrag · 1000            EUR/MWh
       Faktor     = Erlöspreis / Basispreis des Jahres     (Capture Rate)
       Abschlag   = Basispreis / Erlöspreis = 1 / Faktor    (Marktabschlag)

Ergebnisse (CSV):
  data/processed/{NAME}/wirtschaft/typjahr_profil.parquet
  data/processed/{NAME}/wirtschaft/erloese_je_jahr.csv
  data/processed/{NAME}/wirtschaft/erloese_mittel.csv

Aufruf:
  python wirtschaftlichkeit/12_analyze_erloese.py
  python wirtschaftlichkeit/12_analyze_erloese.py --jahre 3 --profil-neu
  python wirtschaftlichkeit/12_analyze_erloese.py --tilts 90        ← nur vertikal (schnell)
"""

from __future__ import annotations

import argparse
import glob
import json
import os
import sys

import duckdb
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

WIRTSCHAFT_DIR = os.path.join(config.DATA_PROCESSED_DIR, "wirtschaft")
PROFIL_PARQUET = os.path.join(WIRTSCHAFT_DIR, "typjahr_profil.parquet")
METADATEN      = os.path.join(WIRTSCHAFT_DIR, "erloese_metadaten.json")


# =============================================================================
# Datenquellen
# =============================================================================

def neueste_pv_datenbank() -> str:
    """Jüngste seriescalc-DuckDB des Standorts (nach Endjahr im Dateinamen)."""
    muster = os.path.join(config.DATA_PROCESSED_DIR, "seriescalc_*.duckdb")
    treffer = glob.glob(muster)
    if not treffer:
        sys.exit(f"Keine seriescalc-Datenbank gefunden: {muster}\n"
                 f"Bitte zuerst scripts/02_fetch_seriescalc.py ausführen.")
    def endjahr(pfad: str) -> int:
        return int(os.path.basename(pfad).rsplit("_", 1)[1].split(".")[0])
    return max(treffer, key=endjahr)


def preisjahre_bestimmen(con: duckdb.DuckDBPyConnection) -> list:
    """Alle vollständigen Kalenderjahre mit Preisdaten."""
    df = con.execute("""
        SELECT year(ts_utc)                  AS jahr,
               COUNT(DISTINCT date(ts_utc))  AS tage
        FROM markt.spotpreise
        GROUP BY 1 ORDER BY 1
    """).fetchdf()
    # Vollständig heißt: das Jahr ist durchgehend abgedeckt. Die Zeilenzahl
    # taugt dafür nicht, seit die Preise viertelstündlich vorliegen.
    return df[df["tage"] >= 364]["jahr"].tolist()


# =============================================================================
# Schritt 1: Typjahr-Profil
# =============================================================================

def typjahr_profil(pv_db: str, tilts: list | None, neu: bool) -> str:
    """Mittlere Stundenleistung je Kombination über alle PVGIS-Jahre."""
    os.makedirs(WIRTSCHAFT_DIR, exist_ok=True)
    if os.path.exists(PROFIL_PARQUET) and not neu:
        print(f"  Typjahr-Profil vorhanden: {PROFIL_PARQUET}")
        return PROFIL_PARQUET

    filter_sql = ""
    if tilts:
        filter_sql = f"WHERE tilt IN ({','.join(str(t) for t in tilts)})"

    print(f"  Typjahr-Profil wird berechnet aus {os.path.basename(pv_db)} ...")
    con = duckdb.connect(pv_db, read_only=True)
    con.execute(f"""
        COPY (
            SELECT tilt,
                   azimuth,
                   month(time) AS monat,
                   day(time)   AS tag,
                   hour(time)  AS stunde,
                   AVG(P)      AS p_w,
                   COUNT(*)    AS jahre
            FROM hourly_data
            {filter_sql}
            GROUP BY 1, 2, 3, 4, 5
        ) TO '{PROFIL_PARQUET}' (FORMAT PARQUET)
    """)
    con.close()
    print(f"  Gespeichert: {PROFIL_PARQUET}")
    return PROFIL_PARQUET


# =============================================================================
# Schritt 2: Erlöse je Preisjahr
# =============================================================================

def erloese_berechnen(profil: str, jahre: list) -> pd.DataFrame:
    """Erlös, Erlöspreis und Capture-Faktor je Kombination und Preisjahr."""
    con = duckdb.connect()
    con.execute(f"ATTACH '{config.MARKT_DB}' AS markt (READ_ONLY)")
    con.execute(f"CREATE VIEW profil AS SELECT * FROM read_parquet('{profil}')")

    jahre_sql = ",".join(str(j) for j in jahre)
    df = con.execute(f"""
        WITH preis_stunde AS (
            SELECT year(ts_utc)  AS jahr,
                   month(ts_utc) AS monat,
                   day(ts_utc)   AS tag,
                   hour(ts_utc)  AS stunde,
                   AVG(preis_eur_mwh) AS preis
            FROM markt.spotpreise
            WHERE year(ts_utc) IN ({jahre_sql})
            GROUP BY 1, 2, 3, 4
        ),
        basis AS (
            SELECT jahr, AVG(preis) AS basispreis, COUNT(*) AS stunden
            FROM preis_stunde GROUP BY 1
        ),
        verknuepft AS (
            SELECT p.tilt, p.azimuth, k.jahr,
                   SUM(p.p_w / 1000.0)                        AS e_kwh,
                   SUM(p.p_w / 1000.0 * k.preis / 1000.0)     AS erloes_eur,
                   SUM(CASE WHEN k.preis < 0 THEN p.p_w / 1000.0 ELSE 0 END)
                                                              AS e_kwh_negativ
            FROM profil p
            JOIN preis_stunde k
              ON k.monat = p.monat AND k.tag = p.tag AND k.stunde = p.stunde
            GROUP BY 1, 2, 3
        )
        SELECT v.tilt, v.azimuth, v.jahr,
               v.e_kwh,
               v.erloes_eur,
               v.erloes_eur / NULLIF(v.e_kwh, 0) * 1000.0      AS erloespreis_eur_mwh,
               b.basispreis                                    AS basispreis_eur_mwh,
               v.erloes_eur / NULLIF(v.e_kwh, 0) * 1000.0 / NULLIF(b.basispreis, 0)
                                                               AS faktor,
               b.basispreis / NULLIF(v.erloes_eur / NULLIF(v.e_kwh, 0) * 1000.0, 0)
                                                               AS marktabschlag,
               v.e_kwh_negativ / NULLIF(v.e_kwh, 0) * 100.0    AS anteil_negativ_prozent
        FROM verknuepft v
        JOIN basis b USING (jahr)
        ORDER BY v.tilt, v.azimuth, v.jahr
    """).fetchdf()
    con.close()
    return df


def mittel_ueber_jahre(df: pd.DataFrame, anzahl: int) -> pd.DataFrame:
    """Mittelwert über die letzten `anzahl` Preisjahre je Kombination."""
    jahre = sorted(df["jahr"].unique())[-anzahl:]
    df = df[df["jahr"].isin(jahre)]
    return (df.groupby(["tilt", "azimuth"], as_index=False)
              .agg(e_kwh=("e_kwh", "mean"),
                   erloes_eur=("erloes_eur", "mean"),
                   erloespreis_eur_mwh=("erloespreis_eur_mwh", "mean"),
                   basispreis_eur_mwh=("basispreis_eur_mwh", "mean"),
                   faktor=("faktor", "mean"),
                   marktabschlag=("marktabschlag", "mean"),
                   anteil_negativ_prozent=("anteil_negativ_prozent", "mean"),
                   jahre=("jahr", "count")))


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--jahre", type=int, default=config.PREIS_ANALYSE_JAHRE,
                        help=f"Anzahl der letzten Preisjahre für den Mittelwert "
                             f"in erloese_mittel.csv (Standard: "
                             f"{config.PREIS_ANALYSE_JAHRE}). Die Datei "
                             f"erloese_je_jahr.csv enthält immer alle "
                             f"vollständigen Preisjahre.")
    parser.add_argument("--tilts", type=int, nargs="*", default=None,
                        help="Nur diese Neigungswinkel auswerten (schneller Test)")
    parser.add_argument("--profil-neu", action="store_true",
                        help="Typjahr-Profil neu berechnen")
    parser.add_argument("--pv-db", default=None,
                        help="Pfad zu einer bestimmten seriescalc-Datenbank "
                             "(Standard: die mit dem jüngsten Endjahr)")
    args = parser.parse_args()

    pv_db = args.pv_db or neueste_pv_datenbank()
    print(f"PV-Daten:      {pv_db}")
    print(f"Marktdaten:    {config.MARKT_DB}")

    con = duckdb.connect()
    con.execute(f"ATTACH '{config.MARKT_DB}' AS markt (READ_ONLY)")
    jahre = preisjahre_bestimmen(con)
    con.close()
    if not jahre:
        sys.exit("Keine vollständigen Preisjahre gefunden — bitte zuerst "
                 "wirtschaftlichkeit/10_fetch_strommarkt.py ausführen.")
    print(f"Preisjahre:    {', '.join(str(j) for j in jahre)}  "
          f"(Mittelwert über die letzten {args.jahre})")

    profil = typjahr_profil(pv_db, args.tilts, args.profil_neu)
    df = erloese_berechnen(profil, jahre)

    os.makedirs(WIRTSCHAFT_DIR, exist_ok=True)
    pfad_jahr = os.path.join(WIRTSCHAFT_DIR, "erloese_je_jahr.csv")
    df.to_csv(pfad_jahr, index=False)
    print(f"  Gespeichert: {pfad_jahr}  ({len(df)} Zeilen)")

    mittel = mittel_ueber_jahre(df, args.jahre)
    pfad_mittel = os.path.join(WIRTSCHAFT_DIR, "erloese_mittel.csv")
    mittel.to_csv(pfad_mittel, index=False)
    print(f"  Gespeichert: {pfad_mittel}  ({len(mittel)} Zeilen)")

    # Herkunft der Daten festhalten, damit die Grafiken korrekt beschriftet werden
    pv_jahre = os.path.basename(pv_db).rsplit(".", 1)[0].split("_")[-2:]
    with open(METADATEN, "w", encoding="utf-8") as datei:
        json.dump({"pv_db": os.path.basename(pv_db),
                   "pv_startjahr": int(pv_jahre[0]),
                   "pv_endjahr":   int(pv_jahre[1]),
                   "preisjahre":   [int(j) for j in jahre],
                   "mittel_jahre": [int(j) for j in sorted(jahre)[-args.jahre:]],
                   "tilts":        args.tilts}, datei, indent=2)
    print(f"  Gespeichert: {METADATEN}")

    best = mittel.loc[mittel["erloes_eur"].idxmax()]
    print(f"\nHöchster Erlös: Tilt {best['tilt']:.0f}°, Azimuth {best['azimuth']:+.0f}° "
          f"→ {best['erloes_eur']:.2f} EUR/kWp und Jahr "
          f"({best['e_kwh']:.0f} kWh/kWp, Capture-Faktor {best['faktor']:.2f})")


if __name__ == "__main__":
    main()
