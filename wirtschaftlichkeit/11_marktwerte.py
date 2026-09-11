"""
11_marktwerte.py
================
Berechnet aus den geladenen Strommarktdaten die monatlichen Marktwerte
(erzeugungsgewichtete Spotpreise) für Solar und Wind und legt sie in der
Marktdatenbank ab.

    Marktwert(Monat, Art) = Σ(Erzeugung_t · Preis_t) / Σ Erzeugung_t
    Basispreis(Monat)     = arithmetisches Mittel aller Spotpreise
    Faktor                = Marktwert / Basispreis   ← Wertigkeit der Technologie

Die Berechnung erfolgt auf Stundenbasis, weil die Spotpreise bis 2025
stündlich vorliegen. Die amtlichen Werte der Übertragungsnetzbetreiber
(netztransparenz.de) können zusätzlich als CSV importiert und damit
gegengeprüft werden — sie sind nicht öffentlich per API abrufbar.

Tabellen:
  marktwerte            ← selbst berechnet aus spotpreise + erzeugung
  marktwerte_offiziell  ← optionaler CSV-Import von netztransparenz.de

Aufruf:
  python wirtschaftlichkeit/11_marktwerte.py
  python wirtschaftlichkeit/11_marktwerte.py --netztransparenz-csv ~/Downloads/Marktwerte.csv
"""

from __future__ import annotations

import argparse
import os
import sys

import duckdb
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS marktwerte (
    monat               VARCHAR NOT NULL,   -- YYYY-MM (lokale Zeit)
    art                 VARCHAR NOT NULL,   -- Solar, Wind onshore, Wind offshore
    marktwert_eur_mwh   DOUBLE,
    basispreis_eur_mwh  DOUBLE,
    faktor              DOUBLE,
    stunden             INTEGER,
    PRIMARY KEY (monat, art)
);

CREATE TABLE IF NOT EXISTS marktwerte_offiziell (
    monat               VARCHAR NOT NULL,
    art                 VARCHAR NOT NULL,
    marktwert_eur_mwh   DOUBLE,
    PRIMARY KEY (monat, art)
);
"""

# Zeitzone für die Monatszuordnung: Marktwerte werden nach deutscher Zeit
# abgegrenzt, die Datenbank speichert UTC.
LOKALZEIT = "Europe/Berlin"

ARTEN = ["Solar", "Wind onshore", "Wind offshore"]


def marktwerte_berechnen(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    """Stundenweiser Join von Erzeugung und Preis, danach Monatsaggregation."""
    con.execute("INSTALL icu; LOAD icu;")
    sql = f"""
    WITH preis AS (
        SELECT date_trunc('hour', ts_utc) AS stunde,
               AVG(preis_eur_mwh)         AS preis
        FROM spotpreise
        GROUP BY 1
    ),
    erz AS (
        SELECT date_trunc('hour', ts_utc) AS stunde,
               art,
               AVG(leistung_mw)           AS leistung_mw
        FROM erzeugung
        WHERE art IN ({','.join(f"'{a}'" for a in ARTEN)})
        GROUP BY 1, 2
    ),
    gemeinsam AS (
        SELECT strftime(e.stunde AT TIME ZONE '{LOKALZEIT}', '%Y-%m') AS monat,
               e.art,
               e.leistung_mw,
               p.preis
        FROM erz e
        JOIN preis p USING (stunde)
    ),
    basis AS (
        SELECT strftime(stunde AT TIME ZONE '{LOKALZEIT}', '%Y-%m') AS monat,
               AVG(preis) AS basispreis
        FROM preis
        GROUP BY 1
    )
    SELECT g.monat,
           g.art,
           SUM(g.leistung_mw * g.preis) / NULLIF(SUM(g.leistung_mw), 0) AS marktwert_eur_mwh,
           b.basispreis                                                AS basispreis_eur_mwh,
           COUNT(*)                                                    AS stunden
    FROM gemeinsam g
    JOIN basis b USING (monat)
    GROUP BY g.monat, g.art, b.basispreis
    ORDER BY g.monat, g.art
    """
    df = con.execute(sql).fetchdf()
    df["faktor"] = df["marktwert_eur_mwh"] / df["basispreis_eur_mwh"]
    return df[["monat", "art", "marktwert_eur_mwh", "basispreis_eur_mwh",
               "faktor", "stunden"]]


def offizielle_werte_importieren(con: duckdb.DuckDBPyConnection, pfad: str) -> None:
    """
    Importiert die Marktwertübersicht von netztransparenz.de.

    Download (kostenlos, ohne Anmeldung, als CSV):
    netztransparenz.de → Erneuerbare Energien und Umlagen → EEG →
    Transparenzanforderungen → Marktprämie → Marktwertübersicht

    Erwartet wird eine Spalte mit dem Monat und je eine Spalte je Technologie.
    Semikolon als Trenner und Komma als Dezimalzeichen werden berücksichtigt.
    """
    roh = pd.read_csv(pfad, sep=";", decimal=",", encoding="utf-8-sig")
    roh.columns = [str(c).strip() for c in roh.columns]
    monat_spalte = roh.columns[0]

    zuordnung = {"MW Solar": "Solar", "MW Wind Onshore": "Wind onshore",
                 "MW Wind Offshore": "Wind offshore"}
    zeilen = []
    for spalte in roh.columns[1:]:
        art = zuordnung.get(spalte.strip(), spalte.strip())
        for _, zeile in roh.iterrows():
            wert = pd.to_numeric(zeile[spalte], errors="coerce")
            if pd.isna(wert):
                continue
            monat = str(zeile[monat_spalte]).strip()[:7].replace("/", "-")
            zeilen.append({"monat": monat, "art": art,
                           "marktwert_eur_mwh": float(wert)})

    if not zeilen:
        print("  Keine verwertbaren Zeilen gefunden.")
        return

    df = pd.DataFrame(zeilen)
    con.execute("INSERT OR REPLACE INTO marktwerte_offiziell SELECT * FROM df")
    con.commit()
    print(f"  {len(df)} amtliche Marktwerte importiert aus {pfad}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--netztransparenz-csv", default=None,
                        help="Pfad zur manuell heruntergeladenen Marktwertübersicht")
    args = parser.parse_args()

    con = duckdb.connect(config.MARKT_DB)
    con.execute(SCHEMA_SQL)

    print(f"Marktwerte berechnen aus {config.MARKT_DB}")
    df = marktwerte_berechnen(con)
    con.execute("DELETE FROM marktwerte")
    con.execute("""INSERT INTO marktwerte
                   SELECT monat, art, marktwert_eur_mwh, basispreis_eur_mwh,
                          faktor, stunden FROM df""")
    con.commit()
    print(f"  {len(df)} Monatswerte berechnet "
          f"({df['monat'].min()} bis {df['monat'].max()})")

    if args.netztransparenz_csv:
        offizielle_werte_importieren(con, args.netztransparenz_csv)

    solar = df[df["art"] == "Solar"].tail(12)
    if not solar.empty:
        print("\nMarktwert Solar, letzte 12 Monate (EUR/MWh):")
        print(solar[["monat", "marktwert_eur_mwh", "basispreis_eur_mwh", "faktor"]]
              .to_string(index=False, float_format=lambda x: f"{x:8.2f}"))
    con.close()


if __name__ == "__main__":
    main()
