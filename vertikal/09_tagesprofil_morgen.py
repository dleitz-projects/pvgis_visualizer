"""
09_tagesprofil_morgen.py
========================
Tagesverlauf der Leistung im Vergleich zum Spotpreis.

Verglichen werden die konventionelle Ausrichtung und zwei gegenläufig
optimierte vertikale Aufstellungen: eine mit der Vorderseite nach Osten für
maximalen Morgenertrag, eine mit der Vorderseite nach Westen für maximalen
Abendertrag.

Zeigt exemplarisch, warum die vertikale Aufstellung am Markt anders bewertet
wird als die konventionelle Ausrichtung: Die senkrechten Module liefern bereits
in den frühen Stunden Leistung, in denen der Preis noch hoch ist. Die
konventionelle Anlage kommt später und trifft die Stunden, in denen der Preis
durch die einsetzende Solarerzeugung schon gefallen ist.

Dargestellt wird wahlweise ein konkreter Kalendertag (Standard) oder das
Mittel eines Monats:
  - PV-Leistung aus dem Typjahr, also der Mittelwert dieser Jahresstunde über
    alle PVGIS-Jahre
  - Spotpreis in Viertelstundenauflösung des gewählten Tages

Datenquellen: data/processed/{NAME}/wirtschaft/typjahr_profil.parquet
              data/markt/strommarkt.duckdb
Ausgabe:      output/plots/{NAME}/vertikal/08_tagesprofil.png

Die Zeitangaben --von und --bis sind Ortszeit.

Aufruf:
  python vertikal/09_tagesprofil_morgen.py
  python vertikal/09_tagesprofil_morgen.py --monat 6 --von 4 --bis 22
"""

from __future__ import annotations

import argparse
import os
import sys

import duckdb
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

WIRTSCHAFT_DIR = os.path.join(config.DATA_PROCESSED_DIR, "wirtschaft")
PROFIL         = os.path.join(WIRTSCHAFT_DIR, "typjahr_profil.parquet")
OUTPUT_DIR     = os.path.join(config.PLOTS_DIR, "vertikal")

REF_TILT, REF_AZI = 40, 0        # konventionelle Ausrichtung
TILT_VERTIKAL     = 90

MONATSNAMEN = ["Januar", "Februar", "März", "April", "Mai", "Juni", "Juli",
               "August", "September", "Oktober", "November", "Dezember"]

FARBE_REF    = "#27ae60"
FARBE_VERTIKAL = "#e74c3c"   # beide Montagerichtungen, unterschieden durch
                             # die Linienart wie in den Erlösgrafiken
FARBE_PREIS  = "#7f8c8d"

plt.rcParams.update({
    "figure.dpi":     150,
    "font.size":      10,
    "axes.titlesize": 12,
    "axes.labelsize": 10,
    "font.family":    "sans-serif",
})


def gegen_azimuth(azi: int) -> int:
    gegen = azi + 180
    return gegen - 360 if gegen > 180 else gegen


def pv_profil(con, tilt: int, azimuth: int, monat: int,
              tag: int | None) -> pd.Series:
    """Leistung je Stunde (UTC) in kW je kWp, für einen Tag oder einen Monat."""
    filter_tag = f"AND tag = {tag}" if tag else ""
    df = con.execute(f"""
        SELECT stunde, AVG(p_w) / 1000.0 AS kw
        FROM profil
        WHERE tilt = {tilt} AND azimuth = {azimuth} AND monat = {monat} {filter_tag}
        GROUP BY 1 ORDER BY 1""").fetchdf()
    return df.set_index("stunde")["kw"]


def preis_profil(con, monat: int, tag: int | None, jahr: int | None) -> tuple:
    """Spotpreis je Viertelstunde (UTC) für einen Tag oder gemittelt über einen Monat."""
    if jahr is None:
        jahr = con.execute("""
            SELECT MAX(year(ts_utc)) FROM markt.spotpreise WHERE aufloesung_min = 15
        """).fetchone()[0]
    filter_tag = f"AND day(ts_utc) = {tag}" if tag else ""
    df = con.execute(f"""
        SELECT hour(ts_utc) AS stunde, minute(ts_utc) AS minute,
               AVG(preis_eur_mwh) AS preis
        FROM markt.spotpreise
        WHERE year(ts_utc) = {jahr} AND month(ts_utc) = {monat} {filter_tag}
        GROUP BY 1, 2 ORDER BY 1, 2""").fetchdf()
    df["zeit"] = df["stunde"] + df["minute"] / 60.0
    return df, jahr


def plot(monat: int, tag: int | None, jahr: int | None, von: float, bis: float,
         f_rueck: float, versatz: int) -> None:
    con = duckdb.connect()
    con.execute(f"CREATE VIEW profil AS SELECT * FROM read_parquet('{PROFIL}')")
    con.execute(f"ATTACH '{config.MARKT_DB}' AS markt (READ_ONLY)")

    # Sommerzeit: PVGIS und Preise liegen in UTC, dargestellt wird Ortszeit
    stunden_versatz = 2 if 4 <= monat <= 9 else 1
    zone = "MESZ" if stunden_versatz == 2 else "MEZ"

    ref = pv_profil(con, REF_TILT, REF_AZI, monat, tag)

    def vertikal(beta: int, gedreht: bool) -> pd.Series:
        """
        Bifaziale Leistung einer Modulebene. `gedreht` vertauscht Vorder- und
        Rückseite, das Modul ist dann um 180° montiert.
        """
        vorne, hinten = beta - 90, gegen_azimuth(beta - 90)
        if gedreht:
            vorne, hinten = hinten, vorne
        return (pv_profil(con, TILT_VERTIKAL, vorne, monat, tag)
                + f_rueck * pv_profil(con, TILT_VERTIKAL, hinten, monat, tag))

    # Zwei gegenläufig optimierte Aufstellungen derselben Bauform:
    # +versatz mit der Vorderseite nach Osten liefert das Maximum am Morgen,
    # -versatz um 180° gedreht mit der Vorderseite nach Westen das am Abend.
    morgen = vertikal(+versatz, gedreht=False)
    abend  = vertikal(-versatz, gedreht=True)
    preise, preisjahr = preis_profil(con, monat, tag, jahr)
    con.close()

    stunden_lokal = ref.index + stunden_versatz
    preis_lokal   = preise["zeit"] + stunden_versatz

    fig, ax = plt.subplots(figsize=(13, 7))

    # Linienart wie in den übrigen Abbildungen: durchgezogen die Ausrichtung
    # mit positivem Vorzeichen, gestrichelt die gegenläufige Drehung.
    for reihe, farbe, stil, name in [
        (ref,   FARBE_REF,   "--", f"Konventionell: Azimut {REF_AZI}° (Süd), "
                                   f"Neigung {REF_TILT}°"),
        (morgen, FARBE_VERTIKAL, "-",  f"Vertikal bifazial +{versatz}°, Vorderseite "
                                       f"nach Osten: Maximum am Morgen"),
        (abend,  FARBE_VERTIKAL, "--", f"Vertikal bifazial −{versatz}°, Modul um 180° "
                                       f"gedreht, Vorderseite nach Westen: "
                                       f"Maximum am Abend"),
    ]:
        ax.plot(stunden_lokal, reihe.values, color=farbe,
                lw=2.4 if stil == "-" else 1.8, ls=stil,
                marker="o" if stil == "-" else "s", ms=4 if stil == "-" else 3.5,
                alpha=1.0 if stil == "-" else 0.85, label=name)

    ax.set_xlim(von, bis)
    im_fenster = [(h, a, b, c) for h, a, b, c in
                  zip(stunden_lokal, ref.values, morgen.values, abend.values)
                  if von <= h <= bis]
    hoehe = max(max(a, b, c) for _, a, b, c in im_fenster)
    ax.set_ylim(0, hoehe * 1.30)
    ax.set_xticks(range(int(von), int(bis) + 1))
    ax.set_xlabel(f"Uhrzeit ({zone})")
    ax.set_ylabel("Mittlere Leistung (kW je kWp)")
    ax.grid(True, alpha=0.3)

    ax2 = ax.twinx()
    datum = (f"{tag}. {MONATSNAMEN[monat - 1]} {preisjahr}" if tag
             else f"{MONATSNAMEN[monat - 1]} {preisjahr}")
    ax2.plot(preis_lokal, preise["preis"], color=FARBE_PREIS, lw=1.8, ls="--",
             label=f"Spotpreis {datum}, Viertelstundenwerte")
    ax2.set_ylabel("Spotpreis (EUR/MWh)", color=FARBE_PREIS)
    ax2.tick_params(axis="y", colors=FARBE_PREIS)

    linien, namen = ax.get_legend_handles_labels()
    l2, n2 = ax2.get_legend_handles_labels()
    ax.legend(linien + l2, namen + n2, fontsize=8.5, loc="upper left",
              framealpha=0.92)

    quelle_preis = (f"Spotpreise vom {datum}" if tag
                    else f"Spotpreise als Mittel aller Tage im {datum}")
    quelle_pv = (f"PV-Leistung: Typjahr-Wert für den "
                 f"{tag}. {MONATSNAMEN[monat - 1]}" if tag
                 else f"PV-Leistung: Stundenmittel des Typjahrs im "
                      f"{MONATSNAMEN[monat - 1]}")
    ax.set_title(f"Tagesverlauf — Leistung und Spotpreis, {datum}\n"
                 f"{config.NAME}  |  {config.STANDORT_ANZEIGE}  |  "
                 f"{quelle_pv}, gemittelt über alle PVGIS-Jahre  |  "
                 f"{quelle_preis}\n"
                 f"Rückseiten-Wirkungsgrad {f_rueck:.0%}", fontsize=11)
    plt.tight_layout()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    pfad = os.path.join(OUTPUT_DIR, "08_tagesprofil.png")
    fig.savefig(pfad, bbox_inches="tight", dpi=150)
    plt.close(fig)
    print(f"  Gespeichert: {pfad}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--monat", type=int, default=6, help="Monat (Standard: 6)")
    parser.add_argument("--tag", type=int, default=15,
                        help="Kalendertag im Monat (Standard: 15); 0 für das "
                             "Monatsmittel")
    parser.add_argument("--jahr", type=int, default=None,
                        help="Preisjahr (Standard: jüngstes Jahr mit "
                             "Viertelstundenpreisen)")
    parser.add_argument("--von", type=float, default=4,
                        help="Erste dargestellte Stunde (Ortszeit)")
    parser.add_argument("--bis", type=float, default=22,
                        help="Letzte dargestellte Stunde (Ortszeit)")
    parser.add_argument("--rueckseite", type=float, default=0.90)
    parser.add_argument("--versatz", type=int, default=20,
                        help="Ausrichtung der vertikalen Modulebene in ° "
                             "(Standard: 20)")
    args = parser.parse_args()
    plot(args.monat, args.tag or None, args.jahr, args.von, args.bis,
         args.rueckseite, args.versatz)


if __name__ == "__main__":
    main()
