"""
08_vertikal_erloese_preisjahre.py
=================================
Wie Plot 1 der Vertikaluntersuchung, aber statt des Jahresertrags in kWh der
Einspeiseerlös in EUR/kWp — stundenaufgelöst gerechnet und getrennt für die
drei letzten verfügbaren Preisjahre.

Rechenweg je Ausrichtung und Preisjahr:

    Erlös = Σ_Stunden ( mittlere PV-Leistung der Stunde × Spotpreis der Stunde )

Die PV-Seite ist das Typjahr, also der Mittelwert jeder Jahresstunde über alle
PVGIS-Jahre. Die Preisseite ist das jeweilige Kalenderjahr. Damit zeigt der
Vergleich der Kurven, wie stark sich das Preisjahr auf die beste Ausrichtung
auswirkt, ohne dass Wetterjahre dazwischenfunken.

Datenquelle: data/processed/{NAME}/wirtschaft/erloese_je_jahr.csv
             (aus wirtschaftlichkeit/12_analyze_erloese.py)
Ausgabe:     output/plots/{NAME}/vertikal/03_vertikal_erloes_preisjahre.png
             output/plots/{NAME}/vertikal/04_vertikal_erloes_varianten.png
             output/plots/{NAME}/vertikal/05_vertikal_erloes_relativ_sued40.png
             output/plots/{NAME}/vertikal/06_vertikal_erloes_relativ_langfrist.png
             output/plots/{NAME}/vertikal/07_vertikal_erloes_maxima_pfad.png

Aufruf:
  python vertikal/08_vertikal_erloese_preisjahre.py
  python vertikal/08_vertikal_erloese_preisjahre.py --rueckseite 0.8
"""

from __future__ import annotations

import argparse
import os
import sys

import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

WIRTSCHAFT_DIR = os.path.join(config.DATA_PROCESSED_DIR, "wirtschaft")
OUTPUT_DIR     = os.path.join(config.PLOTS_DIR, "vertikal")

TILT_VERTIKAL = 90

# Ein Farbton je Preisjahr, jüngstes Jahr zuerst und am kräftigsten
FARBEN_JAHRE = ["#c0392b", "#e67e22", "#8e44ad"]
FARBE_MONO   = "#7f8c8d"

plt.rcParams.update({
    "figure.dpi":     150,
    "font.size":      10,
    "axes.titlesize": 12,
    "axes.labelsize": 10,
    "font.family":    "sans-serif",
})


def plot_speichern(fig, dateiname: str) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    pfad = os.path.join(OUTPUT_DIR, dateiname)
    fig.savefig(pfad, bbox_inches="tight", dpi=150)
    plt.close(fig)
    print(f"  Gespeichert: {pfad}")


def gegen_azimuth(azi: int) -> int:
    gegen = azi + 180
    return gegen - 360 if gegen > 180 else gegen


def sued_referenz(df_jahr: pd.DataFrame) -> tuple:
    """Beste Süd-Ausrichtung (Azimuth 0°) über alle Neigungswinkel."""
    sued = df_jahr[df_jahr["azimuth"] == 0]
    zeile = sued.loc[sued["erloes_eur"].idxmax()]
    return int(zeile["tilt"]), float(zeile["erloes_eur"])


def kurven(df_jahr: pd.DataFrame, beta: list, f_rueck: float) -> dict:
    """
    Erlös- und Ertragskurven über die Ausrichtung.

    erloes_eur   ← stundenscharf: Σ (Leistung der Stunde × Preis der Stunde)
    e_kwh        ← Jahresertrag, für den Vergleich mit dem Jahresmittelpreis
    """
    vert = df_jahr[df_jahr["tilt"] == TILT_VERTIKAL].set_index("azimuth")
    erloes = vert["erloes_eur"]
    ertrag = vert["e_kwh"]
    azi_front = [b - 90 for b in beta]
    azi_back  = [gegen_azimuth(a) for a in azi_front]

    def summe(reihe):
        return np.array([reihe.loc[a] + f_rueck * reihe.loc[r]
                         for a, r in zip(azi_front, azi_back)])

    def summe_gedreht(reihe):
        return np.array([reihe.loc[r] + f_rueck * reihe.loc[a]
                         for a, r in zip(azi_front, azi_back)])

    return {
        "bifazial":         summe(erloes),
        "bifazial_gedreht": summe_gedreht(erloes),
        "bifazial_kwh":     summe(ertrag),
        "monofazial":       np.array([erloes.loc[b] for b in beta]),
        "monofazial_kwh":   np.array([ertrag.loc[b] for b in beta]),
    }


def plot_erloese(df: pd.DataFrame, f_rueck: float) -> None:
    jahre = sorted(df["jahr"].unique())[-3:][::-1]   # jüngstes Jahr zuerst
    vert  = df[df["tilt"] == TILT_VERTIKAL]
    if vert.empty:
        sys.exit("Keine Werte für Tilt 90° in erloese_je_jahr.csv.")

    schritt = int(min(np.diff(sorted(vert["azimuth"].unique()))))
    beta    = list(range(-90, 91, schritt))

    fig, ax = plt.subplots(figsize=(13, 7))
    maximum = 0.0
    zusammenfassung = []

    for farbe, jahr in zip(FARBEN_JAHRE, jahre):
        df_jahr = df[df["jahr"] == jahr]
        k = kurven(df_jahr, beta, f_rueck)
        ref_tilt, ref_wert = sued_referenz(df_jahr)
        basispreis = df_jahr["basispreis_eur_mwh"].mean()

        i_max = int(np.argmax(k["bifazial"]))
        maximum = max(maximum, k["bifazial"].max(), ref_wert)

        ax.plot(beta, k["bifazial"], color=farbe, lw=2.4, marker="o", ms=4,
                label=f"{jahr} — bifazial vertikal   "
                      f"(Basispreis {basispreis:.0f} EUR/MWh)")
        ax.plot(beta, k["monofazial"], color=farbe, lw=1.2, ls=":", alpha=0.8,
                label=f"{jahr} — monofazial vertikal")
        ax.axhline(ref_wert, color=farbe, lw=1.4, ls="--", alpha=0.9)
        ax.text(90, ref_wert, f" Süd {ref_tilt}°: {ref_wert:.0f} ",
                color=farbe, fontsize=7.5, va="center", ha="right",
                bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="none", alpha=0.75))
        ax.plot([beta[i_max]], [k["bifazial"][i_max]], marker="*", ms=14,
                color=farbe, zorder=5)

        zusammenfassung.append({
            "jahr": jahr,
            "basispreis_eur_mwh": basispreis,
            "bester_beta": beta[i_max],
            "bifazial_max_eur": k["bifazial"][i_max],
            "sued_tilt": ref_tilt,
            "sued_eur": ref_wert,
            "verhaeltnis": k["bifazial"][i_max] / ref_wert,
        })

    ax.set_xlim(-92, 92)
    ax.set_ylim(0, maximum * 1.28)
    ax.set_xticks(beta)
    ax.set_xticklabels([f"{b:+d}" for b in beta], fontsize=8)
    ax.set_xlabel("Ausrichtung (°)   —   monofazial: Azimuth der Vorderseite "
                  "(0 = Süd)   |   bifazial: 0 = Ost/West-Ebene")
    ax.set_ylabel("Einspeiseerlös (EUR/kWp und Jahr)")
    ax.set_title(f"Vertikale Module (Tilt 90°) — Einspeiseerlös am Spotmarkt "
                 f"je Preisjahr\n"
                 f"{config.NAME}  |  {config.STANDORT_ANZEIGE}  |  "
                 f"PV-Typjahr stundenaufgelöst × Spotpreise des jeweiligen Jahres  |  "
                 f"Rückseiten-Wirkungsgrad {f_rueck:.0%}\n"
                 f"Gestrichelt: beste Süd-Ausrichtung des jeweiligen Jahres",
                 fontsize=11)
    ax.legend(fontsize=8, loc="lower center", ncol=len(jahre), framealpha=0.92)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plot_speichern(fig, "03_vertikal_erloes_preisjahre.png")

    tabelle = pd.DataFrame(zusammenfassung)
    pfad = os.path.join(WIRTSCHAFT_DIR, "vertikal_erloese_preisjahre.csv")
    tabelle.to_csv(pfad, index=False)
    print(f"  Gespeichert: {pfad}")
    print(tabelle.to_string(index=False, float_format=lambda x: f"{x:8.2f}"))


def plot_erloese_varianten(df: pd.DataFrame, f_rueck: float,
                           referenz_tilt: int = 40) -> None:
    """
    Einspeiseerlös je Preisjahr für alle Montagevarianten in einem Plot.

    Je Preisjahr eine Farbe, je Variante eine Linienart:
      durchgezogen  bifazial, Vorderseite auf der Ost-Seite der Ebene
      gestrichelt   bifazial, Modul um 180° gedreht (Vorderseite West-Seite)
      gepunktet     monofazial vertikal
      waagerecht    konventionelle Ausrichtung (Azimut 0°, Neigung
                    `referenz_tilt`) desselben Jahres — derselbe Bezug wie in
                    den normierten Grafiken und im Text

    Alle Werte stundenscharf: Σ (Leistung der Stunde × Spotpreis der Stunde).
    """
    jahre = sorted(df["jahr"].unique())[-3:][::-1]
    vert  = df[df["tilt"] == TILT_VERTIKAL]
    if vert.empty:
        sys.exit("Keine Werte für Tilt 90° in erloese_je_jahr.csv.")

    schritt = int(min(np.diff(sorted(vert["azimuth"].unique()))))
    beta    = list(range(-90, 91, schritt))

    fig, ax = plt.subplots(figsize=(14, 8))
    maximum = 0.0

    for farbe, jahr in zip(FARBEN_JAHRE, jahre):
        df_jahr = df[df["jahr"] == jahr]
        k = kurven(df_jahr, beta, f_rueck)
        ref_tilt, ref_wert, _ = referenz_sued(df_jahr, referenz_tilt)
        maximum = max(maximum, k["bifazial"].max(), ref_wert)

        ax.plot(beta, k["bifazial"], color=farbe, lw=2.4, marker="o", ms=4,
                label=f"{jahr} — bifazial")
        ax.plot(beta, k["bifazial_gedreht"], color=farbe, lw=1.8, ls="--",
                marker="s", ms=3.5, alpha=0.85,
                label=f"{jahr} — bifazial, Modul um 180° gedreht")
        ax.plot(beta, k["monofazial"], color=farbe, lw=1.2, ls=":", alpha=0.8,
                label=f"{jahr} — monofazial vertikal")

        ax.axhline(ref_wert, color=farbe, lw=1.4, ls="-.", alpha=0.9)
        ax.text(90, ref_wert,
                f" Konventionell {jahr}: {ref_wert:.0f} EUR/kWp ",
                color=farbe, fontsize=7.5, va="center", ha="right",
                bbox=dict(boxstyle="round,pad=0.2", fc="white", ec="none", alpha=0.75))

        i_max = int(np.argmax(k["bifazial"]))
        ax.plot([beta[i_max]], [k["bifazial"][i_max]], marker="*", ms=14,
                color=farbe, zorder=5)

    ax.set_xlim(-92, 92)
    ax.set_ylim(0, maximum * 1.30)
    ax.set_xticks(beta)
    ax.set_xticklabels([f"{b:+d}" for b in beta], fontsize=8)
    ax.set_xlabel("Ausrichtung (°)   —   monofazial: Azimuth der Vorderseite "
                  "(0 = Süd)   |   bifazial: 0 = Ost/West-Ebene")
    ax.set_ylabel("Einspeiseerlös (EUR/kWp und Jahr)")
    ax.set_title(f"Vertikale Module (Tilt 90°) — Einspeiseerlös am Spotmarkt, "
                 f"alle Montagevarianten\n"
                 f"{config.NAME}  |  {config.STANDORT_ANZEIGE}  |  "
                 f"stundenscharf: Σ (Leistung der Stunde × Spotpreis der Stunde)  |  "
                 f"Rückseiten-Wirkungsgrad {f_rueck:.0%}\n"
                 f"Strichpunkt: konventionelle Ausrichtung (Azimut 0°, "
                 f"Neigung {ref_tilt}°) des jeweiligen Jahres", fontsize=11)
    ax.legend(fontsize=8, loc="lower center", ncol=len(jahre), framealpha=0.92)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plot_speichern(fig, "04_vertikal_erloes_varianten.png")


def referenz_sued(df_jahr: pd.DataFrame, tilt: int) -> tuple:
    """
    Erlös der konventionellen Ausrichtung (Azimut 0°) beim gewünschten
    Neigungswinkel.

    Fehlt dieser Neigungswinkel im Datensatz, wird auf die beste vorhandene
    Süd-Ausrichtung ausgewichen. Der tatsächlich verwendete Winkel wird
    zurückgegeben und in der Grafik benannt.
    """
    sued = df_jahr[df_jahr["azimuth"] == 0]
    treffer = sued[sued["tilt"] == tilt]
    if not treffer.empty:
        return tilt, float(treffer.iloc[0]["erloes_eur"]), True
    ersatz = sued.loc[sued["erloes_eur"].idxmax()]
    return int(ersatz["tilt"]), float(ersatz["erloes_eur"]), False


def plot_erloese_relativ(df: pd.DataFrame, f_rueck: float, referenz_tilt: int) -> None:
    """
    Einspeiseerlös vertikaler bifazialer Module, normiert auf die
    Süd-Ausrichtung mit dem angegebenen Neigungswinkel (Standard 40°).

        Wert = Erlös(Ausrichtung) / Erlös(Süd, Tilt 40°)

    Die Normierung erfolgt je Preisjahr mit der Referenz desselben Jahres.
    Damit fällt das Preisniveau heraus und übrig bleibt die Frage, welchen
    Anteil des Erlöses einer klassischen Südanlage die vertikale Anlage
    erreicht. 1,00 heißt gleichauf mit der Südreferenz.
    """
    jahre = sorted(df["jahr"].unique())[-3:][::-1]
    vert  = df[df["tilt"] == TILT_VERTIKAL]
    if vert.empty:
        sys.exit("Keine Werte für Tilt 90° in erloese_je_jahr.csv.")

    schritt = int(min(np.diff(sorted(vert["azimuth"].unique()))))
    beta    = list(range(-90, 91, schritt))

    fig, ax = plt.subplots(figsize=(14, 8))
    hinweise = []
    maximum  = 0.0
    minimum  = np.inf

    for farbe, jahr in zip(FARBEN_JAHRE, jahre):
        df_jahr = df[df["jahr"] == jahr]
        k = kurven(df_jahr, beta, f_rueck)
        tilt_ist, ref_wert, exakt = referenz_sued(df_jahr, referenz_tilt)
        if not exakt:
            hinweise.append(f"{jahr}: Tilt {referenz_tilt}° nicht im Datensatz, "
                            f"ersatzweise Tilt {tilt_ist}°")

        vorne   = k["bifazial"] / ref_wert
        gedreht = k["bifazial_gedreht"] / ref_wert
        maximum = max(maximum, vorne.max(), gedreht.max())
        minimum = min(minimum, vorne.min(), gedreht.min())

        ax.plot(beta, vorne, color=farbe, lw=2.4, marker="o", ms=4,
                label=f"{jahr} — bifazial   "
                      f"(konventionell: {ref_wert:.0f} EUR/kWp)")
        ax.plot(beta, gedreht, color=farbe, lw=1.8, ls="--", marker="s", ms=3.5,
                alpha=0.85, label=f"{jahr} — bifazial, Modul um 180° gedreht")

        i_max = int(np.argmax(vorne))
        ax.plot([beta[i_max]], [vorne[i_max]], marker="*", ms=14,
                color=farbe, zorder=5)

    ax.axhline(1.0, color="black", lw=1.4, ls="--")
    ax.text(-89, 1.005, f"Konventionelle Ausrichtung: Azimut 0°, "
                        f"Neigung {referenz_tilt}° (= 1,00)",
            fontsize=8.5, va="bottom")

    # Ausschnitt eng um die Kurven legen, damit Unterschiede sichtbar werden
    untergrenze = min(0.8, np.floor(minimum * 20) / 20)
    ax.set_xlim(-92, 92)
    ax.set_ylim(untergrenze, max(maximum, 1.0) + 0.05)
    ax.set_xticks(beta)
    ax.set_xticklabels([f"{b:+d}" for b in beta], fontsize=8)
    ax.yaxis.set_major_locator(mticker.MultipleLocator(0.05))
    ax.yaxis.set_minor_locator(mticker.MultipleLocator(0.01))
    ax.xaxis.set_minor_locator(mticker.MultipleLocator(schritt / 2))
    ax.set_xlabel("Ausrichtung (°)   —   bifazial: 0 = Ost/West-Ebene, "
                  "+90 = Süd/Nord")
    ax.set_ylabel("Einspeiseerlös relativ zur konventionellen Ausrichtung")
    zusatz = ("\n" + "  ·  ".join(hinweise)) if hinweise else ""
    ax.set_title(f"Vertikal bifazial (Tilt 90°) — Erlös im Verhältnis zur "
                 f"konventionellen Ausrichtung (Azimut 0°, Neigung "
                 f"{referenz_tilt}°)\n"
                 f"{config.NAME}  |  {config.STANDORT_ANZEIGE}  |  "
                 f"stundenscharf: Σ (Leistung der Stunde × Spotpreis der Stunde)  |  "
                 f"Rückseiten-Wirkungsgrad {f_rueck:.0%}\n"
                 f"Normierung je Preisjahr mit der konventionellen Ausrichtung "
                 f"desselben Jahres{zusatz}", fontsize=11)
    ax.legend(fontsize=8, loc="lower center", ncol=len(jahre), framealpha=0.92)
    ax.grid(True, which="major", alpha=0.35)
    ax.grid(True, which="minor", alpha=0.15, lw=0.5)
    plt.tight_layout()
    plot_speichern(fig, "05_vertikal_erloes_relativ_sued40.png")


def plot_erloese_relativ_langfrist(df: pd.DataFrame, f_rueck: float,
                                   referenz_tilt: int, ab_jahr: int) -> None:
    """
    Wie die normierte Grafik, aber über alle Preisjahre ab `ab_jahr`.

    Farbverlauf von alt nach neu, damit der Trend sichtbar wird. Durchgezogen
    ist die bifaziale Montage mit der guten Seite auf der Ost-Seite der Ebene,
    gestrichelt dasselbe Modul um 180° gedreht.
    """
    jahre = [j for j in sorted(df["jahr"].unique()) if j >= ab_jahr]
    if len(jahre) < 2:
        print(f"  Übersprungen (06): weniger als zwei Preisjahre ab {ab_jahr}")
        return

    vert = df[df["tilt"] == TILT_VERTIKAL]
    schritt = int(min(np.diff(sorted(vert["azimuth"].unique()))))
    beta    = list(range(-90, 91, schritt))

    farben = plt.get_cmap("viridis")(np.linspace(0.88, 0.05, len(jahre)))

    fig, ax = plt.subplots(figsize=(14, 8))
    maximum, minimum = 0.0, np.inf
    hinweise = []

    for farbe, jahr in zip(farben, jahre):
        df_jahr = df[df["jahr"] == jahr]
        k = kurven(df_jahr, beta, f_rueck)
        tilt_ist, ref_wert, exakt = referenz_sued(df_jahr, referenz_tilt)
        if not exakt and f"Tilt {tilt_ist}" not in " ".join(hinweise):
            hinweise.append(f"Tilt {referenz_tilt}° nicht im Datensatz, "
                            f"ersatzweise Tilt {tilt_ist}°")

        vorne   = k["bifazial"] / ref_wert
        gedreht = k["bifazial_gedreht"] / ref_wert
        maximum = max(maximum, vorne.max(), gedreht.max())
        minimum = min(minimum, vorne.min(), gedreht.min())

        ax.plot(beta, vorne, color=farbe, lw=2.2, marker="o", ms=3.5,
                label=f"{jahr}   (konventionell: {ref_wert:.0f} EUR/kWp)")
        ax.plot(beta, gedreht, color=farbe, lw=1.2, ls="--", alpha=0.75)

    ax.axhline(1.0, color="black", lw=1.4, ls="--")
    ax.text(-89, 1.005, f"Konventionelle Ausrichtung: Azimut 0°, "
                        f"Neigung {referenz_tilt}° (= 1,00)",
            fontsize=8.5, va="bottom")

    untergrenze = min(0.8, np.floor(minimum * 20) / 20)
    ax.set_xlim(-92, 92)
    ax.set_ylim(untergrenze, max(maximum, 1.0) + 0.05)
    ax.set_xticks(beta)
    ax.set_xticklabels([f"{b:+d}" for b in beta], fontsize=8)
    ax.yaxis.set_major_locator(mticker.MultipleLocator(0.05))
    ax.yaxis.set_minor_locator(mticker.MultipleLocator(0.01))
    ax.xaxis.set_minor_locator(mticker.MultipleLocator(schritt / 2))
    ax.set_xlabel("Ausrichtung (°)   —   bifazial: 0 = Ost/West-Ebene, "
                  "+90 = Süd/Nord")
    ax.set_ylabel("Einspeiseerlös relativ zur konventionellen Ausrichtung")
    zusatz = ("\n" + "  ·  ".join(hinweise)) if hinweise else ""
    ax.set_title(f"Vertikal bifazial (Tilt 90°) — Erlös relativ zur "
                 f"konventionellen Ausrichtung, "
                 f"Preisjahre {jahre[0]}–{jahre[-1]}\n"
                 f"{config.NAME}  |  {config.STANDORT_ANZEIGE}  |  "
                 f"stundenscharf: Σ (Leistung der Stunde × Spotpreis der Stunde)  |  "
                 f"Rückseiten-Wirkungsgrad {f_rueck:.0%}\n"
                 f"Normierung je Preisjahr mit der konventionellen Ausrichtung  ·  "
                 f"durchgezogen: bifazial, gestrichelt: Modul um 180° "
                 f"gedreht{zusatz}", fontsize=11)
    ax.legend(fontsize=8, loc="lower center", ncol=min(4, len(jahre)),
              framealpha=0.92)
    ax.grid(True, which="major", alpha=0.35)
    ax.grid(True, which="minor", alpha=0.15, lw=0.5)
    plt.tight_layout()
    plot_speichern(fig, "06_vertikal_erloes_relativ_langfrist.png")


def plot_maxima_pfad(df: pd.DataFrame, f_rueck: float,
                     referenz_tilt: int, ab_jahr: int) -> None:
    """
    Wie die Langfristgrafik, zusätzlich der Wanderungspfad der Optima.

    Für jedes Preisjahr wird das Maximum der durchgezogenen Kurve markiert und
    mit dem Maximum des Folgejahres durch einen Pfeil verbunden. Dasselbe für
    die gestrichelte Kurve, also die um 180° gedrehte Montage. Die Kurven
    selbst treten farblich zurück, damit der Pfad lesbar bleibt.
    """
    jahre = [j for j in sorted(df["jahr"].unique()) if j >= ab_jahr]
    if len(jahre) < 2:
        print(f"  Übersprungen (07): weniger als zwei Preisjahre ab {ab_jahr}")
        return

    vert = df[df["tilt"] == TILT_VERTIKAL]
    schritt = int(min(np.diff(sorted(vert["azimuth"].unique()))))
    beta    = list(range(-90, 91, schritt))
    farben  = plt.get_cmap("viridis")(np.linspace(0.88, 0.05, len(jahre)))

    fig, ax = plt.subplots(figsize=(14, 8))
    maximum, minimum = 0.0, np.inf
    pfad_solid, pfad_dash = [], []

    for farbe, jahr in zip(farben, jahre):
        df_jahr = df[df["jahr"] == jahr]
        k = kurven(df_jahr, beta, f_rueck)
        tilt_ist, ref_wert, _ = referenz_sued(df_jahr, referenz_tilt)

        vorne   = k["bifazial"] / ref_wert
        gedreht = k["bifazial_gedreht"] / ref_wert
        maximum = max(maximum, vorne.max(), gedreht.max())
        minimum = min(minimum, vorne.min(), gedreht.min())

        ax.plot(beta, vorne, color=farbe, lw=1.6, alpha=0.30)
        ax.plot(beta, gedreht, color=farbe, lw=1.0, ls="--", alpha=0.30)

        i, j = int(np.argmax(vorne)), int(np.argmax(gedreht))
        pfad_solid.append((jahr, beta[i], vorne[i], farbe))
        pfad_dash.append((jahr, beta[j], gedreht[j], farbe))

    def pfad_zeichnen(punkte, form, farbe_pfeil, nach_rechts, name):
        for (jahr, x, y, farbe), (_, x2, y2, _) in zip(punkte, punkte[1:]):
            ax.annotate("", xy=(x2, y2), xytext=(x, y),
                        arrowprops=dict(arrowstyle="-|>", lw=1.6,
                                        color=farbe_pfeil, alpha=0.85,
                                        shrinkA=7, shrinkB=9,
                                        connectionstyle="arc3,rad=0.12"))
        for jahr, x, y, farbe in punkte:
            ax.plot([x], [y], marker=form, ms=11, color=farbe,
                    markeredgecolor=farbe_pfeil, markeredgewidth=0.8, zorder=6)
            # Jahreszahlen seitlich versetzen, damit sie sich nicht überlagern
            ax.annotate(str(jahr), xy=(x, y),
                        xytext=(13 if nach_rechts else -13, 0),
                        textcoords="offset points",
                        ha="left" if nach_rechts else "right", va="center",
                        fontsize=8, color=farbe_pfeil, weight="bold")
        # Eintrag für die Legende
        ax.plot([], [], marker=form, ls="-" if nach_rechts else "--",
                color=farbe_pfeil, ms=8, label=name)

    pfad_zeichnen(pfad_solid, "*", "#111111", True,
                  "Optimum je Jahr — bifazial (Pfeil: zum Folgejahr)")
    pfad_zeichnen(pfad_dash, "s", "#8e44ad", False,
                  "Optimum je Jahr — Modul um 180° gedreht")

    ax.axhline(1.0, color="black", lw=1.4, ls="--", alpha=0.7)
    ax.text(-89, 1.005, f"Konventionelle Ausrichtung: Azimut 0°, "
                        f"Neigung {referenz_tilt}° (= 1,00)",
            fontsize=8.5, va="bottom")

    untergrenze = min(0.8, np.floor(minimum * 20) / 20)
    ax.set_xlim(-92, 92)
    ax.set_ylim(untergrenze, max(maximum, 1.0) + 0.06)
    ax.set_xticks(beta)
    ax.set_xticklabels([f"{b:+d}" for b in beta], fontsize=8)
    ax.yaxis.set_major_locator(mticker.MultipleLocator(0.05))
    ax.yaxis.set_minor_locator(mticker.MultipleLocator(0.01))
    ax.xaxis.set_minor_locator(mticker.MultipleLocator(schritt / 2))
    ax.set_xlabel("Ausrichtung (°)   —   bifazial: 0 = Ost/West-Ebene, "
                  "+90 = Süd/Nord")
    ax.set_ylabel("Einspeiseerlös relativ zur konventionellen Ausrichtung")
    ax.set_title(f"Vertikal bifazial (Tilt 90°) — Wanderung des Erlösoptimums "
                 f"{jahre[0]}–{jahre[-1]}\n"
                 f"{config.NAME}  |  {config.STANDORT_ANZEIGE}  |  "
                 f"stundenscharf: Σ (Leistung der Stunde × Spotpreis der Stunde)  |  "
                 f"Rückseiten-Wirkungsgrad {f_rueck:.0%}\n"
                 f"Normierung je Preisjahr mit der konventionellen Ausrichtung  ·  "
                 f"Pfeile verbinden das Optimum eines Jahres mit dem des "
                 f"Folgejahres", fontsize=11)
    ax.legend(fontsize=8.5, loc="lower center", framealpha=0.92)
    ax.grid(True, which="major", alpha=0.35)
    ax.grid(True, which="minor", alpha=0.15, lw=0.5)
    plt.tight_layout()
    plot_speichern(fig, "07_vertikal_erloes_maxima_pfad.png")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rueckseite", type=float, default=0.90,
                        help="Wirkungsgrad der Rückseite (Standard: 0.90)")
    parser.add_argument("--ab-jahr", type=int, default=2018,
                        help="Erstes Preisjahr für die Langfristgrafik "
                             "(Standard: 2018)")
    parser.add_argument("--referenz-tilt", type=int, default=40,
                        help="Neigungswinkel der konventionellen Ausrichtung "
                             "für die normierten Grafiken (Standard: 40)")
    args = parser.parse_args()

    pfad = os.path.join(WIRTSCHAFT_DIR, "erloese_je_jahr.csv")
    if not os.path.exists(pfad):
        sys.exit(f"Fehlt: {pfad} — bitte zuerst "
                 f"wirtschaftlichkeit/12_analyze_erloese.py ausführen.")
    df = pd.read_csv(pfad)
    print(f"Erlösdaten: {pfad}  ({len(df)} Zeilen, "
          f"Preisjahre {sorted(df['jahr'].unique())})")
    plot_erloese(df, args.rueckseite)
    plot_erloese_varianten(df, args.rueckseite, args.referenz_tilt)
    plot_erloese_relativ(df, args.rueckseite, args.referenz_tilt)
    plot_erloese_relativ_langfrist(df, args.rueckseite, args.referenz_tilt,
                                   args.ab_jahr)
    plot_maxima_pfad(df, args.rueckseite, args.referenz_tilt, args.ab_jahr)


if __name__ == "__main__":
    main()
