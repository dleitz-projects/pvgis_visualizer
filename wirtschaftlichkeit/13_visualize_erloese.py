"""
13_visualize_erloese.py
=======================
Grafiken zur Rentabilität: Einspeiseerlöse bei Direktvermarktung am Spotmarkt,
je Tilt/Azimuth und für vertikale bifaziale Module.

Datenquelle: data/processed/{NAME}/wirtschaft/erloese_mittel.csv
             (aus 12_analyze_erloese.py)
Ausgabe:     output/plots/{NAME}/wirtschaft/

Plots:
  01_erloes_heatmap.png        ← Erlös EUR/kWp und Jahr über das ganze Raster
  02_marktabschlag.png         ← Basispreis geteilt durch Erlöspreis: um diesen
                                 Faktor liegt der Marktpreis über dem Erlöspreis
  03_vertikal_bifazial_erloes.png ← Erlös vertikaler bifazialer Module über die
                                    Ausrichtung, mit Süd-Referenz
  04_varianten_vergleich.png   ← Ertrag, Erlös und Capture-Faktor ausgewählter
                                 Varianten nebeneinander
  05_erloes_heatmap_{jahr}.png ← wie die Jahresertrag-Heatmap, aber mit dem
                                 stundenscharf gerechneten Erlös, je Preisjahr
  06_erloes_heatmap_jahre_gitter.png ← alle Preisjahre untereinander

Aufruf:
  python wirtschaftlichkeit/13_visualize_erloese.py
  python wirtschaftlichkeit/13_visualize_erloese.py --rueckseite 0.8
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

WIRTSCHAFT_DIR = os.path.join(config.DATA_PROCESSED_DIR, "wirtschaft")
OUTPUT_DIR     = os.path.join(config.PLOTS_DIR, "wirtschaft")

FARBE_VORNE  = "#e74c3c"
FARBE_HINTEN = "#3498db"
FARBE_MONO   = "#7f8c8d"
FARBE_REF    = "#27ae60"

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


def metadaten() -> dict:
    """Herkunft der Auswertung (aus 12_analyze_erloese.py)."""
    pfad = os.path.join(WIRTSCHAFT_DIR, "erloese_metadaten.json")
    if os.path.exists(pfad):
        with open(pfad, encoding="utf-8") as datei:
            return json.load(datei)
    return {}


def kopfzeile(df: pd.DataFrame) -> str:
    meta = metadaten()
    pv = (f"{meta['pv_startjahr']}–{meta['pv_endjahr']}" if meta
          else f"{config.SERIES_STARTYEAR}–{config.SERIES_ENDYEAR}")
    preise = ", ".join(str(j) for j in meta.get("preisjahre", []))
    return (f"{config.NAME}  |  {config.STANDORT_ANZEIGE}  |  "
            f"PV-Typjahr aus PVGIS {pv}  |  "
            f"Spotpreise {preise}  |  "
            f"Basispreis {df['basispreis_eur_mwh'].mean():.1f} EUR/MWh")


# =============================================================================
# Plot 1 und 2: Heatmaps über das Raster
# =============================================================================

def heatmap(df: pd.DataFrame, wert: str, titel: str, cmap: str,
            fmt: str, dateiname: str) -> None:
    pivot = (df.pivot(index="tilt", columns="azimuth", values=wert)
               .sort_index(ascending=False))
    if pivot.shape[0] < 2:
        print(f"  Übersprungen ({dateiname}): nur ein Neigungswinkel im Datensatz")
        return

    max_pos = np.unravel_index(np.nanargmax(pivot.values), pivot.shape)
    fig, ax = plt.subplots(figsize=(15, 7))
    sns.heatmap(pivot, ax=ax, cmap=cmap, cbar=True, linewidths=0.2,
                linecolor="white", annot=True, fmt=fmt,
                annot_kws={"size": 5})
    ax.add_patch(plt.Rectangle((max_pos[1], max_pos[0]), 1, 1,
                 fill=False, edgecolor="blue", lw=2.5))
    ax.set_xlabel("Azimuth (°)  [−180/+180 = Nord, −90 = Ost, 0 = Süd, +90 = West]",
                  fontsize=9)
    ax.set_ylabel("Neigungswinkel / Tilt (°)", fontsize=9)
    ax.set_title(f"{titel}\n{kopfzeile(df)}\n"
                 f"Optimum: Tilt {pivot.index[max_pos[0]]}°, "
                 f"Azimuth {pivot.columns[max_pos[1]]:+d}° "
                 f"= {pivot.values[max_pos]:.2f}", fontsize=11)
    plt.tight_layout()
    plot_speichern(fig, dateiname)


def sigfig_annot(pivot: pd.DataFrame, sig: int = 3) -> np.ndarray:
    """Beschriftung der Kacheln mit fester Anzahl signifikanter Stellen."""
    def fmt(val):
        if np.isnan(val) or val == 0:
            return "0"
        magnitude = int(np.floor(np.log10(abs(val))))
        decimals = max(0, sig - 1 - magnitude)
        return f"{val:.{decimals}f}"
    return np.vectorize(fmt)(pivot.values)


def erloes_pivot(df_jahr: pd.DataFrame) -> pd.DataFrame:
    return (df_jahr.pivot(index="tilt", columns="azimuth", values="erloes_eur")
                   .sort_index(ascending=False))


def heatmaps_je_jahr(df_jahre: pd.DataFrame) -> None:
    """
    Erlös-Heatmap über das volle Tilt/Azimuth-Raster, je Preisjahr eine Grafik.

    Aufbau bewusst identisch zu 01_heatmap_jahresertrag.png aus
    scripts/04_visualize.py: gleiche Größe, gleiche Farbskala, drei Stellen je
    Kachel, Optimum blau umrandet, Farbleiste rechts. Nur der Kachelwert ist
    ein anderer — statt des Jahresertrags in kWh/kWp steht dort der
    stundenscharf gerechnete Erlös in EUR/kWp: Summe über alle Stunden aus
    Leistung der Stunde mal Spotpreis derselben Stunde.
    """
    for jahr in sorted(df_jahre["jahr"].unique()):
        df_jahr = df_jahre[df_jahre["jahr"] == jahr]
        pivot = erloes_pivot(df_jahr)
        if pivot.shape[0] < 2:
            print(f"  Übersprungen (05, {jahr}): nur ein Neigungswinkel im Datensatz")
            continue

        max_val = pivot.max().max()
        max_pos = np.unravel_index(np.nanargmax(pivot.values), pivot.shape)
        opt_tilt = pivot.index[max_pos[0]]
        opt_azi  = pivot.columns[max_pos[1]]
        basispreis = df_jahr["basispreis_eur_mwh"].mean()

        fig, ax = plt.subplots(figsize=(15, 7))
        sns.heatmap(
            pivot,
            ax=ax,
            cmap="RdYlGn",
            vmin=pivot.min().min(), vmax=max_val,
            cbar=True,
            cbar_kws={"label": "EUR/kWp"},
            annot=sigfig_annot(pivot),
            fmt="",
            annot_kws={"size": 5, "weight": "normal"},
            linewidths=0.2,
            linecolor="white",
        )
        ax.add_patch(plt.Rectangle(
            (max_pos[1], max_pos[0]), 1, 1,
            fill=False, edgecolor="blue", lw=2.5,
            label=f"Optimum: {max_val:.1f} EUR/kWp"))

        ax.set_title(f"Einspeiseerlös — Tilt/Azimuth-Raster\n"
                     f"Standort: {config.STANDORT_ANZEIGE}  |  "
                     f"Preisjahr: {jahr}  |  Basispreis: {basispreis:.1f} EUR/MWh\n"
                     f"Optimum: Tilt={opt_tilt}°, Azimuth={opt_azi}°, "
                     f"Erlös={max_val:.1f} EUR/kWp", fontsize=11)
        ax.set_xlabel("Azimuth (°)  [−180/+180=Nord, −90=Ost, 0=Süd, +90=West]",
                      fontsize=9)
        ax.set_ylabel("Neigungswinkel / Tilt (°)", fontsize=9)
        ax.legend(loc="upper right", fontsize=9)

        plt.tight_layout()
        plot_speichern(fig, f"05_erloes_heatmap_{jahr}.png")


def heatmap_gitter_jahre(df_jahre: pd.DataFrame) -> None:
    """Alle Preisjahre untereinander, gemeinsame Farbskala für den Vergleich."""
    jahre = sorted(df_jahre["jahr"].unique())
    pivots = {j: erloes_pivot(df_jahre[df_jahre["jahr"] == j]) for j in jahre}
    if any(p.shape[0] < 2 for p in pivots.values()):
        print("  Übersprungen (06): nur ein Neigungswinkel im Datensatz")
        return

    vmin = min(p.min().min() for p in pivots.values())
    vmax = max(p.max().max() for p in pivots.values())

    fig, achsen = plt.subplots(len(jahre), 1, figsize=(15, 4.6 * len(jahre)))
    achsen = np.atleast_1d(achsen)
    for ax, jahr in zip(achsen, jahre):
        pivot = pivots[jahr]
        max_pos = np.unravel_index(np.nanargmax(pivot.values), pivot.shape)
        sns.heatmap(pivot, ax=ax, cmap="RdYlGn", vmin=vmin, vmax=vmax,
                    cbar=True, linewidths=0.15, linecolor="white")
        ax.add_patch(plt.Rectangle((max_pos[1], max_pos[0]), 1, 1,
                     fill=False, edgecolor="blue", lw=2.0))
        ax.set_title(f"{jahr} — Optimum Tilt {pivot.index[max_pos[0]]}°, "
                     f"Azimuth {pivot.columns[max_pos[1]]:+d}° "
                     f"= {pivot.values[max_pos]:.1f} EUR/kWp", fontsize=10)
        ax.set_xlabel("")
        ax.set_ylabel("Tilt (°)", fontsize=9)
    achsen[-1].set_xlabel("Azimuth (°)  [−90 = Ost, 0 = Süd, +90 = West]", fontsize=9)

    fig.suptitle(f"Einspeiseerlös am Spotmarkt je Preisjahr (EUR/kWp und Jahr) — "
                 f"gemeinsame Farbskala\n"
                 f"{config.NAME}  |  {config.STANDORT_ANZEIGE}  |  "
                 f"stundenscharf gerechnet", fontsize=12)
    plt.tight_layout()
    plot_speichern(fig, "06_erloes_heatmap_jahre_gitter.png")


# =============================================================================
# Plot 3: Vertikal bifazial
# =============================================================================

def plot_vertikal_bifazial(df: pd.DataFrame, f_rueck: float) -> None:
    vert = df[df["tilt"] == 90]
    if vert.empty:
        print("  Übersprungen (03): keine Werte für Tilt 90° vorhanden")
        return
    erloes = vert.set_index("azimuth")["erloes_eur"].sort_index()
    ertrag = vert.set_index("azimuth")["e_kwh"].sort_index()

    schritt = int(min(np.diff(sorted(erloes.index))))
    beta      = list(range(-90, 91, schritt))
    azi_front = [b - 90 for b in beta]
    azi_back  = [gegen_azimuth(a) for a in azi_front]

    e_vorne  = np.array([erloes.loc[a] + f_rueck * erloes.loc[r]
                         for a, r in zip(azi_front, azi_back)])
    e_hinten = np.array([erloes.loc[r] + f_rueck * erloes.loc[a]
                         for a, r in zip(azi_front, azi_back)])
    e_mono   = np.array([erloes.loc[b] for b in beta])

    sued = df[df["azimuth"] == 0]
    ref  = sued.loc[sued["erloes_eur"].idxmax()]
    ref_wert, ref_tilt = float(ref["erloes_eur"]), int(ref["tilt"])

    i_max = int(np.argmax(e_vorne))
    fig, ax = plt.subplots(figsize=(13, 7))
    ax.plot(beta, e_vorne, color=FARBE_VORNE, lw=2.4, marker="o", ms=4,
            label=f"Bifazial — Vorderseite (100 %) auf der Ost-Seite der Ebene "
                  f"(bei 0°: Ost), Rückseite ({f_rueck:.0%}) gegenüber")
    ax.plot(beta, e_hinten, color=FARBE_HINTEN, lw=2.4, marker="s", ms=4,
            label="Bifazial — Modul um 180° gedreht (bei 0°: Vorderseite West)")
    ax.plot(beta, e_mono, color=FARBE_MONO, lw=1.4, ls=":",
            label="Monofazial vertikal (Azimuth der Vorderseite, 0 = Süd)")

    ax.axhline(ref_wert, color=FARBE_REF, lw=2.0, ls="--")
    ax.text(88, ref_wert, f"  Referenz aus dem Datensatz: beste Süd-Ausrichtung "
                          f"(Tilt {ref_tilt}°, Azimuth 0°) = {ref_wert:.2f} EUR/kWp",
            color=FARBE_REF, fontsize=8.5, va="bottom", ha="right")

    ax.plot([beta[i_max]], [e_vorne[i_max]], marker="*", ms=16,
            color=FARBE_VORNE, zorder=5)
    nach_rechts = beta[i_max] < 0
    ax.annotate(f"Optimum vertikal bifazial\nAusrichtung {beta[i_max]:+d}° · "
                f"{e_vorne[i_max]:.2f} EUR/kWp · "
                f"{e_vorne[i_max] / ref_wert:.0%} der Süd-Referenz",
                xy=(beta[i_max], e_vorne[i_max]),
                xytext=(beta[i_max] + (5 if nach_rechts else -5),
                        e_vorne[i_max] - 0.16 * ref_wert),
                ha="left" if nach_rechts else "right",
                fontsize=8.5, color=FARBE_VORNE,
                arrowprops=dict(arrowstyle="->", color=FARBE_VORNE, lw=1.0))

    ax2 = ax.twinx()
    ax.set_ylim(0, max(ref_wert, e_vorne.max()) * 1.22)
    ax2.set_ylim(0, ax.get_ylim()[1] / ref_wert * 100)
    ax2.set_ylabel(f"Anteil an der Süd-Referenz ({ref_wert:.2f} EUR/kWp) in %",
                   fontsize=9)

    ax.set_xlim(-92, 92)
    ax.set_xticks(beta)
    ax.set_xticklabels([f"{b:+d}" for b in beta], fontsize=8)
    ax.set_xlabel("Ausrichtung (°)   —   monofazial: Azimuth der Vorderseite "
                  "(0 = Süd)   |   bifazial: 0 = Ost/West-Ebene")
    ax.set_ylabel("Einspeiseerlös (EUR/kWp und Jahr)")
    ax.set_title(f"Vertikale Module (Tilt 90°) — Einspeiseerlös am Spotmarkt\n"
                 f"{kopfzeile(df)}  |  Rückseiten-Wirkungsgrad {f_rueck:.0%}",
                 fontsize=11)
    ax.legend(fontsize=8.5, loc="lower center", framealpha=0.92)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plot_speichern(fig, "03_vertikal_bifazial_erloes.png")


# =============================================================================
# Plot 4: Variantenvergleich
# =============================================================================

def varianten_sammeln(df: pd.DataFrame, f_rueck: float) -> pd.DataFrame:
    """Ausgewählte Anlagenvarianten mit Ertrag, Erlös und Capture-Faktor."""
    idx = df.set_index(["tilt", "azimuth"])
    sued = df[df["azimuth"] == 0]
    ref_tilt = int(sued.loc[sued["erloes_eur"].idxmax(), "tilt"])

    def hole(tilt, azi):
        return idx.loc[(tilt, azi)]

    varianten = []

    z = hole(ref_tilt, 0)
    varianten.append((f"Süd {ref_tilt}° (Optimum)", z["e_kwh"], z["erloes_eur"]))

    if (90, -90) in idx.index and (90, 90) in idx.index:
        o, w = hole(90, -90), hole(90, 90)
        varianten.append(("Vertikal bifazial Ost/West",
                          o["e_kwh"] + f_rueck * w["e_kwh"],
                          o["erloes_eur"] + f_rueck * w["erloes_eur"]))
    if (90, 0) in idx.index and (90, 180) in idx.index:
        s, n = hole(90, 0), hole(90, 180)
        varianten.append(("Vertikal bifazial Süd/Nord",
                          s["e_kwh"] + f_rueck * n["e_kwh"],
                          s["erloes_eur"] + f_rueck * n["erloes_eur"]))
        varianten.append(("Vertikal monofazial Süd", s["e_kwh"], s["erloes_eur"]))

    if (30, -90) in idx.index and (30, 90) in idx.index:
        o, w = hole(30, -90), hole(30, 90)
        varianten.append(("Ost/West-Dach 30° (2 Flächen)",
                          (o["e_kwh"] + w["e_kwh"]) / 2,
                          (o["erloes_eur"] + w["erloes_eur"]) / 2))

    tabelle = pd.DataFrame(varianten, columns=["variante", "e_kwh", "erloes_eur"])
    basis = df["basispreis_eur_mwh"].mean()
    tabelle["erloespreis"] = tabelle["erloes_eur"] / tabelle["e_kwh"] * 1000
    tabelle["faktor"]        = tabelle["erloespreis"] / basis
    tabelle["marktabschlag"] = basis / tabelle["erloespreis"]
    return tabelle


def plot_varianten(df: pd.DataFrame, f_rueck: float) -> None:
    tabelle = varianten_sammeln(df, f_rueck)
    if tabelle.empty:
        return

    fig, achsen = plt.subplots(1, 3, figsize=(16, 6))
    positionen = np.arange(len(tabelle))
    farben = ["#27ae60", "#e74c3c", "#3498db", "#7f8c8d", "#f39c12"][:len(tabelle)]

    for ax, spalte, titel, einheit, fmt in [
        (achsen[0], "e_kwh",       "Jahresertrag",        "kWh/kWp",     "{:.0f}"),
        (achsen[1], "erloes_eur",  "Einspeiseerlös",      "EUR/kWp · a", "{:.2f}"),
        (achsen[2], "marktabschlag", "Marktabschlag",     "× Erlöspreis", "{:.2f}"),
    ]:
        balken = ax.bar(positionen, tabelle[spalte], color=farben, width=0.65)
        ax.bar_label(balken, labels=[fmt.format(v) for v in tabelle[spalte]],
                     fontsize=8, padding=2)
        ax.set_xticks(positionen)
        ax.set_xticklabels(tabelle["variante"], rotation=25, ha="right", fontsize=8)
        ax.set_title(titel, fontsize=11)
        ax.set_ylabel(einheit, fontsize=9)
        ax.grid(True, axis="y", alpha=0.3)
        ax.set_axisbelow(True)

    achsen[2].axhline(1.0, color="black", lw=1.0, ls="--")
    fig.suptitle(f"Variantenvergleich — Ertrag, Erlös und Marktabschlag  |  "
                 f"Rückseiten-Wirkungsgrad {f_rueck:.0%}\n{kopfzeile(df)}",
                 fontsize=11)
    plt.tight_layout()
    plot_speichern(fig, "04_varianten_vergleich.png")

    pfad = os.path.join(WIRTSCHAFT_DIR, "varianten_vergleich.csv")
    tabelle.to_csv(pfad, index=False)
    print(f"  Gespeichert: {pfad}")


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rueckseite", type=float, default=0.90,
                        help="Wirkungsgrad der Rückseite (Standard: 0.90)")
    args = parser.parse_args()

    pfad = os.path.join(WIRTSCHAFT_DIR, "erloese_mittel.csv")
    if not os.path.exists(pfad):
        sys.exit(f"Fehlt: {pfad} — bitte zuerst "
                 f"wirtschaftlichkeit/12_analyze_erloese.py ausführen.")
    df = pd.read_csv(pfad)
    print(f"Erlösdaten: {pfad}  ({len(df)} Kombinationen)")

    heatmap(df, "erloes_eur",
            "Einspeiseerlös am Spotmarkt (EUR/kWp und Jahr)",
            "RdYlGn", ".1f", "01_erloes_heatmap.png")
    heatmap(df, "marktabschlag",
            "Marktabschlag (Basispreis / Erlöspreis) — je kleiner, desto besser "
            "trifft die Anlage die teuren Stunden",
            "RdYlGn_r", ".2f", "02_marktabschlag.png")
    plot_vertikal_bifazial(df, args.rueckseite)
    plot_varianten(df, args.rueckseite)

    pfad_jahre = os.path.join(WIRTSCHAFT_DIR, "erloese_je_jahr.csv")
    if os.path.exists(pfad_jahre):
        df_jahre = pd.read_csv(pfad_jahre)
        heatmaps_je_jahr(df_jahre)
        heatmap_gitter_jahre(df_jahre)


if __name__ == "__main__":
    main()
