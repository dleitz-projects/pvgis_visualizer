"""
04_visualize.py
===============
Visualisierungen aus den PVcalc-Pivot-CSVs (Phase 1).

Erstellt folgende Plots und speichert sie unter output/plots/:
  1. Heatmap Jahresertrag (E_y)
  2. Heatmap für jeden Monat (E_m01 bis E_m12) — einzeln + Übersichtsgitter
  3. Saisonvergleich zweier Monate nebeneinander (Standard: Januar vs. Juni)
  4. Differenz-Heatmap zwischen zwei Monaten

Aufruf:
  python scripts/04_visualize.py --csv data/processed/pvcalc_52.772_9.825_2023_..._pivot_E_y.csv
  python scripts/04_visualize.py --ordner data/processed/
  python scripts/04_visualize.py --ordner data/processed/ --monat_a 1 --monat_b 7
"""

import argparse
import os
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import numpy as np
import pandas as pd
import seaborn as sns

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# =============================================================================
# Konstanten
# =============================================================================

MONATSNAMEN = [
    "Januar", "Februar", "März", "April", "Mai", "Juni",
    "Juli", "August", "September", "Oktober", "November", "Dezember",
]

MONATSNAMEN_KURZ = [
    "Jan", "Feb", "Mär", "Apr", "Mai", "Jun",
    "Jul", "Aug", "Sep", "Okt", "Nov", "Dez",
]

# Einheitliches Farbschema: dunkel rot → orange → hellgelb → grün
CMAP_ERTRAG  = "RdYlGn"
CMAP_DIFF    = "RdYlGn"

# Annotierungen: Wert in jeder Zelle anzeigen
ANNOT_KWS    = {"size": 5, "weight": "normal"}  # klein wegen 19×37 Zellen


def sigfig_annot(pivot: pd.DataFrame, sig: int = 3) -> np.ndarray:
    """
    Erzeugt ein String-Array mit 3 signifikanten Stellen pro Zelle.
    Beispiele: 987.6 → "988", 88.46 → "88.5", 7.456 → "7.46"
    Bei fmt="" in sns.heatmap direkt als annot übergeben.
    """
    def fmt(val):
        if np.isnan(val) or val == 0:
            return "0"
        magnitude = int(np.floor(np.log10(abs(val))))
        decimals = max(0, sig - 1 - magnitude)
        return f"{val:.{decimals}f}"
    return np.vectorize(fmt)(pivot.values)

plt.rcParams.update({
    "figure.dpi":    150,
    "font.size":     10,
    "axes.titlesize": 12,
    "axes.labelsize": 10,
    "font.family":   "sans-serif",
})


# =============================================================================
# Hilfsfunktionen
# =============================================================================

def pivot_laden(pfad: str) -> pd.DataFrame:
    """Lädt eine Pivot-CSV: Index=Tilt, Spalten=Azimuth."""
    df = pd.read_csv(pfad, index_col=0)
    df.index   = df.index.astype(int)
    df.columns = df.columns.astype(int)
    return df.sort_index(ascending=False)


def pivots_aus_ordner_laden(ordner: str) -> dict:
    """
    Sucht im Ordner nach dem neuesten PVcalc-Datensatz und lädt alle Pivot-CSVs.
    Gibt Dictionary zurück: {"E_y": df, "E_m01": df, ..., "E_m12": df}
    """
    ordner_pfad = Path(ordner)

    # Neueste Pivot-Datei für E_y finden (nach Dateiname sortiert)
    kandidaten = sorted(ordner_pfad.glob("*pivot_E_y.csv"), reverse=True)
    if not kandidaten:
        print(f"  FEHLER: Keine 'pivot_E_y.csv' in '{ordner}' gefunden.")
        print("  Bitte zuerst 01_fetch_pvcalc.py ausführen.")
        sys.exit(1)

    basis = str(kandidaten[0]).replace("_pivot_E_y.csv", "")
    print(f"  Verwende Datensatz: {Path(basis).name}")

    pivots = {"E_y": pivot_laden(f"{basis}_pivot_E_y.csv")}
    for m in range(1, 13):
        pfad = f"{basis}_pivot_monat_{m:02d}.csv"
        if os.path.exists(pfad):
            pivots[f"E_m{m:02d}"] = pivot_laden(pfad)
        else:
            print(f"  Warnung: {pfad} nicht gefunden, übersprungen.")

    print(f"  {len(pivots)} Pivot-Tabellen geladen.\n")
    return pivots


def plot_speichern(fig, dateiname: str) -> None:
    """Speichert Figure unter output/plots/ und schließt sie."""
    os.makedirs(config.PLOTS_DIR, exist_ok=True)
    pfad = os.path.join(config.PLOTS_DIR, dateiname)
    fig.savefig(pfad, bbox_inches="tight", dpi=150)
    plt.close(fig)
    print(f"  Gespeichert: {pfad}")


def heatmap_achsen_formatieren(ax) -> None:
    """Formatiert Achsen und Beschriftungen einer Heatmap-Achse einheitlich."""
    ax.set_xlabel("Azimuth (°)  [−180/+180=Nord, −90=Ost, 0=Süd, +90=West]", fontsize=9)
    ax.set_ylabel("Neigungswinkel / Tilt (°)", fontsize=9)


# =============================================================================
# Plot 1: Jahresertrag-Heatmap
# =============================================================================

def plot_jahresertrag(pivot: pd.DataFrame, standort_info: str = "") -> None:
    """Heatmap des Jahresertrags über das gesamte Tilt/Azimuth-Raster."""
    fig, ax = plt.subplots(figsize=(15, 7))

    max_val = pivot.max().max()
    max_pos = np.unravel_index(np.nanargmax(pivot.values), pivot.shape)
    opt_tilt = pivot.index[max_pos[0]]
    opt_azi  = pivot.columns[max_pos[1]]

    sns.heatmap(
        pivot,
        ax=ax,
        cmap=CMAP_ERTRAG,
        vmin=pivot.min().min(), vmax=max_val,
        cbar=False,
        annot=sigfig_annot(pivot),
        fmt="",
        annot_kws=ANNOT_KWS,
        linewidths=0.2,
        linecolor="white",
    )
    ax.add_patch(plt.Rectangle(
        (max_pos[1], max_pos[0]), 1, 1,
        fill=False, edgecolor="blue", lw=2.5, label=f"Optimum: {max_val:.0f} kWh/kWp"
    ))

    titel = "Jahresertrag — Tilt/Azimuth-Raster"
    if standort_info:
        titel += f"\n{standort_info}"
    ax.set_title(titel + f"\nOptimum: Tilt={opt_tilt}°, Azimuth={opt_azi}°, E_y={max_val:.0f} kWh/kWp",
                 fontsize=11)
    heatmap_achsen_formatieren(ax)
    ax.legend(loc="upper right", fontsize=9)

    plt.tight_layout()
    plot_speichern(fig, "01_heatmap_jahresertrag.png")


# =============================================================================
# Plot 2: Monats-Heatmaps (Übersichtsgitter 4×3)
# =============================================================================

def plot_alle_monate_gitter(pivots: dict) -> None:
    """
    Zeigt alle 12 Monatserträge in einem 4×3-Gitter.
    Gemeinsame Farbskala über alle Monate.
    """
    fig, axes = plt.subplots(3, 4, figsize=(22, 13), sharex=True, sharey=True)
    fig.suptitle("Monatserträge (kWh/kWp) — Tilt/Azimuth-Raster\n"
                 f"{config.NAME}  |  {config.LAT}°N, {config.LON}°E  |  Jahr: {config.PVCALC_YEAR}",
                 fontsize=13, y=1.01)

    for idx, ax in enumerate(axes.flat):
        m     = idx + 1
        pivot = pivots.get(f"E_m{m:02d}")
        if pivot is None:
            ax.set_visible(False)
            continue

        max_val = pivot.max().max()
        max_pos = np.unravel_index(np.nanargmax(pivot.values), pivot.shape)
        opt_tilt = pivot.index[max_pos[0]]
        opt_azi  = pivot.columns[max_pos[1]]

        sns.heatmap(
            pivot, ax=ax,
            cmap=CMAP_ERTRAG,
            vmin=pivot.min().min(), vmax=max_val,
            cbar=False,
            linewidths=0.1,
            linecolor="white",
            annot=sigfig_annot(pivot),
            fmt="",
            annot_kws={"size": 4},
        )
        ax.add_patch(plt.Rectangle(
            (max_pos[1], max_pos[0]), 1, 1,
            fill=False, edgecolor="blue", lw=1.5
        ))
        ax.set_title(f"{MONATSNAMEN[idx]}  |  Max: {max_val:.0f} kWh/kWp\n"
                     f"Tilt={opt_tilt}°, Azi={opt_azi}°", fontsize=8)
        ax.set_xlabel("Azimuth (°)" if idx >= 8 else "", fontsize=7)
        ax.set_ylabel("Tilt (°)" if idx % 4 == 0 else "", fontsize=7)
        ax.tick_params(labelsize=7)

    plt.tight_layout()
    plot_speichern(fig, "02_heatmap_alle_monate_gitter.png")


# =============================================================================
# Plot 3: Saisonvergleich zweier Monate nebeneinander
# =============================================================================

def plot_saisonvergleich(pivots: dict, monat_a: int, monat_b: int) -> None:
    """Zwei Monatsertrags-Heatmaps nebeneinander mit einheitlicher Farbskala."""
    key_a = f"E_m{monat_a:02d}"
    key_b = f"E_m{monat_b:02d}"

    if key_a not in pivots or key_b not in pivots:
        print(f"  Warnung: Daten für Monat {monat_a} oder {monat_b} fehlen, übersprungen.")
        return

    pivot_a = pivots[key_a]
    pivot_b = pivots[key_b]

    fig, axes = plt.subplots(1, 2, figsize=(22, 7))

    for ax, pivot, monat in zip(axes, [pivot_a, pivot_b], [monat_a, monat_b]):
        max_val = pivot.max().max()
        max_pos = np.unravel_index(np.nanargmax(pivot.values), pivot.shape)
        opt_tilt = pivot.index[max_pos[0]]
        opt_azi  = pivot.columns[max_pos[1]]

        sns.heatmap(
            pivot, ax=ax,
            cmap=CMAP_ERTRAG,
            vmin=pivot.min().min(), vmax=max_val,
            cbar=False,
            linewidths=0.2,
            linecolor="white",
            annot=sigfig_annot(pivot),
            fmt="",
            annot_kws=ANNOT_KWS,
        )
        ax.add_patch(plt.Rectangle(
            (max_pos[1], max_pos[0]), 1, 1,
            fill=False, edgecolor="blue", lw=2.5
        ))
        ax.set_title(f"{MONATSNAMEN[monat - 1]}  |  Max: {max_val:.0f} kWh/kWp\n"
                     f"Optimum: Tilt={opt_tilt}°, Azimuth={opt_azi}°", fontsize=11)
        heatmap_achsen_formatieren(ax)

    fig.suptitle(
        f"Saisonvergleich: {MONATSNAMEN[monat_a-1]} vs. {MONATSNAMEN[monat_b-1]}\n"
        f"Standort: {config.LAT}°N, {config.LON}°E  |  Jahr: {config.PVCALC_YEAR}",
        fontsize=13,
    )
    plt.tight_layout()
    datei = f"03_saisonvergleich_{MONATSNAMEN_KURZ[monat_a-1].lower()}_{MONATSNAMEN_KURZ[monat_b-1].lower()}.png"
    plot_speichern(fig, datei)


# =============================================================================
# Plot 4: Differenz-Heatmap zwischen zwei Monaten
# =============================================================================

def plot_faktor(pivots: dict, monat_a: int, monat_b: int) -> None:
    """
    Zeigt den Faktor (Monat A / Monat B) als Heatmap.
    Wert > 1: Monat A hat höheren Ertrag, Wert < 1: Monat B hat höheren Ertrag.
    Farbskala normiert von 0 bis max(Faktor).
    """
    key_a = f"E_m{monat_a:02d}"
    key_b = f"E_m{monat_b:02d}"

    if key_a not in pivots or key_b not in pivots:
        return

    # Division mit Schutz gegen Nullwerte
    faktor = pivots[key_a] / pivots[key_b].replace(0, np.nan)
    max_val = faktor.max().max()
    max_pos = np.unravel_index(np.nanargmax(faktor.values), faktor.shape)

    fig, ax = plt.subplots(figsize=(15, 7))
    sns.heatmap(
        faktor, ax=ax,
        cmap=CMAP_DIFF,
        center=1,
        vmin=faktor.min().min(), vmax=max_val,
        cbar=False,
        linewidths=0.2,
        linecolor="white",
        annot=sigfig_annot(faktor),
        fmt="",
        annot_kws=ANNOT_KWS,
    )
    ax.add_patch(plt.Rectangle(
        (max_pos[1], max_pos[0]), 1, 1,
        fill=False, edgecolor="blue", lw=2.5
    ))
    ax.set_title(
        f"Faktor {MONATSNAMEN[monat_a-1]} / {MONATSNAMEN[monat_b-1]}\n"
        f"Grün (>1) = {MONATSNAMEN[monat_a-1]} ertragreicher  |  "
        f"Rot (<1) = {MONATSNAMEN[monat_b-1]} ertragreicher",
        fontsize=11,
    )
    heatmap_achsen_formatieren(ax)
    plt.tight_layout()
    datei = f"04_faktor_{MONATSNAMEN_KURZ[monat_a-1].lower()}_{MONATSNAMEN_KURZ[monat_b-1].lower()}.png"
    plot_speichern(fig, datei)


# =============================================================================
# Plot 5: Einzelner Monat als große Heatmap
# =============================================================================

def plot_monat_einzeln(pivots: dict, monat: int) -> None:
    """Einzelne große Heatmap für einen bestimmten Monat."""
    key = f"E_m{monat:02d}"
    if key not in pivots:
        print(f"  Warnung: Daten für Monat {monat} fehlen.")
        return

    pivot   = pivots[key]
    max_val = pivot.max().max()
    max_pos = np.unravel_index(np.nanargmax(pivot.values), pivot.shape)
    opt_tilt = pivot.index[max_pos[0]]
    opt_azi  = pivot.columns[max_pos[1]]

    fig, ax = plt.subplots(figsize=(15, 7))
    sns.heatmap(
        pivot, ax=ax,
        cmap=CMAP_ERTRAG,
        vmin=pivot.min().min(), vmax=max_val,
        cbar=False,
        linewidths=0.2,
        linecolor="white",
        annot=sigfig_annot(pivot),
        fmt="",
        annot_kws=ANNOT_KWS,
    )
    ax.add_patch(plt.Rectangle(
        (max_pos[1], max_pos[0]), 1, 1,
        fill=False, edgecolor="blue", lw=2.5
    ))
    ax.set_title(
        f"Monatsertrag {MONATSNAMEN[monat-1]}\n"
        f"Optimum: Tilt={opt_tilt}°, Azimuth={opt_azi}°, E_m={max_val:.0f} kWh/kWp",
        fontsize=11,
    )
    heatmap_achsen_formatieren(ax)
    plt.tight_layout()
    datei = f"05_monat_{monat:02d}_{MONATSNAMEN_KURZ[monat-1].lower()}.png"
    plot_speichern(fig, datei)


# =============================================================================
# Hauptprogramm
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="PVGIS PVcalc-Visualisierungen erstellen"
    )
    parser.add_argument(
        "--ordner",
        default=config.DATA_PROCESSED_DIR,
        help=f"Ordner mit Pivot-CSVs (Standard: {config.DATA_PROCESSED_DIR})",
    )
    parser.add_argument(
        "--monat_a",
        type=int,
        default=1,
        help="Erster Monat für Saisonvergleich (1–12, Standard: 1 = Januar)",
    )
    parser.add_argument(
        "--monat_b",
        type=int,
        default=6,
        help="Zweiter Monat für Saisonvergleich (1–12, Standard: 6 = Juni)",
    )
    parser.add_argument(
        "--monat_einzeln",
        type=int,
        default=None,
        help="Einzelnen Monat als große Heatmap darstellen (1–12)",
    )
    args = parser.parse_args()

    standort_info = f"{config.NAME}  |  {config.LAT}°N, {config.LON}°E  |  Jahr: {config.PVCALC_YEAR}"
    print(f"\nPVGIS Visualizer — {standort_info}")
    print(f"Lade Pivot-Tabellen aus: {args.ordner}\n")

    pivots = pivots_aus_ordner_laden(args.ordner)

    print("Erstelle Plots ...")

    # 1. Jahresertrag-Heatmap
    if "E_y" in pivots:
        print("  [1/4] Jahresertrag-Heatmap ...")
        plot_jahresertrag(pivots["E_y"], standort_info)

    # 2. Alle Monate im Gitter
    print("  [2/4] Monats-Übersichtsgitter (4×3) ...")
    plot_alle_monate_gitter(pivots)

    # 3. Saisonvergleich
    print(f"  [3/4] Saisonvergleich {MONATSNAMEN[args.monat_a-1]} vs. {MONATSNAMEN[args.monat_b-1]} ...")
    plot_saisonvergleich(pivots, args.monat_a, args.monat_b)

    # 4. Faktor-Heatmap
    print(f"  [4/4] Faktor-Heatmap {MONATSNAMEN[args.monat_a-1]} / {MONATSNAMEN[args.monat_b-1]} ...")
    plot_faktor(pivots, args.monat_a, args.monat_b)

    # Optional: Einzelmonat
    if args.monat_einzeln:
        print(f"  [+]   Einzelmonat {MONATSNAMEN[args.monat_einzeln-1]} ...")
        plot_monat_einzeln(pivots, args.monat_einzeln)

    print(f"\n{'='*60}")
    print(f"Alle Plots gespeichert unter: {config.PLOTS_DIR}/")
    print(f"\nTipp: Saisonvergleich anpassen z. B. für Dez vs. Jun:")
    print(f"  python scripts/04_visualize.py --monat_a 12 --monat_b 6")
    print(f"  python scripts/04_visualize.py --monat_einzeln 3")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
