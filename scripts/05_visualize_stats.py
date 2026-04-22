"""
05_visualize_stats.py
=====================
Visualisierungen auf Basis der 16-Jahres-Statistiken (seriescalc).

Plots werden gespeichert unter: output/plots/{NAME}/stats/

Gruppe 1 — Jahresebene (Heatmaps):
  01_jahresertrag_mean.png         ← E_y Mittelwert über 16 Jahre
  02_jahresertrag_std.png          ← E_y Standardabweichung
  03_jahresertrag_vk.png           ← Variationskoeffizient (std/mean in %)

Gruppe 2 — Tagesverläufe:
  04_tagesverlauf_optimal.png      ← mean ± std für optimale Kombination (Jahresschnitt)
  05_tagesverlauf_jan_jun.png      ← Jan vs. Jun für 4 Orientierungen
  06_tagesverlauf_12monate.png     ← Alle 12 Monate für optimale Kombination

Gruppe 3 — Monatserträge:
  07_monatsertraege_fehlerbalken.png ← E_m mean ± std für ausgewählte Kombinationen

Aufruf:
  python scripts/05_visualize_stats.py
  python scripts/05_visualize_stats.py --tilt 35 --azi 0
"""

import argparse
import os
import sys
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
import seaborn as sns

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# =============================================================================
# Konstanten
# =============================================================================

STATS_DIR    = os.path.join(config.DATA_PROCESSED_DIR, "stats")
OUTPUT_DIR   = os.path.join(config.PLOTS_DIR, "stats")

MONATSNAMEN  = ["Januar","Februar","März","April","Mai","Juni",
                "Juli","August","September","Oktober","November","Dezember"]
MONATSNAMEN_KURZ = ["Jan","Feb","Mär","Apr","Mai","Jun",
                    "Jul","Aug","Sep","Okt","Nov","Dez"]

CMAP_ERTRAG  = "RdYlGn"
CMAP_STD     = "YlOrRd"     # Für std: niedrig=gut (gelb), hoch=schlecht (rot)
CMAP_VK      = "RdYlGn_r"   # Für VK: niedrig=gut (grün), hoch=schlecht (rot)

# Farben für Orientierungsvergleiche
FARBEN_LIST = ["#e74c3c", "#3498db", "#2ecc71", "#9b59b6"]  # Süd, Ost, West, Flach

plt.rcParams.update({
    "figure.dpi":     150,
    "font.size":      10,
    "axes.titlesize": 12,
    "axes.labelsize": 10,
    "font.family":    "sans-serif",
})

ANNOT_KWS = {"size": 5, "weight": "normal"}


# =============================================================================
# Hilfsfunktionen
# =============================================================================

def plot_speichern(fig, dateiname: str) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    pfad = os.path.join(OUTPUT_DIR, dateiname)
    fig.savefig(pfad, bbox_inches="tight", dpi=150)
    plt.close(fig)
    print(f"  Gespeichert: {pfad}")


def pivot_erstellen(df: pd.DataFrame, wert: str) -> pd.DataFrame:
    return (df.pivot(index="tilt", columns="azimuth", values=wert)
              .sort_index(ascending=False))


def sigfig_annot(pivot: pd.DataFrame, sig: int = 3) -> np.ndarray:
    def fmt(val):
        if np.isnan(val) or val == 0:
            return "0"
        magnitude = int(np.floor(np.log10(abs(val))))
        decimals = max(0, sig - 1 - magnitude)
        return f"{val:.{decimals}f}"
    return np.vectorize(fmt)(pivot.values)


def heatmap_achsen(ax) -> None:
    ax.set_xlabel("Azimuth (°)  [−180/+180=Nord, −90=Ost, 0=Süd, +90=West]", fontsize=9)
    ax.set_ylabel("Neigungswinkel / Tilt (°)", fontsize=9)


def optimum_finden(df: pd.DataFrame, wert: str = "E_y_mean"):
    """Gibt (tilt, azimuth, wert) der Zelle mit dem höchsten Wert zurück."""
    idx = df[wert].idxmax()
    return df.loc[idx, "tilt"], df.loc[idx, "azimuth"], df.loc[idx, wert]


def standort_info() -> str:
    return f"{config.NAME}  |  {config.LAT}°N, {config.LON}°E  |  2005–2020 (16 Jahre)"


# =============================================================================
# Gruppe 1: Jahres-Heatmaps
# =============================================================================

def plot_jahresertrag_mean(df_jahr: pd.DataFrame) -> None:
    pivot   = pivot_erstellen(df_jahr, "E_y_mean")
    max_val = pivot.max().max()
    max_pos = np.unravel_index(np.nanargmax(pivot.values), pivot.shape)
    opt_tilt = pivot.index[max_pos[0]]
    opt_azi  = pivot.columns[max_pos[1]]

    fig, ax = plt.subplots(figsize=(15, 7))
    sns.heatmap(pivot, ax=ax, cmap=CMAP_ERTRAG,
                vmin=pivot.min().min(), vmax=max_val,
                cbar=False, linewidths=0.2, linecolor="white",
                annot=sigfig_annot(pivot), fmt="", annot_kws=ANNOT_KWS)
    ax.add_patch(plt.Rectangle((max_pos[1], max_pos[0]), 1, 1,
                 fill=False, edgecolor="blue", lw=2.5))
    ax.set_title(f"Mittlerer Jahresertrag (kWh/kWp) — 16-Jahres-Mittel\n"
                 f"{standort_info()}\n"
                 f"Optimum: Tilt={opt_tilt}°, Azimuth={opt_azi}°, "
                 f"E_y={max_val:.0f} kWh/kWp", fontsize=11)
    heatmap_achsen(ax)
    plt.tight_layout()
    plot_speichern(fig, "01_jahresertrag_mean.png")


def plot_jahresertrag_std(df_jahr: pd.DataFrame) -> None:
    pivot   = pivot_erstellen(df_jahr, "E_y_std")
    max_val = pivot.max().max()

    fig, ax = plt.subplots(figsize=(15, 7))
    sns.heatmap(pivot, ax=ax, cmap=CMAP_STD,
                vmin=pivot.min().min(), vmax=max_val,
                cbar=False, linewidths=0.2, linecolor="white",
                annot=sigfig_annot(pivot), fmt="", annot_kws=ANNOT_KWS)
    ax.set_title(f"Standardabweichung Jahresertrag (kWh/kWp)\n"
                 f"{standort_info()}\n"
                 f"Gelb = stabiler Ertrag  |  Rot = hohe Schwankung", fontsize=11)
    heatmap_achsen(ax)
    plt.tight_layout()
    plot_speichern(fig, "02_jahresertrag_std.png")


def plot_variationskoeffizient(df_jahr: pd.DataFrame) -> None:
    df_vk = df_jahr.copy()
    df_vk["vk"] = df_vk["E_y_std"] / df_vk["E_y_mean"] * 100
    pivot   = pivot_erstellen(df_vk, "vk")
    max_val = pivot.max().max()

    fig, ax = plt.subplots(figsize=(15, 7))
    sns.heatmap(pivot, ax=ax, cmap=CMAP_VK,
                vmin=pivot.min().min(), vmax=max_val,
                cbar=False, linewidths=0.2, linecolor="white",
                annot=sigfig_annot(pivot), fmt="", annot_kws=ANNOT_KWS)
    ax.set_title(f"Variationskoeffizient Jahresertrag (std/mean in %)\n"
                 f"{standort_info()}\n"
                 f"Grün = vorhersehbar  |  Rot = hohe interannuelle Variabilität", fontsize=11)
    heatmap_achsen(ax)
    plt.tight_layout()
    plot_speichern(fig, "03_jahresertrag_vk.png")


# =============================================================================
# Gruppe 2: Tagesverläufe
# =============================================================================

def plot_tagesverlauf_optimal(df_gesamt: pd.DataFrame, opt_tilt: int, opt_azi: int) -> None:
    """Mittlerer Tagesverlauf für die optimale Kombination mit ±std Band."""
    sub = df_gesamt[(df_gesamt["tilt"] == opt_tilt) & (df_gesamt["azimuth"] == opt_azi)]

    fig, ax = plt.subplots(figsize=(11, 5))
    ax.plot(sub["stunde"], sub["P_mean"], color="#e74c3c", lw=2, label="Mittelwert")
    ax.fill_between(sub["stunde"],
                    (sub["P_mean"] - sub["P_std"]).clip(lower=0),
                    sub["P_mean"] + sub["P_std"],
                    alpha=0.25, color="#e74c3c", label="±1 Std.abw.")
    ax.set_xlabel("Stunde des Tages")
    ax.set_ylabel("Leistung (W/kWp)")
    ax.set_title(f"Mittlerer Tagesverlauf — Tilt={opt_tilt}°, Azimuth={opt_azi}° (Jahresschnitt)\n"
                 f"{standort_info()}")
    ax.set_xticks(range(0, 24))
    ax.legend()
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plot_speichern(fig, "04_tagesverlauf_optimal.png")


def plot_tagesverlauf_jan_jun(opt_tilt: int, opt_azi: int) -> None:
    """Jan vs. Jun für 4 Orientierungen: Süd-optimal, Ost, West, Flach."""
    orientierungen = {
        f"Süd-optimal (Tilt={opt_tilt}°, Azi={opt_azi}°)": (opt_tilt, opt_azi),
        "Ost (Tilt=35°, Azi=−90°)":                        (35, -90),
        "West (Tilt=35°, Azi=+90°)":                       (35,  90),
        "Flach (Tilt=10°, Azi=0°)":                        (10,   0),
    }

    fig, axes = plt.subplots(1, 2, figsize=(18, 6), sharey=True)

    for ax, monat, monat_name in zip(axes, [1, 6], ["Januar", "Juni"]):
        df_m = pd.read_csv(os.path.join(STATS_DIR, f"tagesverlauf_monat_{monat:02d}.csv"))

        for (name, (tilt, azi)), farbe in zip(orientierungen.items(), FARBEN_LIST):
            sub = df_m[(df_m["tilt"] == tilt) & (df_m["azimuth"] == azi)]
            if sub.empty:
                continue
            ax.plot(sub["stunde"], sub["P_mean"], color=farbe, lw=2, label=name)
            ax.fill_between(sub["stunde"],
                            (sub["P_mean"] - sub["P_std"]).clip(lower=0),
                            sub["P_mean"] + sub["P_std"],
                            alpha=0.15, color=farbe)

        ax.set_title(f"{monat_name}", fontsize=12)
        ax.set_xlabel("Stunde des Tages")
        ax.set_ylabel("Leistung (W/kWp)")
        ax.set_xticks(range(0, 24))
        ax.legend(fontsize=9)
        ax.grid(True, alpha=0.3)

    fig.suptitle(f"Tagesverlauf Januar vs. Juni — 4 Orientierungen\n{standort_info()}",
                 fontsize=12)
    plt.tight_layout()
    plot_speichern(fig, "05_tagesverlauf_jan_jun.png")


def plot_tagesverlauf_12monate(opt_tilt: int, opt_azi: int) -> None:
    """Alle 12 Monate für die optimale Kombination in einem Gitter."""
    fig, axes = plt.subplots(3, 4, figsize=(22, 13), sharex=True, sharey=True)
    fig.suptitle(f"Tagesverläufe — Alle 12 Monate\n"
                 f"Süd-optimal: Tilt={opt_tilt}°, Azimuth={opt_azi}°  |  {standort_info()}",
                 fontsize=12, y=1.01)

    # Gemeinsame Y-Achse: max über alle Monate (P_max wenn verfügbar, sonst mean+std)
    p_max = 0
    for m in range(1, 13):
        df_m = pd.read_csv(os.path.join(STATS_DIR, f"tagesverlauf_monat_{m:02d}.csv"))
        sub  = df_m[(df_m["tilt"] == opt_tilt) & (df_m["azimuth"] == opt_azi)]
        if not sub.empty:
            col = "P_max" if "P_max" in sub.columns else "P_mean"
            p_max = max(p_max, sub[col].max())

    for idx, ax in enumerate(axes.flat):
        m    = idx + 1
        df_m = pd.read_csv(os.path.join(STATS_DIR, f"tagesverlauf_monat_{m:02d}.csv"))
        sub  = df_m[(df_m["tilt"] == opt_tilt) & (df_m["azimuth"] == opt_azi)]

        if sub.empty:
            ax.set_visible(False)
            continue

        # Min/Max gestrichelt (gedämpft)
        if "P_min" in sub.columns and "P_max" in sub.columns:
            ax.plot(sub["stunde"], sub["P_max"], color="#c0392b", lw=0.8,
                    linestyle="--", alpha=0.35)
            ax.plot(sub["stunde"], sub["P_min"], color="#c0392b", lw=0.8,
                    linestyle="--", alpha=0.35)

        ax.plot(sub["stunde"], sub["P_mean"], color="#e74c3c", lw=1.5)
        ax.fill_between(sub["stunde"],
                        (sub["P_mean"] - sub["P_std"]).clip(lower=0),
                        sub["P_mean"] + sub["P_std"],
                        alpha=0.25, color="#e74c3c")
        ax.set_title(MONATSNAMEN[idx], fontsize=10)
        ax.set_ylim(0, p_max * 1.05)
        ax.set_xticks(range(0, 24, 4))
        ax.set_xlabel("Stunde" if idx >= 8 else "", fontsize=8)
        ax.set_ylabel("W/kWp" if idx % 4 == 0 else "", fontsize=8)
        ax.tick_params(labelsize=8)
        ax.grid(True, alpha=0.3)

    plt.tight_layout()
    plot_speichern(fig, "06_tagesverlauf_12monate.png")


# =============================================================================
# Gruppe 3: Monatserträge mit Fehlerbalken
# =============================================================================

def plot_monatsertraege_fehlerbalken(df_monat: pd.DataFrame, opt_tilt: int, opt_azi: int) -> None:
    """E_m mean ± std für 4 Orientierungen als gruppiertes Balkendiagramm."""
    orientierungen = {
        f"Süd-optimal (Tilt={opt_tilt}°, Azi={opt_azi}°)": (opt_tilt, opt_azi),
        "Ost (Tilt=35°, Azi=−90°)":                        (35, -90),
        "West (Tilt=35°, Azi=+90°)":                       (35,  90),
        "Flach (Tilt=10°, Azi=0°)":                        (10,   0),
    }

    x      = np.arange(12)
    breite = 0.2

    fig, ax = plt.subplots(figsize=(14, 6))

    for i, ((name, (tilt, azi)), farbe) in enumerate(zip(orientierungen.items(), FARBEN_LIST)):
        sub = df_monat[(df_monat["tilt"] == tilt) & (df_monat["azimuth"] == azi)]
        if sub.empty:
            continue
        sub = sub.sort_values("monat")
        ax.bar(x + i * breite, sub["E_m_mean"], breite,
               yerr=sub["E_m_std"], capsize=3,
               color=farbe, alpha=0.8, label=name,
               error_kw={"elinewidth": 1, "ecolor": "black"})

    ax.set_xticks(x + breite * 1.5)
    ax.set_xticklabels(MONATSNAMEN_KURZ)
    ax.set_ylabel("Monatsertrag (kWh/kWp)")
    ax.set_title(f"Monatserträge mean ± std — 4 Orientierungen\n{standort_info()}")
    ax.legend(fontsize=9)
    ax.grid(True, axis="y", alpha=0.3)
    plt.tight_layout()
    plot_speichern(fig, "07_monatsertraege_fehlerbalken.png")


# =============================================================================
# Hauptprogramm
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Statistik-Visualisierungen aus seriescalc-Auswertungen"
    )
    parser.add_argument("--tilt", type=int, default=None,
                        help="Tilt der 'Süd-optimal'-Kombination (Standard: aus Daten)")
    parser.add_argument("--azi",  type=int, default=None,
                        help="Azimuth der 'Süd-optimal'-Kombination (Standard: aus Daten)")
    args = parser.parse_args()

    print(f"\nPVGIS Stats Visualizer")
    print(f"Statistiken aus: {STATS_DIR}")
    print(f"Ausgabe:         {OUTPUT_DIR}\n")

    # Daten laden
    df_jahr  = pd.read_csv(os.path.join(STATS_DIR, "jahresertraege.csv"))
    df_monat = pd.read_csv(os.path.join(STATS_DIR, "monatsertraege.csv"))
    df_tages = pd.read_csv(os.path.join(STATS_DIR, "tagesverlauf_gesamt.csv"))

    # Optimum bestimmen
    if args.tilt is not None and args.azi is not None:
        opt_tilt, opt_azi = args.tilt, args.azi
        opt_val = df_jahr[(df_jahr["tilt"] == opt_tilt) &
                          (df_jahr["azimuth"] == opt_azi)]["E_y_mean"].values[0]
    else:
        opt_tilt, opt_azi, opt_val = optimum_finden(df_jahr)

    print(f"Optimum (16-Jahres-Mittel): Tilt={opt_tilt}°, Azimuth={opt_azi}°, "
          f"E_y={opt_val:.0f} kWh/kWp\n")
    print("Erstelle Plots ...")

    # Gruppe 1: Jahres-Heatmaps
    print("  [1/7] Jahresertrag mean ...")
    plot_jahresertrag_mean(df_jahr)

    print("  [2/7] Jahresertrag std ...")
    plot_jahresertrag_std(df_jahr)

    print("  [3/7] Variationskoeffizient ...")
    plot_variationskoeffizient(df_jahr)

    # Gruppe 2: Tagesverläufe
    print("  [4/7] Tagesverlauf optimal (Jahresschnitt) ...")
    plot_tagesverlauf_optimal(df_tages, opt_tilt, opt_azi)

    print("  [5/7] Tagesverlauf Januar vs. Juni ...")
    plot_tagesverlauf_jan_jun(opt_tilt, opt_azi)

    print("  [6/7] Tagesverlauf alle 12 Monate ...")
    plot_tagesverlauf_12monate(opt_tilt, opt_azi)

    # Gruppe 3: Monatserträge
    print("  [7/7] Monatserträge mit Fehlerbalken ...")
    plot_monatsertraege_fehlerbalken(df_monat, opt_tilt, opt_azi)

    print(f"\n{'='*60}")
    print(f"Alle 7 Plots gespeichert unter: {OUTPUT_DIR}/")
    print(f"\nTipp: Andere Kombination als Referenz:")
    print(f"  python scripts/05_visualize_stats.py --tilt 30 --azi -10")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
