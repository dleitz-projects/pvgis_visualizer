"""
06_visualize_objekt.py
======================
Tagesverläufe für eine konkrete PV-Anlage mit mehreren Strings.
Jeder String wird mit seiner tatsächlichen kWp-Leistung skaliert.
Die Tilt/Azimuth-Werte werden automatisch auf das nächste Raster-Gitter gerastet.

Datenquelle: data/processed/{NAME}/stats/tagesverlauf_*.csv (aus 03_analyze.py)
Ausgabe:     output/plots/{NAME}/objekt/{OBJEKT_NAME}/

Plots:
  01_tagesverlauf_gesamt.png   ← Jahresschnitt: je String (mean) + Gesamt (mean ± std)
  02_tagesverlauf_12monate.png ← 12-Monatsgitter: je String + Gesamt (mean ± std)

Aufruf:
  python scripts/06_visualize_objekt.py
"""

import os
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# =============================================================================
# Anlagenkonfiguration (hier anpassen)
# =============================================================================

OBJEKT_NAME = "Objekt_1"

# Strings: name, Modulanzahl, kWp pro Modul, Tilt (°), Azimuth (°)
# Tilt/Azimuth werden auf das nächste verfügbare Rasterpunkt gerastet.
STRINGS = [
    {"name": "String 1", "anzahl":  5, "kwp_modul": 0.400, "tilt": 90, "azimuth":  75},
    {"name": "String 2", "anzahl": 8, "kwp_modul": 0.465, "tilt": 15, "azimuth": -15},
    {"name": "String 3", "anzahl": 12, "kwp_modul": 0.465, "tilt": 45, "azimuth": -15},
    {"name": "String 4", "anzahl":  4, "kwp_modul": 0.465, "tilt": 20, "azimuth": -15},
]

# Farben: je ein Farbton pro String, Rot für Gesamt
FARBEN_STRINGS = ["#3498db", "#2ecc71", "#9b59b6", "#f39c12"]
FARBE_GESAMT   = "#e74c3c"

# =============================================================================
# Pfade
# =============================================================================

STATS_DIR  = os.path.join(config.DATA_PROCESSED_DIR, "stats")
OUTPUT_DIR = os.path.join(config.PLOTS_DIR, "objekt", OBJEKT_NAME)

MONATSNAMEN = ["Januar", "Februar", "März", "April", "Mai", "Juni",
               "Juli", "August", "September", "Oktober", "November", "Dezember"]
MONATSNAMEN_KURZ = ["Jan", "Feb", "Mär", "Apr", "Mai", "Jun",
                    "Jul", "Aug", "Sep", "Okt", "Nov", "Dez"]

plt.rcParams.update({
    "figure.dpi":     150,
    "font.size":      10,
    "axes.titlesize": 12,
    "axes.labelsize": 10,
    "font.family":    "sans-serif",
})


# =============================================================================
# Hilfsfunktionen
# =============================================================================

def snap(wert: float, werte_liste: list) -> int:
    """Nächster Wert in der Raster-Liste (bei Gleichstand: kleinerer Wert)."""
    return min(werte_liste, key=lambda x: (abs(x - wert), x))


def strings_vorbereiten() -> list:
    """Ergänzt jeden String um kwp_gesamt und gesnappte Grid-Werte."""
    ergebnis = []
    for s in STRINGS:
        tilt_grid = snap(s["tilt"],    config.TILTS)
        azi_grid  = snap(s["azimuth"], config.AZIMUTHS)
        kwp_ges   = round(s["anzahl"] * s["kwp_modul"], 4)
        ergebnis.append({**s, "kwp_gesamt": kwp_ges,
                         "tilt_grid": tilt_grid, "azi_grid": azi_grid})
    return ergebnis


def sub_laden(df: pd.DataFrame, tilt: int, azimuth: int) -> pd.DataFrame:
    """Filtert und sortiert eine Tilt/Azimuth-Kombination."""
    return (df[(df["tilt"] == tilt) & (df["azimuth"] == azimuth)]
              .sort_values("stunde").reset_index(drop=True))


def gesamt_berechnen(strings_data: list) -> pd.DataFrame:
    """
    Summiert skalierte Leistung aller Strings.
      P_mean_ges = Σ(P_mean_i × kWp_i)   [W absolut]
      P_std_ges  = Σ(P_std_i  × kWp_i)   [W] — konservativ (gleicher Standort, korreliert)
    """
    stunden   = strings_data[0]["sub"]["stunde"].values
    mean_sum  = np.zeros(len(stunden))
    std_sum   = np.zeros(len(stunden))
    for item in strings_data:
        kwp       = item["kwp_gesamt"]
        mean_sum += item["sub"]["P_mean"].values * kwp
        std_sum  += item["sub"]["P_std"].values  * kwp
    return pd.DataFrame({"stunde": stunden, "P_mean": mean_sum, "P_std": std_sum})


def plot_speichern(fig, dateiname: str) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    pfad = os.path.join(OUTPUT_DIR, dateiname)
    fig.savefig(pfad, bbox_inches="tight", dpi=150)
    plt.close(fig)
    print(f"  Gespeichert: {pfad}")


def standort_info() -> str:
    return f"{config.NAME}  |  {config.LAT}°N, {config.LON}°E  |  2005–2020 (16 Jahre)"


def string_label(s: dict, kurz: bool = False) -> str:
    """Beschriftung eines Strings mit allen relevanten Parametern."""
    basis = (f"{s['name']}: {s['anzahl']}×{s['kwp_modul']:.3f} kWp "
             f"({s['kwp_gesamt']:.3f} kWp)")
    grid  = f"Tilt={s['tilt_grid']}°, Azi={s['azi_grid']:+d}°"
    if s["tilt"] != s["tilt_grid"] or s["azimuth"] != s["azi_grid"]:
        original = f"[orig. Tilt={s['tilt']}°, Azi={s['azimuth']:+d}°]"
        return f"{basis}  {grid}  {original}" if not kurz else f"{basis}  {grid}*"
    return f"{basis}  {grid}"


# =============================================================================
# Plot 1: Tagesverlauf Jahresschnitt
# =============================================================================

def plot_tagesverlauf_gesamt(strings: list) -> None:
    print("  [1/2] Tagesverlauf Jahresschnitt ...")
    df_all = pd.read_csv(os.path.join(STATS_DIR, "tagesverlauf_gesamt.csv"))

    strings_data = []
    for s in strings:
        sub = sub_laden(df_all, s["tilt_grid"], s["azi_grid"])
        if sub.empty:
            print(f"    WARNUNG: Keine Daten für {s['name']} "
                  f"(Tilt={s['tilt_grid']}°, Azi={s['azi_grid']}°)")
        strings_data.append({**s, "sub": sub})

    gesamt    = gesamt_berechnen(strings_data)
    kwp_total = sum(s["kwp_gesamt"] for s in strings)

    fig, ax = plt.subplots(figsize=(13, 6))

    # Einzelne Strings
    for item, farbe in zip(strings_data, FARBEN_STRINGS):
        if item["sub"].empty:
            continue
        skaliert = item["sub"]["P_mean"] * item["kwp_gesamt"]
        ax.plot(item["sub"]["stunde"], skaliert, color=farbe, lw=1.8,
                label=string_label(item))

    # Gesamt mit ±std
    ax.plot(gesamt["stunde"], gesamt["P_mean"],
            color=FARBE_GESAMT, lw=2.5,
            label=f"Gesamt  {kwp_total:.3f} kWp")
    ax.fill_between(gesamt["stunde"],
                    (gesamt["P_mean"] - gesamt["P_std"]).clip(lower=0),
                    gesamt["P_mean"] + gesamt["P_std"],
                    alpha=0.2, color=FARBE_GESAMT, label="Gesamt ±1 Std.abw.")

    ax.set_xlabel("Stunde des Tages")
    ax.set_ylabel("Leistung (W)")
    ax.set_title(f"Tagesverlauf — Jahresschnitt  |  {OBJEKT_NAME}\n{standort_info()}")
    ax.set_xticks(range(0, 24))
    ax.legend(fontsize=8, loc="upper left")
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plot_speichern(fig, "01_tagesverlauf_gesamt.png")


# =============================================================================
# Plot 2: Tagesverlauf 12 Monate
# =============================================================================

def plot_tagesverlauf_12monate(strings: list) -> None:
    print("  [2/2] Tagesverlauf 12 Monate ...")
    kwp_total = sum(s["kwp_gesamt"] for s in strings)

    # Alle 12 Monate vorladen
    monate_data = []
    for m in range(1, 13):
        df_m = pd.read_csv(os.path.join(STATS_DIR, f"tagesverlauf_monat_{m:02d}.csv"))
        strings_data = [{**s, "sub": sub_laden(df_m, s["tilt_grid"], s["azi_grid"])}
                        for s in strings]
        gesamt = gesamt_berechnen(strings_data)
        monate_data.append({"strings_data": strings_data, "gesamt": gesamt})

    # Gemeinsame Y-Achse
    y_max = max(
        (g["gesamt"]["P_mean"] + g["gesamt"]["P_std"]).max()
        for g in monate_data
    )

    fig, axes = plt.subplots(3, 4, figsize=(22, 13), sharex=True, sharey=True)
    fig.suptitle(
        f"Tagesverläufe — Alle 12 Monate  |  {OBJEKT_NAME}\n"
        f"{standort_info()}  |  Gesamt: {kwp_total:.3f} kWp",
        fontsize=12, y=1.01
    )

    for idx, ax in enumerate(axes.flat):
        m    = idx + 1
        data = monate_data[idx]

        # Einzelne Strings (dünn)
        for item, farbe in zip(data["strings_data"], FARBEN_STRINGS):
            if item["sub"].empty:
                continue
            skaliert = item["sub"]["P_mean"] * item["kwp_gesamt"]
            ax.plot(item["sub"]["stunde"], skaliert,
                    color=farbe, lw=1.0, alpha=0.85)

        # Gesamt (fett, mit Band)
        g = data["gesamt"]
        ax.plot(g["stunde"], g["P_mean"], color=FARBE_GESAMT, lw=2.0)
        ax.fill_between(g["stunde"],
                        (g["P_mean"] - g["P_std"]).clip(lower=0),
                        g["P_mean"] + g["P_std"],
                        alpha=0.2, color=FARBE_GESAMT)

        ax.set_title(MONATSNAMEN[idx], fontsize=10)
        ax.set_ylim(0, y_max * 1.05)
        ax.set_xticks(range(0, 24, 4))
        ax.set_xlabel("Stunde" if idx >= 8 else "", fontsize=8)
        ax.set_ylabel("W" if idx % 4 == 0 else "", fontsize=8)
        ax.tick_params(labelsize=8)
        ax.grid(True, alpha=0.3)

    # Gemeinsame Legende unterhalb der Grafik
    handles = []
    for s, farbe in zip(strings, FARBEN_STRINGS):
        handles.append(plt.Line2D([0], [0], color=farbe, lw=1.5,
                                  label=string_label(s, kurz=True)))
    handles.append(plt.Line2D([0], [0], color=FARBE_GESAMT, lw=2.5,
                               label=f"Gesamt ({kwp_total:.3f} kWp)  mean ± std"))
    fig.legend(handles=handles, loc="lower center", ncol=2,
               fontsize=9, bbox_to_anchor=(0.5, -0.05))

    plt.tight_layout()
    plot_speichern(fig, "02_tagesverlauf_12monate.png")


# =============================================================================
# Plot 3: Tageserträge (Tag 1–365)
# =============================================================================

# Monatsgrenzen für X-Achsen-Beschriftung (kumulierter Tagesbeginn je Monat)
MONAT_TAGE     = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
MONAT_GRENZEN  = [sum(MONAT_TAGE[:i]) + 1 for i in range(12)]   # Erster Tag je Monat


def plot_tagesertraege_365(strings: list) -> None:
    """
    Täglicher Ertrag (kWh absolut) von Tag 1 bis 365.
    Einzelne Strings als Linien, Gesamt mit ±std-Band.
    """
    print("  [3/4] Tageserträge 365 Tage ...")
    df_tag    = pd.read_csv(os.path.join(STATS_DIR, "tagesertraege.csv"))
    kwp_total = sum(s["kwp_gesamt"] for s in strings)

    # Strings laden und skalieren
    strings_tag = []
    for s in strings:
        sub = (df_tag[(df_tag["tilt"]    == s["tilt_grid"]) &
                      (df_tag["azimuth"] == s["azi_grid"])]
                     .sort_values("tag").reset_index(drop=True))
        strings_tag.append({
            **s,
            "E_d_mean": sub["E_d_mean"].values * s["kwp_gesamt"],
            "E_d_std":  sub["E_d_std"].values  * s["kwp_gesamt"],
            "tag":      sub["tag"].values,
        })

    # Gesamt
    tage     = strings_tag[0]["tag"]
    ges_mean = sum(st["E_d_mean"] for st in strings_tag)
    ges_std  = sum(st["E_d_std"]  for st in strings_tag)

    fig, ax = plt.subplots(figsize=(15, 6))

    # Einzelne Strings
    for st, farbe in zip(strings_tag, FARBEN_STRINGS):
        ax.plot(st["tag"], st["E_d_mean"], color=farbe, lw=1.2, alpha=0.85,
                label=string_label(st, kurz=True))

    # Gesamt mit ±std-Band
    ax.plot(tage, ges_mean, color=FARBE_GESAMT, lw=2.0,
            label=f"Gesamt ({kwp_total:.3f} kWp)")
    ax.fill_between(tage,
                    (ges_mean - ges_std).clip(min=0),
                    ges_mean + ges_std,
                    alpha=0.2, color=FARBE_GESAMT, label="Gesamt ±1 Std.abw.")

    # Monatsmarkierungen
    for i, (tag, name) in enumerate(zip(MONAT_GRENZEN, MONATSNAMEN_KURZ)):
        ax.axvline(tag, color="gray", lw=0.5, ls="--", alpha=0.5)
        ax.text(tag + 1, ax.get_ylim()[1] * 0.98, name, fontsize=7,
                va="top", color="gray")

    ax.set_xlabel("Tag des Jahres")
    ax.set_ylabel("Tagesertrag (kWh)")
    ax.set_xlim(1, 365)
    ax.set_ylim(bottom=0)
    ax.set_title(f"Tageserträge Tag 1–365  |  {OBJEKT_NAME}\n{standort_info()}")
    ax.legend(fontsize=8, loc="upper left")
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plot_speichern(fig, "03_tagesertraege_365.png")


# =============================================================================
# Plot 4: Monatserträge mit Fehlerbalken
# =============================================================================

def plot_monatsertraege(strings: list) -> None:
    """
    Monatserträge je String (skaliert auf kWh absolut) + Gesamt als gruppiertes
    Balkendiagramm mit Fehlerbalken (mean ± std).
    """
    print("  [4/4] Monatserträge mit Fehlerbalken ...")
    df_monat  = pd.read_csv(os.path.join(STATS_DIR, "monatsertraege.csv"))
    kwp_total = sum(s["kwp_gesamt"] for s in strings)

    # Monatsdaten je String laden und skalieren
    strings_monat = []
    for s in strings:
        sub = (df_monat[(df_monat["tilt"]    == s["tilt_grid"]) &
                        (df_monat["azimuth"] == s["azi_grid"])]
                       .sort_values("monat").reset_index(drop=True))
        strings_monat.append({
            **s,
            "E_m_mean": sub["E_m_mean"].values * s["kwp_gesamt"],
            "E_m_std":  sub["E_m_std"].values  * s["kwp_gesamt"],
        })

    # Gesamt: Summe über alle Strings
    monate = df_monat["monat"].unique()
    ges_mean = sum(sm["E_m_mean"] for sm in strings_monat)
    ges_std  = sum(sm["E_m_std"]  for sm in strings_monat)

    # Balkenbreiten für n_strings + Gesamt
    n      = len(strings) + 1          # Strings + Gesamt
    x      = np.arange(12)
    breite = 0.8 / n

    fig, ax = plt.subplots(figsize=(15, 6))

    # Einzelne Strings
    for i, (sm, farbe) in enumerate(zip(strings_monat, FARBEN_STRINGS)):
        ax.bar(x + i * breite, sm["E_m_mean"], breite,
               yerr=sm["E_m_std"], capsize=3,
               color=farbe, alpha=0.85,
               label=string_label(sm, kurz=True),
               error_kw={"elinewidth": 1, "ecolor": "black"})

    # Gesamt
    ax.bar(x + len(strings) * breite, ges_mean, breite,
           yerr=ges_std, capsize=3,
           color=FARBE_GESAMT, alpha=0.85,
           label=f"Gesamt ({kwp_total:.3f} kWp)",
           error_kw={"elinewidth": 1, "ecolor": "black"})

    ax.set_xticks(x + breite * (n - 1) / 2)
    ax.set_xticklabels(MONATSNAMEN_KURZ)
    ax.set_ylabel("Monatsertrag (kWh)")
    ax.set_title(f"Monatserträge mean ± std — je String + Gesamt  |  {OBJEKT_NAME}\n"
                 f"{standort_info()}")
    ax.legend(fontsize=8, loc="upper left")
    ax.grid(True, axis="y", alpha=0.3)
    plt.tight_layout()
    plot_speichern(fig, "04_monatsertraege.png")


# =============================================================================
# Hauptprogramm
# =============================================================================

def main():
    strings = strings_vorbereiten()
    kwp_total = sum(s["kwp_gesamt"] for s in strings)

    print(f"\nPVGIS Objekt-Visualizer  —  {OBJEKT_NAME}")
    print(f"Standort: {standort_info()}")
    print(f"Ausgabe:  {OUTPUT_DIR}\n")

    print("Strings:")
    for s in strings:
        gerastert = ""
        if s["tilt"] != s["tilt_grid"] or s["azimuth"] != s["azi_grid"]:
            gerastert = (f"  → gerastert auf Tilt={s['tilt_grid']}°, "
                         f"Azi={s['azi_grid']:+d}°")
        print(f"  {s['name']}: {s['anzahl']}×{s['kwp_modul']:.3f} kWp = "
              f"{s['kwp_gesamt']:.3f} kWp  "
              f"Tilt={s['tilt']}°, Azi={s['azimuth']:+d}°{gerastert}")
    print(f"  Gesamt: {kwp_total:.3f} kWp\n")

    print("Erstelle Plots ...")
    plot_tagesverlauf_gesamt(strings)
    plot_tagesverlauf_12monate(strings)
    plot_tagesertraege_365(strings)
    plot_monatsertraege(strings)

    print(f"\n{'='*60}")
    print(f"Fertig. 4 Plots unter: {OUTPUT_DIR}/")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
