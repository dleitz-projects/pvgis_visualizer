"""
07_vertikal_bifazial.py
=======================
Untersuchung vertikal aufgeständerter bifazialer Module (Tilt = 90°).

Modell
------
Ein vertikales bifaziales Modul besteht aus einer definierten Vorderseite
(Azimuth a) und einer definierten Rückseite (Azimuth a + 180°). Die Rückseite
wird mit einem Wirkungsgrad-Faktor (Bifazialitätsfaktor, Standard 0.90)
angesetzt:

    E_bifazial(a) = E(90°, a) + f_rueck * E(90°, a + 180°)

Beide Seiten stammen aus derselben PVGIS-Rasterrechnung (jeweils monofazial,
1 kWp). Verschattung der Rückseite durch die Unterkonstruktion und ein
erhöhter Bodenalbedo-Anteil sind darin nicht abgebildet — die Werte sind
daher eine obere Abschätzung.

Datenquelle: data/processed/{NAME}/stats/jahresertraege.csv  (16-Jahres-Mittel)
             data/processed/{NAME}/stats/tagesertraege.csv   (Tagesmittel 1–365)
Ausgabe:     output/plots/{NAME}/vertikal/

Plots:
  01_vertikal_jahresertrag_azimuth.png  ← Jahresertrag über die Ausrichtung
                                          (-90° bis +90°; bifazial ist 0° = Ost/West-
                                          Ebene), beide Montagerichtungen + Süd-Referenz
  02_vertikal_tageskacheln.png          ← Kalender-Kacheln: mittlerer Tagesertrag
                                          jedes Tages im Jahr für eine Ausrichtung

Aufruf:
  python vertikal/07_vertikal_bifazial.py
  python vertikal/07_vertikal_bifazial.py --rueckseite 0.75 --drehung 0
"""

import argparse
import os
import sys

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import config

# =============================================================================
# Konstanten
# =============================================================================

STATS_DIR  = os.path.join(config.DATA_PROCESSED_DIR, "stats")
OUTPUT_DIR = os.path.join(config.PLOTS_DIR, "vertikal")

TILT_VERTIKAL = 90

MONATSNAMEN      = ["Januar", "Februar", "März", "April", "Mai", "Juni",
                    "Juli", "August", "September", "Oktober", "November", "Dezember"]
MONATSTAGE       = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]  # Nicht-Schaltjahr

FARBE_VORNE  = "#e74c3c"   # bifaziale Aufstellung
FARBE_HINTEN = "#e74c3c"   # dasselbe Modul um 180° gedreht: gleiche Farbe,
                           # gestrichelt — wie in den Erlösgrafiken
FARBE_MONO   = "#7f8c8d"   # nur Vorderseite (monofazial)
FARBE_REF    = "#27ae60"   # Süd-Referenz

CMAP_KACHELN = "RdYlGn"

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

def plot_speichern(fig, dateiname: str) -> None:
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    pfad = os.path.join(OUTPUT_DIR, dateiname)
    fig.savefig(pfad, bbox_inches="tight", dpi=150)
    plt.close(fig)
    print(f"  Gespeichert: {pfad}")


def standort_info() -> str:
    return (f"{config.NAME}  |  {config.STANDORT_ANZEIGE}  |  "
            f"{config.SERIES_STARTYEAR}–{config.SERIES_ENDYEAR} "
            f"({config.SERIES_ENDYEAR - config.SERIES_STARTYEAR + 1} Jahre)  |  "
            f"1 kWp, {config.LOSS} % Systemverluste")


def gegen_azimuth(azi: int) -> int:
    """Azimuth der Rückseite: +180° und zurück auf das Intervall [-180, +180]."""
    gegen = azi + 180
    if gegen > 180:
        gegen -= 360
    return gegen


def azimuth_label(azi: int) -> str:
    """Himmelsrichtung als Klartext, z. B. '-20° (SSO)'."""
    richtungen = ["S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW",
                  "N", "NNO", "NO", "ONO", "O", "OSO", "SO", "SSO"]
    idx = int(round((azi % 360) / 22.5)) % 16
    return f"{azi:+d}° ({richtungen[idx]})"


def vertikal_serie(df: pd.DataFrame, wert: str) -> pd.Series:
    """Werte für Tilt = 90° als Series, indiziert über den Azimuth."""
    sub = df[df["tilt"] == TILT_VERTIKAL]
    return sub.set_index("azimuth")[wert].sort_index()


def bifazial(serie: pd.Series, azi: int, f_rueck: float) -> float:
    """Bifazialer Ertrag für eine Vorderseiten-Ausrichtung."""
    return serie.loc[azi] + f_rueck * serie.loc[gegen_azimuth(azi)]


def sued_referenz(df_jahr: pd.DataFrame):
    """Beste reine Süd-Ausrichtung (Azimuth 0°) über alle Neigungswinkel."""
    sued = df_jahr[df_jahr["azimuth"] == 0]
    zeile = sued.loc[sued["E_y_mean"].idxmax()]
    return int(zeile["tilt"]), float(zeile["E_y_mean"])


# =============================================================================
# Plot 1: Jahresertrag über die Ausrichtung
# =============================================================================

def drehung_zu_azimuth(beta: int) -> int:
    """
    Bifaziale Ausrichtung beta → Azimuth der Vorderseite.

    Für bifaziale Module ist die symmetrische Ost/West-Ebene die Entsprechung
    der Süd-Ausrichtung eines monofazialen Moduls: sie sammelt Morgen- und
    Abendsonne spiegelbildlich zur Südachse. Sie liegt deshalb bei beta = 0.

      beta = -90° → Vorderseite Nord (−180°), Rückseite Süd
      beta =   0° → Vorderseite Ost  (−90°),  Rückseite West
      beta = +90° → Vorderseite Süd  (0°),    Rückseite Nord
    """
    return beta - 90


def plot_jahresertrag_azimuth(df_jahr: pd.DataFrame, f_rueck: float) -> dict:
    """
    Jahresertrag vertikaler Module über die Ausrichtung, von -90° bis +90°.

    Monofazial: x ist der Azimuth der Vorderseite in der Projektkonvention
                (0 = Süd, -90 = Ost, +90 = West).
    Bifazial:   x ist um 90° verschoben, so dass die symmetrische
                Ost/West-Ebene bei 0° liegt (siehe drehung_zu_azimuth).

    Kurve 1: gute Seite auf der Ost-Seite der Ebene (Azimuth beta - 90°).
    Kurve 2: dieselbe Ebene, Modul um 180° gedreht (gute Seite auf der
             West-Seite, Azimuth beta + 90°).
    """
    print("  [1/2] Jahresertrag über die Ausrichtung ...")
    serie = vertikal_serie(df_jahr, "E_y_mean")

    schritt = int(min(np.diff(sorted(serie.index))))
    beta    = list(range(-90, 91, schritt))

    azi_front = [drehung_zu_azimuth(b) for b in beta]          # -180 ... 0
    azi_back  = [gegen_azimuth(a) for a in azi_front]          #    0 ... 180

    e_vorne  = np.array([serie.loc[a] + f_rueck * serie.loc[r]
                         for a, r in zip(azi_front, azi_back)])
    e_hinten = np.array([serie.loc[r] + f_rueck * serie.loc[a]
                         for a, r in zip(azi_front, azi_back)])
    e_mono   = np.array([serie.loc[b] for b in beta])          # unverändert

    ref_tilt, ref_wert = sued_referenz(df_jahr)
    e_sued_nord = e_vorne[beta.index(90)]                      # Vorderseite Süd

    i_max    = int(np.argmax(e_vorne))
    beta_max = beta[i_max]
    azi_max  = azi_front[i_max]
    max_val  = e_vorne[i_max]

    fig, ax = plt.subplots(figsize=(13, 7))

    ax.plot(beta, e_vorne, color=FARBE_VORNE, lw=2.4, marker="o", ms=4,
            label=f"Bifazial — Vorderseite (100 %) auf der Ost-Seite der Ebene "
                  f"(bei 0°: Ost), Rückseite ({f_rueck:.0%}) gegenüber")
    ax.plot(beta, e_hinten, color=FARBE_HINTEN, lw=1.8, ls="--", marker="s", ms=3.5,
            alpha=0.85,
            label=f"Bifazial — Modul um 180° gedreht: Vorderseite (100 %) auf der "
                  f"West-Seite der Ebene (bei 0°: West)")
    ax.plot(beta, e_mono, color=FARBE_MONO, lw=1.4, ls=":",
            label="Monofazial vertikal (Azimuth der Vorderseite, 0 = Süd)")

    ax.axhline(ref_wert, color=FARBE_REF, lw=2.0, ls="--")
    ax.text(88, ref_wert, f"  Konventionelle Ausrichtung: Azimut 0° (Süd), "
                          f"Neigung {ref_tilt}° = {ref_wert:.0f} kWh/kWp",
            color=FARBE_REF, fontsize=8.5, va="bottom", ha="right")

    ax.plot([beta_max], [max_val], marker="*", ms=16, color=FARBE_VORNE, zorder=5)
    nach_rechts = beta_max < 0
    ax.annotate(f"Optimum vertikal bifazial\nAusrichtung {beta_max:+d}° "
                f"(Vorderseite {azimuth_label(azi_max)}) · {max_val:.0f} kWh/kWp · "
                f"{max_val / ref_wert:.0%} der konventionellen Ausrichtung",
                xy=(beta_max, max_val),
                xytext=(beta_max + (5 if nach_rechts else -5),
                        max_val - 0.16 * ref_wert),
                ha="left" if nach_rechts else "right",
                fontsize=8.5, color=FARBE_VORNE,
                arrowprops=dict(arrowstyle="->", color=FARBE_VORNE, lw=1.0))

    # Rechte Achse: Anteil an der Süd-Referenz
    ax2 = ax.twinx()
    ax2.set_ylabel(f"Anteil an der konventionellen Ausrichtung "
                   f"({ref_wert:.0f} kWh/kWp) in %", fontsize=9)

    ax.set_ylim(0, max(ref_wert, e_vorne.max()) * 1.22)
    ax2.set_ylim(0, ax.get_ylim()[1] / ref_wert * 100)

    ax.set_xlim(-92, 92)
    ax.set_xticks(beta)
    ax.set_xticklabels([f"{b:+d}" for b in beta], fontsize=8)
    ax.set_xlabel("Ausrichtung (°)   —   monofazial: Azimuth der Vorderseite "
                  "(0 = Süd)   |   bifazial: 0 = Ost/West-Ebene")
    ax.set_ylabel("Jahresertrag (kWh/kWp)")

    ax.set_title(f"Vertikale Module (Tilt 90°) — Jahresertrag über alle "
                 f"Himmelsausrichtungen\n{standort_info()}  |  "
                 f"Rückseiten-Wirkungsgrad {f_rueck:.0%}\n"
                 f"Bifazial Ost/West (0°): {e_vorne[beta.index(0)]:.0f} kWh/kWp  ·  "
                 f"Bifazial Süd/Nord (+90°): {e_sued_nord:.0f} kWh/kWp  ·  "
                 f"Konventionell: {ref_wert:.0f} kWh/kWp",
                 fontsize=11)
    ax.legend(fontsize=8.5, loc="lower center", framealpha=0.92)
    ax.grid(True, alpha=0.3)
    plt.tight_layout()
    plot_speichern(fig, "01_vertikal_jahresertrag_azimuth.png")

    return {"beta": beta, "azi_front": azi_front,
            "e_vorne": e_vorne, "e_hinten": e_hinten, "e_mono": e_mono,
            "beta_max": beta_max, "azi_max": azi_max, "max_val": max_val,
            "ref_tilt": ref_tilt, "ref_wert": ref_wert}


# =============================================================================
# Plot 2: Kalender-Kacheln der Tageserträge
# =============================================================================

def kalender_matrix(werte: np.ndarray) -> np.ndarray:
    """365 Tageswerte → Matrix 12 Monate × 31 Tage (fehlende Tage = NaN)."""
    matrix = np.full((12, 31), np.nan)
    tag = 0
    for m, tage in enumerate(MONATSTAGE):
        matrix[m, :tage] = werte[tag:tag + tage]
        tag += tage
    return matrix


def plot_tageskacheln(df_tag: pd.DataFrame, azi_front: int, f_rueck: float,
                      ref_wert: float) -> None:
    """Mittlerer Tagesertrag jedes einzelnen Tages im Jahr als Kachelgrafik."""
    print("  [2/2] Tageskacheln ...")
    azi_rueck = gegen_azimuth(azi_front)

    def tagesreihe(azi: int) -> np.ndarray:
        sub = df_tag[(df_tag["tilt"] == TILT_VERTIKAL) & (df_tag["azimuth"] == azi)]
        return sub.sort_values("tag")["E_d_mean"].values[:365]

    werte  = tagesreihe(azi_front) + f_rueck * tagesreihe(azi_rueck)
    matrix = kalender_matrix(werte)
    jahr   = float(np.nansum(matrix))

    fig, ax = plt.subplots(figsize=(16, 6))
    bild = ax.imshow(matrix, cmap=CMAP_KACHELN, aspect="auto",
                     vmin=0, vmax=np.nanmax(matrix))

    for m in range(12):
        for t in range(31):
            if np.isnan(matrix[m, t]):
                continue
            ax.text(t, m, f"{matrix[m, t]:.2f}", ha="center", va="center",
                    fontsize=5.2, color="black")

    ax.set_xticks(range(31))
    ax.set_xticklabels(range(1, 32), fontsize=7)
    ax.set_yticks(range(12))
    ax.set_yticklabels(MONATSNAMEN, fontsize=9)
    ax.set_xlabel("Tag im Monat")
    ax.set_xticks(np.arange(-0.5, 31, 1), minor=True)
    ax.set_yticks(np.arange(-0.5, 12, 1), minor=True)
    ax.grid(which="minor", color="white", lw=0.6)
    ax.tick_params(which="minor", length=0)

    cbar = fig.colorbar(bild, ax=ax, pad=0.015)
    cbar.set_label("Mittlerer Tagesertrag (kWh/kWp und Tag)", fontsize=9)

    ax.set_title(f"Vertikal bifazial (Tilt 90°) — mittlerer Tagesertrag je Kalendertag\n"
                 f"Vorderseite {azimuth_label(azi_front)} · "
                 f"Rückseite {azimuth_label(azi_rueck)} mit {f_rueck:.0%} Wirkungsgrad\n"
                 f"{standort_info()}\n"
                 f"Jahressumme: {jahr:.0f} kWh/kWp "
                 f"({jahr / ref_wert:.0%} der besten Süd-Ausrichtung)", fontsize=11)
    plt.tight_layout()
    plot_speichern(fig, "02_vertikal_tageskacheln.png")


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--rueckseite", type=float, default=0.90,
                        help="Wirkungsgrad der Rückseite relativ zur Vorderseite "
                             "(Standard: 0.90)")
    parser.add_argument("--azimuth", type=int, default=None,
                        help="Azimuth der Vorderseite für die Tageskacheln "
                             "(Standard: das Optimum aus Plot 1)")
    parser.add_argument("--drehung", type=int, default=None,
                        help="Alternative zu --azimuth: bifaziale Ausrichtung in ° "
                             "wie in Plot 1 (0 = Ost/West, +90 = Süd/Nord)")
    args = parser.parse_args()

    f_rueck = args.rueckseite
    if not 0.0 <= f_rueck <= 1.0:
        parser.error("--rueckseite muss zwischen 0.0 und 1.0 liegen")

    pfad_jahr = os.path.join(STATS_DIR, "jahresertraege.csv")
    pfad_tag  = os.path.join(STATS_DIR, "tagesertraege.csv")
    for pfad in (pfad_jahr, pfad_tag):
        if not os.path.exists(pfad):
            sys.exit(f"Fehlt: {pfad} — bitte zuerst scripts/03_analyze.py ausführen.")

    df_jahr = pd.read_csv(pfad_jahr)
    df_tag  = pd.read_csv(pfad_tag)

    print(f"Vertikale bifaziale Module — {config.NAME}, "
          f"Rückseiten-Wirkungsgrad {f_rueck:.0%}")

    ergebnis = plot_jahresertrag_azimuth(df_jahr, f_rueck)

    if args.azimuth is not None and args.drehung is not None:
        parser.error("--azimuth und --drehung schließen sich gegenseitig aus")
    if args.azimuth is not None:
        azi_front = args.azimuth
    elif args.drehung is not None:
        azi_front = drehung_zu_azimuth(args.drehung)
    else:
        azi_front = ergebnis["azi_max"]
    verfuegbar = sorted(df_tag[df_tag["tilt"] == TILT_VERTIKAL]["azimuth"].unique())
    if azi_front not in verfuegbar:
        azi_front = min(verfuegbar, key=lambda x: (abs(x - azi_front), x))
        print(f"  Azimuth auf Rasterwert {azi_front}° gesetzt.")

    plot_tageskacheln(df_tag, azi_front, f_rueck, ergebnis["ref_wert"])

    print(f"\nOptimum vertikal bifazial: Ausrichtung {ergebnis['beta_max']:+d}° "
          f"(Vorderseite Azimuth {ergebnis['azi_max']:+d}°), "
          f"{ergebnis['max_val']:.0f} kWh/kWp "
          f"({ergebnis['max_val'] / ergebnis['ref_wert']:.0%} der Süd-Referenz "
          f"Tilt {ergebnis['ref_tilt']}° = {ergebnis['ref_wert']:.0f} kWh/kWp)")


if __name__ == "__main__":
    main()
