# Vertikale bifaziale Module

Untersuchung senkrecht aufgeständerter Module (Tilt = 90°) über alle
Himmelsausrichtungen, auf Basis der vorhandenen 16-Jahres-Statistiken
(`data/processed/{NAME}/stats/`, erzeugt von `scripts/03_analyze.py`).

## Modell

Ein bifaziales Modul hat eine definierte Vorderseite (Azimuth a) und eine
Rückseite (Azimuth a + 180°). Die Rückseite wird mit einem Wirkungsgradfaktor
angesetzt:

    E_bifazial(a) = E(90°, a) + f_rueck * E(90°, a + 180°)

`f_rueck` ist über `--rueckseite` einstellbar, Standard 0.90.

Beide Seiten stammen aus derselben PVGIS-Rasterrechnung (jeweils monofazial,
1 kWp, 14 % Systemverluste). Rückseitenverschattung durch die
Unterkonstruktion und zusätzlicher Bodenalbedo-Gewinn sind nicht abgebildet —
die Werte sind eine obere Abschätzung.

## Achsen-Konvention

Die x-Achse von Plot 1 läuft von −90° bis +90°.

- **Monofazial** ist das der Azimuth der Vorderseite in der Projektkonvention:
  0° = Süd, −90° = Ost, +90° = West. Diese Kurve ist unverändert.
- **Bifazial** ist die Achse um 90° verschoben, so dass die symmetrische
  Ost/West-Ebene bei 0° liegt. Sie ist für ein bifaziales Modul das
  Gegenstück zur Süd-Ausrichtung eines monofazialen Moduls, weil sie Morgen-
  und Abendsonne spiegelbildlich zur Südachse einsammelt.

| Ausrichtung | Vorderseite | Rückseite |
|-------------|-------------|-----------|
| −90° | Nord | Süd |
| −45° | Nordost | Südwest |
| 0° | Ost | West |
| +45° | Südost | Nordwest |
| +90° | Süd | Nord |

Die zweite (blaue) Kurve ist dieselbe Modulebene mit um
180° gedrehtem Modul, also gute Seite auf der West- bzw. Nordseite.

## Aufruf

```bash
python vertikal/07_vertikal_bifazial.py
python vertikal/07_vertikal_bifazial.py --rueckseite 0.75 --drehung 0
```

| Option | Bedeutung |
|--------|-----------|
| `--rueckseite` | Wirkungsgrad der Rückseite relativ zur Vorderseite (0.0–1.0, Standard 0.90) |
| `--drehung` | Bifaziale Ausrichtung für die Tageskacheln, −90° bis +90° wie in Plot 1 (Standard: Optimum) |
| `--azimuth` | Alternative dazu: Azimuth der Vorderseite in der Projektkonvention |

## Ausgabe

`output/plots/{NAME}/vertikal/`

| Datei | Inhalt |
|-------|--------|
| `01_vertikal_jahresertrag_azimuth.png` | Jahresertrag über die Ausrichtung (−90° bis +90°), beide Montagerichtungen, monofaziale Vergleichskurve und die beste Süd-Ausrichtung als Referenzlinie |
| `02_vertikal_tageskacheln.png` | Kalender-Kacheln 12 × 31: mittlerer Tagesertrag jedes Kalendertages für eine gewählte Ausrichtung |

## Erlösgrafik je Preisjahr

`08_vertikal_erloese_preisjahre.py` zeichnet dieselbe Ausrichtungsachse, aber
mit dem Einspeiseerlös in EUR/kWp statt dem Ertrag in kWh. Gerechnet wird
stundenweise: mittlere PV-Leistung der Jahresstunde mal Spotpreis derselben
Stunde. Je Kurve ein Preisjahr, jüngstes zuerst, dazu je Jahr die beste
Süd-Ausrichtung als gestrichelte Linie.

```bash
python vertikal/08_vertikal_erloese_preisjahre.py
```

Voraussetzung ist `wirtschaftlichkeit/12_analyze_erloese.py`, das die
Erlöse je Kombination und Preisjahr berechnet. Ausgabe:
`output/plots/{NAME}/vertikal/03_vertikal_erloes_preisjahre.png`.

## Ergebnis für einen Standort in Norddeutschland (52,3°N, 9,6°E, f_rueck = 0.90)

| Variante | Jahresertrag | Anteil an Süd-Referenz |
|----------|--------------|------------------------|
| Beste Süd-Ausrichtung (Tilt 40°, Azimuth 0°) | 1000 kWh/kWp | 100 % |
| Vertikal bifazial Ost/West (Optimum bei +10°) | 958 kWh/kWp | 96 % |
| Vertikal bifazial Süd/Nord (+90°) | 887 kWh/kWp | 89 % |
| Vertikal monofazial Süd | 716 kWh/kWp | 72 % |

Die Ost/West-Ebene ist das Optimum: beide Seiten bekommen jeweils eine halbe
Tageshälfte volle Sonne. Die Süd/Nord-Ebene liegt rund 7 % darunter, dafür mit
ausgeprägtem Morgen- und Abendmaximum im Tagesverlauf.
