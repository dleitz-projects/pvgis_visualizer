PVGIS SOLARERTRAG VISUALIZER — VOLLSTÄNDIGE DOKUMENTATION
==========================================================
Stand: April 2026 | Autor: Dennis Leitz
Projekt: pvgis-solar-ertrag-visualizer


ÜBERBLICK
---------
Dieses Python-Projekt analysiert den spezifischen Solarertrag (kWh/kWp) einer
Photovoltaikanlage systematisch über ein vollständiges Tilt/Azimuth-Raster.
Datengrundlage ist die kostenfreie PVGIS API v5.2 der Europäischen Kommission
(Joint Research Centre, JRC). Das Ziel ist es, Planungsfragen zur optimalen
Ausrichtung einer PV-Anlage direkt aus vorberechneten Grafiken ablesen zu können —
ohne jede Einzelsimulation.

Kernfrage: Welcher Neigungswinkel (Tilt) und welche Himmelsrichtung (Azimuth)
liefern den besten Jahresertrag, die beste Eigennutzung oder die gleichmäßigste
Tagesauslastung — und wie groß sind die Unterschiede?


RASTER-PARAMETER
----------------
  Tilt (Neigungswinkel):  0° bis 90° in 5°-Schritten  → 19 Werte
  Azimuth (Himmelsrichtung): −180° bis +180° in 10°-Schritten → 37 Werte
  Kombinationen gesamt: 19 × 37 = 703

  Azimuth-Konvention (PVGIS): 0° = Süd, −90° = Ost, +90° = West, ±180° = Nord


ZWEI-PHASEN-ARCHITEKTUR
-----------------------

Phase 1 — PVcalc (schnell, Jahres- und Monatswerte):
  - API-Endpoint: /PVcalc
  - Gibt E_y (kWh/kWp/Jahr) und E_m (12 Monatswerte) zurück
  - 703 Requests, ca. 12 Minuten bei 0,7 req/s
  - Zwischenspeicherung als JSON-Cache (wiederverwendbar)

Phase 2 — seriescalc (umfangreich, stündliche Zeitreihen):
  - API-Endpoint: /seriescalc
  - Gibt stündliche Leistungswerte über mehrere Jahre zurück
  - 703 × 10 Jahre × 8.760 h ≈ 61 Millionen Zeilen
  - Speicherung in DuckDB (ca. 1,5 GB, Laufzeit 1–2 Stunden)
  - Ermöglicht statistische Auswertungen: Mittelwert, Standardabweichung,
    Variationskoeffizient über interannuelle Variabilität


DATEIEN UND SKRIPTE
-------------------

config.py
  Zentrale Konfiguration. Alle Standort- und Rasterparameter werden hier
  einmalig gesetzt und von allen Skripten importiert.

  Wichtige Parameter:
    LAT, LON         Koordinaten des Standorts
    NAME             Kurzname für Ordnerstruktur (z. B. "Standort_2")
    TILT_MIN/MAX/STEP  Tilt-Raster (Standard: 0–90°, 5°-Schritte)
    AZI_MIN/MAX/STEP   Azimuth-Raster (Standard: −180°–+180°, 10°-Schritte)
    PEAKPOWER        Anlagenleistung für Normierung (Standard: 1 kWp)
    LOSS             Systemverluste in % (Standard: 14 %)
    PVCALC_YEAR      Abfragejahr für Phase 1 (Standard: 2023)
    SERIES_STARTYEAR / SERIES_ENDYEAR  Zeitraum für Phase 2 (2005–2020)
    REQUEST_DELAY    Pause zwischen API-Calls (Standard: 0,7 s)
    Pfade: DATA_RAW_DIR, DATA_PROCESSED_DIR, PLOTS_DIR


scripts/01_fetch_pvcalc.py  — Phase 1: PVcalc-Daten abrufen
  Ruft für alle 703 Tilt/Azimuth-Kombinationen die PVGIS PVcalc API ab.
  Speichert einen JSON-Cache (fortsetzbarer Download) und daraus abgeleitete
  CSV-Dateien im Long- und Pivot-Format.

  Funktionen:
    cache_laden(pfad)
      Lädt einen vorhandenen JSON-Cache oder gibt eine leere Struktur zurück.
      Ermöglicht das Fortsetzen abgebrochener Downloads.

    cache_speichern(pfad, cache)
      Speichert den aktuellen Cache als JSON-Datei (UTF-8).

    pvcalc_abrufen(tilt, azimuth, jahr)
      HTTP-GET gegen die PVGIS PVcalc API mit Retry-Logik (max. 3 Versuche,
      exponentielles Backoff). Gibt E_y und E_m zurück oder None bei Fehler.

    daten_abrufen(cache_pfad, jahr)
      Iteriert über alle 703 Kombinationen, überspringt bereits gecachte
      Einträge, speichert zwischendurch alle 50 neuen Einträge.

    cache_zu_dataframe(cache)
      Wandelt den JSON-Cache in ein Long-Format DataFrame um:
      Spalten: tilt, azimuth, E_y, E_m01 bis E_m12.

    pivot_erstellen(df, spalte)
      Erstellt eine Pivot-Tabelle: Index = Tilt (absteigend), Spalten = Azimuth.
      Direkt verwendbar als Heatmap-Eingabe.

    ergebnisse_speichern(df, basisname)
      Speichert Long-Format-CSV, Jahres-Pivot und 12 Monats-Pivots.

  Aufruf:
    python scripts/01_fetch_pvcalc.py
    python scripts/01_fetch_pvcalc.py --cache data/raw/pvcalc_..._2023_....json
    python scripts/01_fetch_pvcalc.py --jahr 2022

  Erzeugte Dateien:
    data/raw/pvcalc_{lat}_{lon}_{jahr}_{timestamp}.json
    data/processed/{NAME}/pvcalc_..._long.csv
    data/processed/{NAME}/pvcalc_..._pivot_E_y.csv
    data/processed/{NAME}/pvcalc_..._pivot_monat_01.csv  bis  _monat_12.csv


scripts/02_fetch_seriescalc.py  — Phase 2: Stündliche Zeitreihendaten
  Ruft für alle 703 Kombinationen stündliche Werte über einen mehrjährigen
  Zeitraum ab und speichert diese in einer DuckDB-Datenbank.

  Funktionen:
    db_verbinden(db_pfad)
      Öffnet die DuckDB-Datenbank und erstellt die Fortschrittstabelle
      fetch_log falls noch nicht vorhanden.

    tabelle_erstellen(con, df)
      Erstellt die hourly_data-Tabelle dynamisch aus dem ersten API-Response.
      Schema damit automatisch aus den tatsächlichen API-Spalten abgeleitet.

    bereits_abgerufen(con, tilt, azimuth)
      Prüft per SQL, ob eine Kombination bereits in der fetch_log-Tabelle
      eingetragen ist (crash-sicherer Fortschritt).

    als_abgerufen_markieren(con, tilt, azimuth)
      Trägt eine erfolgreich abgefragte Kombination in fetch_log ein.
      INSERT OR REPLACE verhindert Duplikate bei Wiederaufnahme.

    seriescalc_abrufen(tilt, azimuth, startyear, endyear)
      HTTP-GET gegen /seriescalc, Retry-Logik (max. 3 Versuche).
      Parst Zeitstempel, fügt tilt/azimuth als Spalten hinzu, filtert
      relevante Spalten: time, tilt, azimuth, P, Gb_i, Gd_i, Gr_i,
      H_sun, T2m, WS10m.

    daten_abrufen(db_pfad, startyear, endyear)
      Hauptschleife über alle 703 Kombinationen. Überspringt vorhandene
      Einträge via fetch_log. Expliziter con.commit() nach jedem Insert
      (wichtig auf Netzlaufwerken wie Google Drive). Erstellt Index
      auf (tilt, azimuth) nach Abschluss.

  Tabellenschema hourly_data:
    time (TIMESTAMP), tilt (INTEGER), azimuth (INTEGER),
    P (DOUBLE) [W/kWp], Gb_i/Gd_i/Gr_i (DOUBLE) [W/m²],
    H_sun (DOUBLE) [h], T2m (DOUBLE) [°C], WS10m (DOUBLE) [m/s]

  Aufruf:
    python scripts/02_fetch_seriescalc.py
    python scripts/02_fetch_seriescalc.py --startyear 2010 --endyear 2020

  Erzeugte Datei:
    data/processed/{NAME}/seriescalc_{lat}_{lon}_{startyear}_{endyear}.duckdb


scripts/03_analyze.py  — Statistische Auswertungen
  Berechnet Aggregationen direkt via DuckDB-SQL aus der hourly_data-Tabelle.
  Alle Auswertungen laufen auf der Datenbankebene (keine RAM-Engpässe).

  Funktionen:
    db_finden(ordner)
      Sucht die größte (vollständigste) seriescalc-Datenbank im Verzeichnis
      als Fallback, wenn kein expliziter Pfad angegeben wurde.

    pivot_erstellen(df, wert_spalte)
      Wie in 01: Pivot mit Tilt als Index, Azimuth als Spalten.

    csv_speichern(df, pfad) / pivot_speichern(df, pfad)
      Hilfsfunktionen zum Schreiben von Long-Format-CSV und Pivot-CSV.

    analyse_jahresertraege(con, stats_dir)
      SQL: Summiert P pro Jahr/Tilt/Azimuth → E_y in kWh/kWp.
      Aggregiert über alle Jahre: Anzahl Jahre, E_y_mean, E_y_std.
      Speichert jahresertraege.csv und Pivots für mean und std.

    analyse_monatsertraege(con, stats_dir)
      SQL: Summiert P pro Monat/Jahr/Tilt/Azimuth → E_m.
      Ergebnis: E_m_mean und E_m_std je Tilt/Azimuth/Monat.
      Speichert monatsertraege.csv und je 12 Pivot-CSVs für mean + std.

    analyse_tagesverlauf_gesamt(con, stats_dir)
      SQL: Mittelt P pro Stunde über alle Tage und Jahre.
      Ergebnis: P_mean, P_std, P_min, P_max je Tilt/Azimuth/Stunde.
      Repräsentiert den durchschnittlichen Jahresertragsverlauf über den Tag.

    analyse_tagesverlauf_monate(con, stats_dir)
      SQL: Wie tagesverlauf_gesamt, aber aufgeschlüsselt nach Monat.
      12 CSV-Dateien (tagesverlauf_monat_01.csv bis _12.csv).
      Zeigt saisonale Unterschiede im Tagesverlauf.

    analyse_tagesertraege(con, stats_dir)
      SQL: Summiert P pro Tag/Jahr/Tilt/Azimuth → E_d in kWh/kWp.
      Tag 366 (Schaltjahre) wird verworfen.
      Ergebnis: E_d_mean und E_d_std je Tilt/Azimuth/Kalendertag (1–365).

  Aufruf:
    python scripts/03_analyze.py
    python scripts/03_analyze.py --db data/processed/Standort_2/seriescalc_....duckdb

  Erzeugte Dateien unter data/processed/{NAME}/stats/:
    jahresertraege.csv, monatsertraege.csv
    tagesverlauf_gesamt.csv, tagesverlauf_monat_01.csv bis _12.csv
    tagesertraege.csv
    pivot/E_y_mean.csv, pivot/E_y_std.csv
    pivot/E_m01_mean.csv ... pivot/E_m12_std.csv


scripts/04_visualize.py  — Visualisierungen aus PVcalc-Daten (Phase 1)
  Erstellt Heatmaps und Vergleichsplots aus den Pivot-CSVs.

  Hilfsfunktionen:
    sigfig_annot(pivot, sig=3)
      Erzeugt String-Array mit 3 signifikanten Stellen für Heatmap-Annotierungen.
      Beispiel: 987,6 → "988", 88,46 → "88.5", 7,456 → "7.46".

    pivot_laden(pfad)
      Lädt Pivot-CSV: Index = Tilt (int), Spalten = Azimuth (int), absteigend sortiert.

    pivots_aus_ordner_laden(ordner)
      Sucht den neuesten PVcalc-Datensatz im Ordner und lädt alle zugehörigen
      Pivot-CSVs als Dictionary {"E_y": df, "E_m01": df, ..., "E_m12": df}.

    plot_speichern(fig, dateiname)
      Speichert Figure unter output/plots/{NAME}/ und schließt sie (Speicher).

    heatmap_achsen_formatieren(ax)
      Setzt einheitliche Achsenbeschriftungen auf allen Heatmap-Achsen.

  Plot-Funktionen:
    plot_jahresertrag(pivot, standort_info)
      Heatmap des Jahresertrags über alle 703 Tilt/Azimuth-Kombinationen.
      Optimum wird mit blauem Rechteck markiert. Farbschema: RdYlGn.
      → 01_heatmap_jahresertrag.png

    plot_alle_monate_gitter(pivots)
      4×3-Gitter mit allen 12 Monatsertrags-Heatmaps.
      Jede Zelle zeigt das Maximum mit blauem Rechteck.
      → 02_heatmap_alle_monate_gitter.png

    plot_saisonvergleich(pivots, monat_a, monat_b)
      Zwei Monatsertrags-Heatmaps nebeneinander für direkten Vergleich.
      Standard: Januar vs. Juni.
      → 03_saisonvergleich_jan_jun.png

    plot_faktor(pivots, monat_a, monat_b)
      Faktor-Heatmap: Monat A / Monat B.
      Grün (>1): Monat A ertragreicher, Rot (<1): Monat B ertragreicher.
      Normiert auf 1, Farbmittelpunkt bei 1.
      → 04_faktor_jan_jun.png

    plot_monat_einzeln(pivots, monat)
      Einzelne große Heatmap für einen bestimmten Monat.
      → 05_monat_{nr}_{name}.png

  Aufruf:
    python scripts/04_visualize.py
    python scripts/04_visualize.py --monat_a 12 --monat_b 6
    python scripts/04_visualize.py --monat_einzeln 3


scripts/05_visualize_stats.py  — Statistik-Visualisierungen (Phase 2)
  Erstellt 7 Plots auf Basis der 16-Jahres-Statistiken aus 03_analyze.py.
  Ausgabe: output/plots/{NAME}/stats/

  Gruppe 1 — Jahres-Heatmaps:
    plot_jahresertrag_mean(df_jahr)
      Heatmap des 16-Jahres-Mittels E_y_mean.
      Optimum blau markiert.
      → 01_jahresertrag_mean.png

    plot_jahresertrag_std(df_jahr)
      Heatmap der Standardabweichung E_y_std.
      Farbschema YlOrRd: gelb = stabiler Ertrag, rot = hohe Schwankung.
      → 02_jahresertrag_std.png

    plot_variationskoeffizient(df_jahr)
      VK = std/mean × 100 % als Heatmap.
      Zeigt relative interannuelle Variabilität (unabhängig vom absoluten Ertrag).
      → 03_jahresertrag_vk.png

  Gruppe 2 — Tagesverläufe:
    plot_tagesverlauf_optimal(df_gesamt, opt_tilt, opt_azi)
      Mittlerer Jahres-Tagesverlauf der optimalen Kombination.
      Linie (P_mean) + Streuband (±1σ).
      → 04_tagesverlauf_optimal.png

    plot_tagesverlauf_jan_jun(opt_tilt, opt_azi)
      Jan vs. Jun für 4 Orientierungen nebeneinander:
      Süd-optimal, Ost (35°/−90°), West (35°/+90°), Flach (10°/0°).
      → 05_tagesverlauf_jan_jun.png

    plot_tagesverlauf_12monate(opt_tilt, opt_azi)
      3×4-Gitter mit allen 12 Monaten für die optimale Kombination.
      Linie + ±1σ-Band + gestrichelte Min/Max-Linien über 16 Jahre.
      → 06_tagesverlauf_12monate.png

  Gruppe 3 — Monatserträge:
    plot_monatsertraege_fehlerbalken(df_monat, opt_tilt, opt_azi)
      Gruppiertes Balkendiagramm (mean ± std) für 4 Orientierungen.
      → 07_monatsertraege_fehlerbalken.png

  Hilfsfunktionen:
    optimum_finden(df, wert)
      Gibt (tilt, azimuth, wert) der Kombination mit dem höchsten Mittelwert zurück.

  Aufruf:
    python scripts/05_visualize_stats.py
    python scripts/05_visualize_stats.py --tilt 35 --azi 0


scripts/06_visualize_objekt.py  — Objektspezifische Visualisierungen
  Berechnet Ertragsprognosen für eine konkrete PV-Anlage mit mehreren Strings
  aus unterschiedlichen Tilt/Azimuth-Ausrichtungen. Jeder String wird mit seiner
  tatsächlichen kWp-Leistung skaliert.

  Konfiguration (direkt im Skript anzupassen):
    OBJEKT_NAME      Name der Anlage (für Ordner und Titel)
    STRINGS          Liste mit Strings: Name, Modulanzahl, kWp/Modul, Tilt, Azimuth

  Hilfsfunktionen:
    snap(wert, werte_liste)
      Nächster Rasterpunkt: findet den nächsten verfügbaren Tilt/Azimuth-Wert
      im PVGIS-Raster (bei Gleichstand: kleinerer Wert).

    strings_vorbereiten()
      Ergänzt jeden String um kwp_gesamt und auf das Raster gerasterte Werte
      (tilt_grid, azi_grid).

    sub_laden(df, tilt, azimuth)
      Filtert DataFrame auf eine Tilt/Azimuth-Kombination und sortiert nach Stunde.

    gesamt_berechnen(strings_data)
      Summiert skalierte Leistung aller Strings:
        P_mean_ges = Σ(P_mean_i × kWp_i)
        P_std_ges  = Σ(P_std_i  × kWp_i)  [konservativ, da gleicher Standort]

    string_label(s, kurz=False)
      Erzeugt beschrifteten Label mit allen String-Parametern inkl. Hinweis
      auf gerastertes Optimum wenn Originalwerte abweichen.

  Plot-Funktionen:
    plot_tagesverlauf_gesamt(strings)
      Jahresschnitt: Einzelne Strings (skalierte W) + Gesamt (mean ± std).
      → 01_tagesverlauf_gesamt.png

    plot_tagesverlauf_12monate(strings)
      3×4-Gitter, alle 12 Monate. Einzelne Strings + Gesamt-Band.
      Gemeinsame Y-Achse über alle Monate.
      → 02_tagesverlauf_12monate.png

    plot_tagesertraege_365(strings)
      Täglicher Ertrag (kWh absolut) von Tag 1 bis 365.
      Monatsmarkierungen auf der X-Achse.
      → 03_tagesertraege_365.png

    plot_monatsertraege(strings)
      Gruppiertes Balkendiagramm je String + Gesamt mit Fehlerbalken.
      → 04_monatsertraege.png

  Aufruf:
    python scripts/06_visualize_objekt.py
    (Konfiguration direkt im Skript, Abschnitt "Anlagenkonfiguration")

  Ausgabe: output/plots/{NAME}/objekt/{OBJEKT_NAME}/


TECHNISCHER STACK
-----------------
  requests       HTTP-Abfragen gegen die PVGIS API
  pandas         Datenverarbeitung, Pivot-Tabellen, CSV-Export
  duckdb         Analytische Datenbank für 61 Mio. Zeilen Zeitreihendaten
  tqdm           Fortschrittsbalken für lange API-Abfrageläufe
  matplotlib     Grafiken (Heatmaps, Linienplots, Balkendiagramme)
  seaborn        Heatmap-Rendering auf Basis von matplotlib
  openpyxl       Optional: Excel-Export
  pyarrow        Optional: Parquet-Unterstützung
  argparse       CLI-Argumente für alle Skripte
  watchdog       Optional: Datei-Watcher für lokale Automatisierung


ABHÄNGIGKEITEN INSTALLIEREN
----------------------------
  pip install -r requirements.txt


TYPISCHER WORKFLOW
------------------
  Schritt 1 — Standort konfigurieren:
    config.py anpassen: LAT, LON, NAME, ggf. PVCALC_YEAR

  Schritt 2 — Phase 1 (schnell, ~12 Minuten):
    python scripts/01_fetch_pvcalc.py

  Schritt 3 — Heatmaps aus Phase 1:
    python scripts/04_visualize.py

  Schritt 4 — Phase 2 (ausführlich, ~1–2 Stunden):
    python scripts/02_fetch_seriescalc.py

  Schritt 5 — Statistische Auswertungen:
    python scripts/03_analyze.py

  Schritt 6 — Statistik-Visualisierungen:
    python scripts/05_visualize_stats.py

  Schritt 7 — Objektspezifische Analyse (optional):
    STRINGS in 06_visualize_objekt.py anpassen
    python scripts/06_visualize_objekt.py


DATENQUELLEN UND HINWEISE
--------------------------
  PVGIS API v5.2: https://re.jrc.ec.europa.eu/api/v5_2/
  Betreiber: European Commission, Joint Research Centre (JRC)
  Verfügbarer Zeitraum seriescalc: 2005–2020
  Systemverluste (loss=14 %): Wechselrichter, Verkabelung, Verschmutzung usw.
  Alle Erträge sind auf 1 kWp Anlagenleistung normiert (spezifischer Ertrag).
  Für absolute Ertragswerte: Wert × installierte Anlagenleistung in kWp.

  Rate-Limiting: Die PVGIS API verlangt keine Authentifizierung, reagiert aber
  empfindlich auf zu viele Anfragen. REQUEST_DELAY = 0,7 s ist ein sicherer Wert.
  Bei Fehler 429 (Too Many Requests) den Delay auf 2–3 s erhöhen.


PROJEKTSTRUKTUR (REPO)
----------------------
  pvgis-solar-ertrag-visualizer/
  ├── config.py                   Zentrale Konfiguration  ← hier Standort eintragen
  ├── requirements.txt            Python-Abhängigkeiten
  ├── readme.txt                  Diese Datei
  ├── .gitignore
  ├── scripts/
  │   ├── 01_fetch_pvcalc.py      Phase 1: PVcalc-Daten abrufen
  │   ├── 02_fetch_seriescalc.py  Phase 2: Zeitreihendaten abrufen
  │   ├── 03_analyze.py           Statistische Auswertungen
  │   ├── 04_visualize.py         Heatmaps und Saisonplots
  │   ├── 05_visualize_stats.py   Statistik-Visualisierungen
  │   └── 06_visualize_objekt.py  Anlagen-spezifische Prognose
  ├── img/                        Beispielgrafiken (Blogbeitrag)
  ├── index.html                  Blogbeitrag (GitHub Pages)
  └── style.css

  Lokal beim Ausführen erzeugt (nicht im Repo):
  ├── data/raw/                   JSON-Rohdaten-Cache (PVcalc)
  ├── data/processed/             DuckDB + CSV-Dateien
  └── output/plots/               Erzeugte Grafiken


ERZEUGTE GRAFIKEN — ÜBERSICHT
------------------------------
  Phase 1 (04_visualize.py), output/plots/{NAME}/:
    01_heatmap_jahresertrag.png       Jahresertrag aller 703 Kombinationen
    02_heatmap_alle_monate_gitter.png Alle 12 Monate im 4×3-Gitter
    03_saisonvergleich_jan_jun.png    Januar vs. Juni nebeneinander
    04_faktor_jan_jun.png             Faktor-Heatmap Jan/Jun

  Phase 2 (05_visualize_stats.py), output/plots/{NAME}/stats/:
    01_jahresertrag_mean.png          16-Jahres-Mittel Jahresertrag
    02_jahresertrag_std.png           Standardabweichung Jahresertrag
    03_jahresertrag_vk.png            Variationskoeffizient
    04_tagesverlauf_optimal.png       Tagesverlauf optimale Kombination
    05_tagesverlauf_jan_jun.png       Jan vs. Jun, 4 Orientierungen
    06_tagesverlauf_12monate.png      Alle 12 Monate im Gitter
    07_monatsertraege_fehlerbalken.png Monatserträge mit Fehlerbalken

  Objekt (06_visualize_objekt.py), output/plots/{NAME}/objekt/{OBJEKT_NAME}/:
    01_tagesverlauf_gesamt.png        Jahresschnitt je String + Gesamt
    02_tagesverlauf_12monate.png      12 Monate je String + Gesamt
    03_tagesertraege_365.png          Tägliche Erträge Tag 1–365
    04_monatsertraege.png             Monatserträge mit Fehlerbalken
