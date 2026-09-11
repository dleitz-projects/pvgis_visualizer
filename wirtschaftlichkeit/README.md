# Wirtschaftlichkeit: Einspeiseerlöse am Spotmarkt

Verknüpft die stündlichen PV-Zeitreihen aus PVGIS mit den historischen
Day-Ahead-Spotpreisen und beantwortet damit die Frage, welche Ausrichtung nicht
den höchsten Ertrag, sondern den höchsten Erlös bringt. Relevant für
Großprojekte in der Direktvermarktung, wo nicht die Kilowattstunde zählt,
sondern wann sie eingespeist wird.

## Datenquellen

| Quelle | Inhalt | Zugang |
|--------|--------|--------|
| [Energy-Charts API](https://api.energy-charts.info), Fraunhofer ISE | Day-Ahead-Spotpreise, Erzeugung je Energieträger, Last | frei, kein Schlüssel, CC BY 4.0, Datenbasis SMARD |
| [PVGIS v5.3](https://re.jrc.ec.europa.eu/pvg_tools/de/) | stündliche PV-Leistung 2005–2023 (SARAH3) | frei, kein Schlüssel |
| [netztransparenz.de](https://www.netztransparenz.de) | amtliche Monatsmarktwerte der Übertragungsnetzbetreiber | CSV-Download von Hand, optionaler Import |

Gebotszonen: bis 30.09.2018 DE-AT-LU, danach DE-LU. Beide werden geladen und in
derselben Tabelle abgelegt.

## Ablauf

```bash
# 1. Strommarktdaten laden (2015 bis gestern, monatsweise, wiederaufnehmbar)
python wirtschaftlichkeit/10_fetch_strommarkt.py

# 2. Monatsmarktwerte berechnen (erzeugungsgewichtete Spotpreise)
python wirtschaftlichkeit/11_marktwerte.py

# 3. Erlöse je Tilt/Azimuth berechnen
python wirtschaftlichkeit/12_analyze_erloese.py

# 4. Grafiken erzeugen
python wirtschaftlichkeit/13_visualize_erloese.py
```

Voraussetzung für Schritt 3 sind die PV-Zeitreihen aus
`scripts/02_fetch_seriescalc.py`. Das Skript nimmt automatisch die
seriescalc-Datenbank mit dem jüngsten Endjahr.

## Methodik

**Typjahr statt Einzeljahr.** Aus allen PVGIS-Jahren wird je Kombination und je
Stunde des Jahres der Mittelwert der Leistung gebildet. Damit fällt die
Wetterschwankung einzelner Jahre heraus.

**Nur aktuelle Preisjahre.** Die Erlösrechnung nutzt standardmäßig die letzten
drei vollständigen Kalenderjahre. Preisniveau und Mittagsdelle haben sich seit
2021 stark verändert, ältere Jahre würden das Bild verfälschen. Einstellbar über
`PREIS_ANALYSE_JAHRE` in `config.py` oder `--jahre`.

**Die Datenbank bleibt vollständig.** Preise und Erzeugung werden ab 2015
lückenlos geladen, unabhängig vom Auswertungsfenster. Andere Zeiträume lassen
sich damit später ohne erneuten Download auswerten.

**Kennzahlen je Kombination und Preisjahr:**

| Größe | Bedeutung |
|-------|-----------|
| `e_kwh` | Jahresertrag des Typjahrs in kWh/kWp |
| `erloes_eur` | Erlös in EUR/kWp und Jahr, Summe aus Stundenertrag mal Stundenpreis |
| `erloespreis_eur_mwh` | Erlös je erzeugter MWh |
| `basispreis_eur_mwh` | ungewichteter Mittelwert aller Spotpreise des Jahres |
| `faktor` | Capture-Faktor, Erlöspreis geteilt durch Basispreis |
| `anteil_negativ_prozent` | Anteil der Erzeugung in Stunden mit negativem Preis |

Der Capture-Faktor ist die eigentliche Kennzahl für die Ausrichtungsfrage. Ein
Wert unter 1 heißt, dass die Anlage überdurchschnittlich dann einspeist, wenn
der Strom wenig wert ist.

## Zeitzonen

Die Marktdatenbank speichert UTC. PVGIS-Zeitstempel sind ebenfalls UTC, der
Join erfolgt also konsistent. Nur die Monatsabgrenzung der Marktwerte in
`11_marktwerte.py` rechnet auf Europe/Berlin um, weil die amtlichen Marktwerte
so abgegrenzt sind.

## Amtliche Marktwerte

Die Monatsmarktwerte der Übertragungsnetzbetreiber sind nicht ohne Anmeldung
per API abrufbar. Sie lassen sich als CSV von netztransparenz.de herunterladen
(Erneuerbare Energien und Umlagen → EEG → Transparenzanforderungen →
Marktprämie → Marktwertübersicht) und importieren:

```bash
python wirtschaftlichkeit/11_marktwerte.py --netztransparenz-csv ~/Downloads/Marktwerte.csv
```

Sie landen in der Tabelle `marktwerte_offiziell` und dienen als Gegenprobe zu
den selbst berechneten Werten in `marktwerte`.

## Tabellen in `data/markt/strommarkt.duckdb`

| Tabelle | Inhalt |
|---------|--------|
| `spotpreise` | ts_utc, zone, preis_eur_mwh, aufloesung_min |
| `erzeugung` | ts_utc, land, art, leistung_mw, aufloesung_min |
| `marktwerte` | monat, art, marktwert, basispreis, faktor (berechnet) |
| `marktwerte_offiziell` | monat, art, marktwert (importiert) |
| `fetch_log_markt` | geladene Monate je Datensatz, für Wiederaufnahme |
