#!/bin/bash
# Wartet auf das Ende des laufenden PVGIS-Abrufs und startet danach die
# vollständige Erlösauswertung samt Grafiken.
cd "$(dirname "$0")/.."
while pgrep -f 02_fetch_seriescalc.py > /dev/null; do sleep 60; done
echo "PVGIS-Abruf beendet: $(date)"
# Statistiken (Jahres-, Monats-, Tageswerte) auf die neuen PV-Daten aktualisieren
/usr/bin/python3 scripts/03_analyze.py
/usr/bin/python3 wirtschaftlichkeit/12_analyze_erloese.py --profil-neu
/usr/bin/python3 wirtschaftlichkeit/13_visualize_erloese.py
/usr/bin/python3 vertikal/07_vertikal_bifazial.py
/usr/bin/python3 vertikal/08_vertikal_erloese_preisjahre.py
echo "Fertig: $(date)"
