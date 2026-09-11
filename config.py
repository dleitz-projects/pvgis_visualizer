# =============================================================================
# Zentrale Konfiguration für den PVGIS Solarertrag Visualizer
# =============================================================================

# --- Standort ---
LAT  = 48.137     # Breitengrad  ← anpassen
LON  = 11.576     # Längengrad   ← anpassen
NAME = "Standort_1"  # Kurzer Name für Ordner und Dateinamen (keine Leerzeichen)

# Standortangabe für Grafiktitel: bewusst auf eine Nachkommastelle gerundet
# (rund 10 km Raster), damit die Abbildungen keine genaue Adresse preisgeben.
STANDORT_ANZEIGE = f"{LAT:.1f}°N, {LON:.1f}°E".replace(".", ",")

# --- Tilt/Azimuth-Raster ---
TILT_MIN   = 0
TILT_MAX   = 90
TILT_STEP  = 5   # → 19 Werte: 0, 5, 10, ..., 90

AZI_MIN    = -180
AZI_MAX    = 180
AZI_STEP   = 10  # → 37 Werte: -180, -170, ..., 180

# Abgeleitet (nicht ändern)
TILTS    = list(range(TILT_MIN, TILT_MAX + 1, TILT_STEP))    # 19 Werte
AZIMUTHS = list(range(AZI_MIN,  AZI_MAX  + 1, AZI_STEP))     # 37 Werte
# Gesamt: 703 Kombinationen

# --- PVcalc Parameter ---
PEAKPOWER    = 1    # kWp (Ergebnisse immer pro 1 kWp)
LOSS         = 14   # Systemverluste in %
PVCALC_YEAR  = 2023 # Letztes verfügbares volles Jahr

# --- seriescalc Parameter (Phase 2) ---
SERIES_STARTYEAR = 2005
SERIES_ENDYEAR   = 2023  # PVGIS v5.3 / SARAH3 liefert 2005–2023 (19 Jahre)

# --- API ---
# v5.3 nutzt die Strahlungsdatenbank SARAH3 und reicht bis 2023.
# (v5.2 mit SARAH2 endete bei 2020.)
PVGIS_BASE_URL    = "https://re.jrc.ec.europa.eu/api/v5_3"
PVCALC_ENDPOINT   = f"{PVGIS_BASE_URL}/PVcalc"
SERIES_ENDPOINT   = f"{PVGIS_BASE_URL}/seriescalc"

# --- Rate-Limiting ---
REQUEST_DELAY = 0.7  # Sekunden zwischen API-Calls

# --- Pfade (standortspezifisch über NAME) ---
DATA_RAW_DIR       = f"data/raw/{NAME}"
DATA_PROCESSED_DIR = f"data/processed/{NAME}"
OUTPUT_DIR         = "output"
PLOTS_DIR          = f"output/plots/{NAME}"

# =============================================================================
# Strommarkt (Spotpreise, Erzeugung, Marktwerte)
# =============================================================================

# Datenquelle: Energy-Charts API des Fraunhofer ISE (Basis: SMARD/Bundesnetzagentur)
# Kein API-Schlüssel nötig, Lizenz CC BY 4.0.
ENERGY_CHARTS_BASE = "https://api.energy-charts.info"

# Gebotszonen mit Gültigkeitszeitraum: bis zur Trennung von Österreich
# (30.09.2018) hieß die deutsche Zone DE-AT-LU, danach DE-LU.
PREIS_ZONEN = [
    ("DE-AT-LU", "2015-01-01", "2018-09-30"),
    ("DE-LU",    "2018-10-01", None),          # None = bis heute
]

# Erzeugungszeitreihen (für den selbst berechneten Marktwert Solar)
ERZEUGUNG_ZONE = "DE"

# Zeitfenster für Auswertungen: PV-Ertrag als Langzeitmittel über alle Jahre,
# Strompreise nur aus den letzten N vollständigen Jahren.
PREIS_ANALYSE_JAHRE = 3

# Pfade Strommarkt (standortunabhängig)
MARKT_DIR        = "data/markt"
MARKT_DB         = f"{MARKT_DIR}/strommarkt.duckdb"
