# =============================================================================
# Zentrale Konfiguration für den PVGIS Solarertrag Visualizer
# =============================================================================

# --- Standort ---
LAT  = 48.137     # Breitengrad  ← anpassen
LON  = 11.576     # Längengrad   ← anpassen
NAME = "Standort_1"  # Kurzer Name für Ordner und Dateinamen (keine Leerzeichen)

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
SERIES_ENDYEAR   = 2020  # PVGIS seriescalc unterstützt max. 2005–2020 (16 Jahre)

# --- API ---
PVGIS_BASE_URL    = "https://re.jrc.ec.europa.eu/api/v5_2"
PVCALC_ENDPOINT   = f"{PVGIS_BASE_URL}/PVcalc"
SERIES_ENDPOINT   = f"{PVGIS_BASE_URL}/seriescalc"

# --- Rate-Limiting ---
REQUEST_DELAY = 0.7  # Sekunden zwischen API-Calls

# --- Pfade (standortspezifisch über NAME) ---
DATA_RAW_DIR       = f"data/raw/{NAME}"
DATA_PROCESSED_DIR = f"data/processed/{NAME}"
OUTPUT_DIR         = "output"
PLOTS_DIR          = f"output/plots/{NAME}"
