"""TNG Sky Map — HEALPix instrument map of 26 years of TNG observations.

Downloads observation metadata from the IA2 Virtual Observatory, caches it
locally in SQLite, and generates a HEALPix Mollweide sky map colored by the
dominant instrument at each sky position.

Usage:
    tng-sky-map                     # Generate sky_instruments.png
    tng-sky-map -o map.png          # Custom output path
    tng-sky-map --svg               # SVG output
    tng-sky-map --refresh           # Re-download data from VO
"""

import argparse
import datetime
import sqlite3
from collections import defaultdict
from pathlib import Path

import healpy as hp
import matplotlib.pyplot as plt
import numpy as np
import pyvo
from matplotlib.colors import BoundaryNorm, ListedColormap
from matplotlib.patches import Patch

# ── VO source ────────────────────────────────────────────────────────────────

IA2_TAP_URL = "http://archives.ia2.inaf.it/vo/tap/tng"

# ── Filter constants ─────────────────────────────────────────────────────────

EXCLUDED_PROGRAMS = ["CALIB", "TEST", "Test", "test", "???", "NULL", "None", "undefined"]
SOLAR_PROGRAMS = ["SOLAR", "GIANO-SOLAR"]

# ── TNG telescope constraints ───────────────────────────────────────────────

TNG_LATITUDE = 28.7567  # degrees N
TNG_MIN_ELEVATION = 12  # degrees
TNG_MIN_DEC = TNG_LATITUDE - (90 - TNG_MIN_ELEVATION)  # -49.24°
ZENITH_PARKING_DEC_MIN = TNG_LATITUDE - 2.0
ZENITH_PARKING_DEC_MAX = TNG_LATITUDE + 2.0
MAX_EXPTIME = 7200  # 2 hours — anything above is a data error

# Drift scan detection
DRIFT_DEC_TOL = 0.01  # degrees
DRIFT_RA_MIN = 5.0    # degrees
DRIFT_MIN_RUN = 5

# ── HEALPix ─────────────────────────────────────────────────────────────────

HEALPIX_NSIDE = 128  # ~27.5 arcmin

# ── Plot style ───────────────────────────────────────────────────────────────

plt.style.use("dark_background")

FACECOLOR = "#0a0a1a"
COLOR_TITLE = "white"
COLOR_LABEL = "#cccccc"
COLOR_TICK = "#cccccc"
COLOR_GRID = "white"
COLOR_GRID_ALPHA = 0.15
COLOR_CBAR_OUTLINE = "#333333"

INSTRUMENT_COLORS = {
    "LRS": "#e6194b",
    "NICS": "#3cb44b",
    "OIG": "#4363d8",
    "SRG": "#f58231",
    "SHB": "#911eb4",
    "SHA": "#42d4f4",
    "TKB": "#f032e6",
    "HARPN": "#ffe119",
    "GIANO": "#dcbeff",
    "GIANO-B": "#aaffc3",
    "SIFAP2": "#fabed4",
}

# ── Record indices ───────────────────────────────────────────────────────────

_DATE, _TIME, _RA, _DEC, _EXPTIME, _INST, _OBSTYPE, _PROG = range(8)

# ── Cache ────────────────────────────────────────────────────────────────────

CACHE_DIR = Path(__file__).resolve().parent / "cache"
CACHE_DB = CACHE_DIR / "observations.db"

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS observations (
    date TEXT, time TEXT, ra REAL, dec REAL, exptime REAL,
    instrument TEXT, obs_type TEXT, program TEXT
)"""
_CREATE_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_date ON observations(date)",
    "CREATE INDEX IF NOT EXISTS idx_program ON observations(program)",
    "CREATE INDEX IF NOT EXISTS idx_instrument ON observations(instrument)",
]
_INSERT = "INSERT INTO observations VALUES (?,?,?,?,?,?,?,?)"
_SELECT_ALL = "SELECT date, time, ra, dec, exptime, instrument, obs_type, program FROM observations ORDER BY date, time"


def _get_db():
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(CACHE_DB))
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute(_CREATE_TABLE)
    for idx in _CREATE_INDEXES:
        conn.execute(idx)
    return conn


def _fetch_vo_year(tap, year):
    job = tap.submit_job(f"""
        SELECT RA_RAD, DEC_RAD, DATE_OBS, EXPSTART, EXPTIME,
               INSTRUMENT, OBS_MODE, PROGRAM
        FROM tng.TNG_TAP
        WHERE OBS_MODE IN ('SCIENCE', 'OBJECT')
          AND RA_RAD IS NOT NULL AND DEC_RAD IS NOT NULL
          AND DATE_OBS >= '{year}-01-01' AND DATE_OBS < '{year + 1}-01-01'
        ORDER BY DATE_OBS, EXPSTART
    """)
    job.run()
    job.wait()
    result = job.fetch_result()

    records = []
    for row in result:
        ra = np.degrees(float(row["RA_RAD"]))
        dec = np.degrees(float(row["DEC_RAD"]))
        if not (0 <= ra <= 360 and -90 <= dec <= 90):
            continue
        date_obs = str(row["DATE_OBS"] or "")
        if "T" in date_obs:
            date_obs = date_obs.split("T")[0]
        expstart = str(row["EXPSTART"] or "")
        if "T" in expstart:
            expstart = expstart.split("T")[1][:8]
        try:
            exptime = float(row["EXPTIME"]) if row["EXPTIME"] is not None else 0.0
        except (ValueError, TypeError):
            exptime = 0.0
        records.append((
            date_obs, expstart, round(ra, 6), round(dec, 6), round(exptime, 2),
            str(row["INSTRUMENT"] or ""),
            str(row["OBS_MODE"] or "").upper(),
            str(row["PROGRAM"] or ""),
        ))
    job.delete()
    return records


def build_cache():
    print(f"Downloading observations from VO ({IA2_TAP_URL}) ...")
    tap = pyvo.dal.TAPService(IA2_TAP_URL)
    current_year = datetime.date.today().year
    all_records = []
    for year in range(2000, current_year + 1):
        try:
            records = _fetch_vo_year(tap, year)
            all_records.extend(records)
            print(f"  {year}: {len(records):>8,}")
        except Exception as e:
            print(f"  {year}: ERROR — {e}")

    conn = _get_db()
    conn.execute("DELETE FROM observations")
    conn.executemany(_INSERT, all_records)
    conn.commit()

    # Deduplicate multi-file exposures (HARPS-N ~20x, GIANO ~3x)
    before = len(all_records)
    conn.execute("DROP TABLE IF EXISTS _dedup")
    conn.execute("""
        CREATE TABLE _dedup AS
        SELECT date, time, ra, dec, exptime, instrument, obs_type, program
        FROM observations GROUP BY date, time, ra, dec, instrument
    """)
    conn.execute("DROP TABLE observations")
    conn.execute("ALTER TABLE _dedup RENAME TO observations")
    for idx in _CREATE_INDEXES:
        conn.execute(idx)
    conn.execute("VACUUM")
    conn.commit()
    after = conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
    conn.close()
    print(f"  Deduplicated: {before:,} -> {after:,} unique exposures.")
    return _load_cache()


def _load_cache():
    if not CACHE_DB.exists():
        return None
    conn = _get_db()
    records = conn.execute(_SELECT_ALL).fetchall()
    conn.close()
    if not records:
        return None
    print(f"Loaded {len(records):,} observations from cache.")
    return records


# ── Filters ──────────────────────────────────────────────────────────────────

def _filter_drift_scans(records):
    groups = defaultdict(list)
    for i, r in enumerate(records):
        groups[(r[_DATE], r[_INST])].append(i)
    drift = set()
    for indices in groups.values():
        if len(indices) < DRIFT_MIN_RUN:
            continue
        decs = [records[i][_DEC] for i in indices]
        ras = [records[i][_RA] for i in indices]
        s = 0
        while s < len(indices):
            e = s + 1
            while e < len(indices) and abs(decs[e] - decs[s]) < DRIFT_DEC_TOL:
                e += 1
            if e - s >= DRIFT_MIN_RUN and max(ras[s:e]) - min(ras[s:e]) > DRIFT_RA_MIN:
                drift.update(indices[s:e])
            s = e
    if drift:
        records = [r for i, r in enumerate(records) if i not in drift]
        print(f"  Drift scans: -{len(drift):,}")
    return records


def apply_filters(records):
    n = len(records)

    # Excluded programs
    excluded = set(EXCLUDED_PROGRAMS)
    records = [r for r in records if r[_PROG] not in excluded]
    d = n - len(records); n = len(records)
    if d: print(f"  Excluded programs: -{d:,}")

    # Solar
    records = [r for r in records if r[_PROG] not in SOLAR_PROGRAMS]
    d = n - len(records); n = len(records)
    if d: print(f"  Solar: -{d:,}")

    # Elevation limit
    records = [r for r in records if r[_DEC] >= TNG_MIN_DEC]
    d = n - len(records); n = len(records)
    if d: print(f"  Below elevation limit: -{d:,}")

    # Default coordinates
    records = [r for r in records if not (r[_RA] < 0.5 and abs(r[_DEC]) < 0.5)]
    d = n - len(records); n = len(records)
    if d: print(f"  Default coordinates: -{d:,}")

    # Drift scans
    records = _filter_drift_scans(records)

    # Zenith parking
    n2 = len(records)
    records = [r for r in records if not (
        r[_PROG] == "NONE" and ZENITH_PARKING_DEC_MIN <= r[_DEC] <= ZENITH_PARKING_DEC_MAX
    )]
    d = n2 - len(records)
    if d: print(f"  Zenith parking: -{d:,}")

    print(f"  Result: {len(records):,} observations.")
    return records


# ── HEALPix rendering ───────────────────────────────────────────────────────

def _healpix_to_grid(skymap):
    nside = hp.npix2nside(len(skymap))
    step = max(hp.nside2resol(nside, arcmin=True) / 60 / 2, 0.25)
    n_lon, n_lat = int(360 / step), int(180 / step)
    lon_e = np.linspace(-np.pi, np.pi, n_lon + 1)
    lat_e = np.linspace(-np.pi / 2, np.pi / 2, n_lat + 1)
    lon_c = np.degrees(0.5 * (lon_e[:-1] + lon_e[1:]))
    lat_c = np.degrees(0.5 * (lat_e[:-1] + lat_e[1:]))
    ra_g, dec_g = np.meshgrid(np.mod(-lon_c, 360), lat_c)
    values = skymap[hp.ang2pix(nside, ra_g, dec_g, lonlat=True)]
    ra_grid, dec_grid = np.meshgrid(lon_e, lat_e)
    return ra_grid, dec_grid, values


def _style_ax(ax):
    ax.grid(True, color=COLOR_GRID, alpha=COLOR_GRID_ALPHA, linewidth=0.5)
    ax.set_xticklabels(
        ["10h", "8h", "6h", "4h", "2h", "0h", "22h", "20h", "18h", "16h", "14h"],
        fontsize=9, color=COLOR_TICK, fontweight="light",
    )
    ax.tick_params(axis="y", labelsize=9, labelcolor=COLOR_TICK)


# ── Main ─────────────────────────────────────────────────────────────────────

def generate_map(records, output_path="sky_instruments.png", svg=False):
    ra = np.array([r[_RA] for r in records])
    dec = np.array([r[_DEC] for r in records])
    instruments = np.array([r[_INST] for r in records])

    npix = hp.nside2npix(HEALPIX_NSIDE)
    unique = sorted(set(i for i in instruments if i))
    idx_map = {name: idx for idx, name in enumerate(unique)}

    counts = np.zeros((len(unique), npix), dtype=int)
    for i, inst in enumerate(instruments):
        if inst and inst in idx_map:
            pix = hp.ang2pix(HEALPIX_NSIDE, ra[i], dec[i], lonlat=True)
            counts[idx_map[inst], pix] += 1

    total = counts.sum(axis=0)
    dominant = np.argmax(counts, axis=0).astype(float) + 1
    dominant[total == 0] = np.nan

    ra_grid, dec_grid, values = _healpix_to_grid(dominant)

    colors = ["#000000"] + [INSTRUMENT_COLORS.get(i, "#ffffff") for i in unique]
    cmap = ListedColormap(colors)
    norm = BoundaryNorm(np.arange(-0.5, len(unique) + 1.5, 1), cmap.N)

    fig = plt.figure(figsize=(16, 9), facecolor=FACECOLOR)
    ax = fig.add_subplot(111, projection="mollweide", facecolor=FACECOLOR)
    ax.pcolormesh(ra_grid, dec_grid, values, cmap=cmap, norm=norm, shading="flat")
    _style_ax(ax)
    ax.set_title("TNG Sky Survey — Instruments", fontsize=18, fontweight="bold", color=COLOR_TITLE, pad=24)

    patches = []
    for inst in unique:
        idx = idx_map[inst]
        n_obs = int(counts[idx].sum())
        n_pix = int(np.sum(counts[idx] > 0))
        if n_obs > 0:
            patches.append(Patch(
                facecolor=INSTRUMENT_COLORS.get(inst, "#ffffff"), edgecolor="none",
                label=f"{inst} ({n_obs:,} obs, {n_pix:,} pix)",
            ))
    ax.legend(
        handles=patches, loc="lower center", bbox_to_anchor=(0.5, -0.15),
        ncol=min(len(patches), 6), frameon=False, fontsize=9, labelcolor=COLOR_LABEL,
    )

    plt.tight_layout()
    fmt = "svg" if svg else None
    fig.savefig(output_path, dpi=200, bbox_inches="tight",
                facecolor=fig.get_facecolor(), edgecolor="none", format=fmt)
    plt.close(fig)

    observed = np.sum(total > 0)
    coverage = observed / npix * 100
    print(f"Map saved to {output_path}")
    print(f"  {len(records):,} observations, {observed:,}/{npix:,} pixels ({coverage:.1f}% sky)")


def main():
    parser = argparse.ArgumentParser(
        description="Generate a HEALPix instrument sky map from TNG observations",
    )
    parser.add_argument("-o", "--output", default="sky_instruments.png", help="Output file (default: sky_instruments.png)")
    parser.add_argument("--svg", action="store_true", help="SVG output")
    parser.add_argument("--refresh", action="store_true", help="Re-download data from VO")
    args = parser.parse_args()

    records = None
    if not args.refresh:
        records = _load_cache()
    if records is None:
        records = build_cache()

    print("Applying filters...")
    records = apply_filters(records)

    output = args.output
    if args.svg:
        output = str(Path(output).with_suffix(".svg"))

    generate_map(records, output_path=output, svg=args.svg)


if __name__ == "__main__":
    main()
