# NYC Taxi Lakehouse

Databricks Unity Catalog pipeline for [NYC TLC Yellow Taxi](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) trip data. Public monthly Parquet files and the taxi-zone lookup are landed in a volume, then processed through a medallion architecture into Delta tables for analysis.

TLC publishes trip files with a lag, so the **incremental** path always targets the month **two months prior** to the run date.

## Architecture

Catalog: `nyctaxi`

| Schema | Layer | What it stores |
| --- | --- | --- |
| `00_landing` | Files | Volume `data_sources` (raw Parquet / CSV) |
| `01_bronze` | Raw | Yellow trips as ingested, plus `processed_timestamp` |
| `02_silver` | Conformed | Cleansed trips, zone lookup (SCD Type 2), enriched trips with borough/zone names |
| `03_gold` | Serving | Daily trip and revenue summary |

```
TLC CloudFront
        │
        ▼
00_landing (volume)
        │
        ▼
01_bronze.yellow_trips_raw
        │
        ▼
02_silver.yellow_trips_cleansed  +  02_silver.taxi_zone_lookup
        │
        ▼
02_silver.yellow_trips_enriched
        │
        ▼
03_gold.daily_trip_summary
```

### Landing paths

- Trips: `/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{yyyy-MM}/yellow_tripdata_{yyyy-MM}.parquet`
- Zones: `/Volumes/nyctaxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv`

### Source URLs

- `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{yyyy-MM}.parquet`
- `https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv`

## Repository layout

```
nyctaxi_project/
├── modules/                         Shared Python helpers
│   ├── data_loader/file_downloader.py
│   ├── transformations/metadata.py
│   └── utils/date_utils.py
├── one_off/                         Run once: catalog + historical load
│   ├── creating_catalogs_schemas_volume.py
│   └── initial_load/notebooks/
├── transformations/notebooks/       Recurring monthly pipeline
│   ├── 00_landing/
│   ├── 01_bronze/
│   ├── 02_silver/
│   └── 03_gold/
└── ad_hoc/                          Analysis and cleanup notebooks
```

Notebooks are Databricks notebooks. Incremental notebooks add the project root to `sys.path` so they can import `modules`. Attach this repo in the Databricks workspace (or a similar layout) before running jobs.

## Prerequisites

- Databricks workspace with Unity Catalog
- Cluster or SQL warehouse that can write to the `nyctaxi` catalog
- Network access to TLC CloudFront
- Python packages used by the notebooks: `pyspark`, `python-dateutil`

## One-time setup

### 1. Create catalog, schemas, and volume

Edit `one_off/creating_catalogs_schemas_volume.py` and set a valid managed location, then run it. It creates:

- Catalog `nyctaxi`
- Schemas `00_landing`, `01_bronze`, `02_silver`, `03_gold`
- Volume `nyctaxi.00_landing.data_sources`

### 2. Historical backfill

Update the month list in `one_off/initial_load/notebooks/00_landing/backfill_historical_yellow_trips.py` (comment in the file: about six months of data, ending three months before today). Also update the pickup-date filter in `one_off/initial_load/notebooks/02_silver/yellow_trips_cleansed.py` so it matches that range.

Run notebooks in this order:

1. `00_landing/backfill_historical_yellow_trips.py`
2. `00_landing/load_taxi_zone_lookup.py`
3. `01_bronze/yellow_trips_raw.py`
4. `02_silver/taxi_zone_lookup.py`
5. `02_silver/yellow_trips_cleansed.py`
6. `02_silver/yellow_trips_enriched.py`
7. `03_gold/daily_trip_summary.py`

Initial-load notebooks **overwrite** the Delta tables.

## Recurring pipeline

Use `transformations/notebooks/` as a Databricks Job. Landing tasks set `dbutils.jobs.taskValues` key `continue_downstream` to `yes` or `no` so later tasks can skip when the month file already exists or the download failed.

Suggested task order:

1. `00_landing/ingest_yellow_trips.py` — download `{yyyy-MM}` two months ago if missing
2. `00_landing/ingest_lookup.py` — refresh zone CSV
3. `01_bronze/yellow_trips_raw.py` — append that month to bronze
4. `02_silver/taxi_zone_lookup.py` — SCD2 merge into zone lookup
5. `02_silver/yellow_trips_cleansed.py` — decode codes, trip duration, append
6. `02_silver/yellow_trips_enriched.py` — join pickup/dropoff zones, append
7. `03_gold/daily_trip_summary.py` — daily metrics, append

Incremental notebooks **append**. Re-running a month without a purge will duplicate rows. Use `ad_hoc/purge_tables_from_date.py` if you need to delete from a cutoff and reload.

### Silver transformations

Cleansed trips map TLC codes to labels (vendor, rate type, payment type), rename location/fee columns, and compute `trip_duration` in minutes.

Enriched trips left-join `taxi_zone_lookup` twice to add `pu_borough`, `pu_zone`, `do_borough`, and `do_zone`.

Zone lookup is maintained as SCD Type 2 (`effective_date` / `end_date`). Changed borough, zone, or service zone closes the current row and inserts a new version; new location IDs are inserted.

### Gold metrics

`03_gold.daily_trip_summary` is one row per pickup date:

- `total_trips`
- `average_passengers`
- `average_distance`
- `average_fare_per_trip`
- `max_fare` / `min_fare`
- `total_revenue`

## Ad-hoc notebooks

| Notebook | Purpose |
| --- | --- |
| `ad_hoc/yellow_taxi_eda.py` | Vendor revenue, popular pickup boroughs, borough-to-borough journeys, daily trips vs revenue |
| `ad_hoc/yellow_taxi_eda_2.py` | Record counts by `yyyy-MM` across layers (coverage check) |
| `ad_hoc/purge_tables_from_date.py` | Delete bronze/silver/gold rows from a start date before a reload |

## Shared modules

| Module | Role |
| --- | --- |
| `modules.data_loader.file_downloader.download_file` | HTTP download into a volume path |
| `modules.transformations.metadata.add_processed_timestamp` | Add `processed_timestamp` |
| `modules.utils.date_utils.get_target_yyyymm` | Year-month string `n` months ago (default 2) |
| `modules.utils.date_utils.get_month_start_n_months_ago` | First day of the month `n` months ago |

## License / data

Trip records are published by the NYC Taxi & Limousine Commission. This repository contains pipeline code only, not the trip files themselves.
