# Input data

Raw files aren't committed. Put them in this folder (or point `DATA_DIR` in `.env` somewhere else) with these exact names:

| File | Source | Format | What it holds |
|------|--------|--------|---------------|
| `Supplier Transactions XML.xml` | Course dataset (WideWorldImporters-style) | XML | Supplier invoices: amounts, tax, PO ID, dates |
| `Monthly PO Data/*.csv` | Course dataset | CSV, one file per month | Purchase order lines: outers received, expected unit price |
| `supplier_case.pgsql` | Course dataset | PostgreSQL dump | Supplier master data, including postal codes |
| `2021_Gaz_zcta_national.txt` | [U.S. Census Bureau Gazetteer Files](https://www.census.gov/geographies/reference-files/time-series/geo/gazetteer-files.html) (2021 ZCTA) | Tab-delimited | ZIP code (ZCTA) centroid lat/long and area |

The weather data isn't a file. It's read directly from the **NOAA weather** listing on the Snowflake Marketplace (`NOAA_WEATHER_STATION_INDEX` and `NOAA_WEATHER_METRICS_TIMESERIES`), which your Snowflake account must be subscribed to.

`supplier_case.csv` is written here by the pipeline when it exports from PostgreSQL.
