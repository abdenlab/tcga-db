#!/usr/bin/env bash

mkdir -p schemas download db

# Download schemas and GDC data from API
uv run python 00_fetch_schemas.py
uv run python 01_fetch_projects.py -o download/projects.json
uv run python 02_fetch_cases_clinical.py -o download/cases.json
uv run python 03_fetch_cases_samples.py -o download/cases_samples.json
uv run python 04_fetch_annotations.py -o download/annotations.json
uv run python 05_fetch_files.py -o download/files.json

# Generate parquet files
uv run python 10_etl_projects.py
uv run python 11_etl_cases_clinical.py
uv run python 12_etl_cases_samples.py
uv run python 13_etl_annotations.py
uv run python 14_etl_files.py
uv run python 15_etl_codebook.py
