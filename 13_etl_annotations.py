import duckdb
import polars as pl

conn = duckdb.connect(':memory:')


# Annotation Table
df = conn.read_json("download/annotations.json", format="array").pl()

df.select(
    "id",
    "submitter_id",
    "case_id",
    "case_submitter_id",
    "entity_id",
    "entity_submitter_id",
    "entity_type",
    "category",
    "classification",
    "notes",
    "created_datetime",
    "updated_datetime",
    # "legacy_created_datetime",
    # "legacy_updated_datetime",
    "state",
    "status",
    # pl.col("project").struct["code"].alias("project_code"),
).write_parquet("db/annotations.pq")
