import duckdb
import polars as pl

conn = duckdb.connect(':memory:')


# Files Table
df = conn.read_json("download/files.json", format="array").pl()


files = df.select([
    "id", # "file_id",
    "submitter_id",
    "file_name",
    "file_size",
    "md5sum",
    "type",
    "data_category",
    "data_type",
    "data_format",
    "experimental_strategy",
    "platform",
    "access",
    "state",
    "created_datetime",
    "updated_datetime",
    pl.col("associated_entities").map_elements(
        lambda x: [s["entity_id"] for s in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("entity_ids"),
    pl.col("associated_entities").map_elements(
        lambda x: [s["entity_submitter_id"] for s in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("entity_submitter_ids"),
    pl.col("associated_entities").map_elements(
        lambda x: [s["entity_type"] for s in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("entity_types"),
    pl.col("cases").map_elements(
        lambda x: [s["case_id"] for s in x],
    ).alias("case_ids"),
    pl.col("cases").map_elements(
        lambda x: [s["submitter_id"] for s in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("submitter_case_ids"),
    pl.col("analysis").struct['analysis_id'],
    pl.col("analysis").struct['submitter_id'].alias("submitter_analysis_id"),
    pl.col("index_files").map_elements(
        lambda x: [s["file_id"] for s in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("index_file_ids"),
    pl.col("index_files").map_elements(
        lambda x: [s["file_name"] for s in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("index_file_names"),
])


files.write_parquet("db/files.pq")
