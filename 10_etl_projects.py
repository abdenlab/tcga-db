import duckdb
import polars as pl

conn = duckdb.connect(':memory:')
df = conn.read_json("download/projects.json", format="array").pl()

df = df.select(
    "id",
    "name",
    df['program'].struct['program_id'],
    df['program'].struct['name'].alias("program_name"),
    "disease_type",
    "primary_site",
    df['summary'].struct['case_count'],
    df['summary'].struct['file_count'],
    df['summary'].struct['file_size'],
    # df['summary'].struct['data_categories'],
    # df['summary'].struct['experimental_strategies'],
    "released",
    "state",
)

df.write_parquet("db/projects.pq")
