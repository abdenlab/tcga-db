import polars as pl

df = pl.read_csv("codebook/diseaseStudy.tsv", separator="\t")
df.select(
    pl.col("Study Abbreviation").alias("code"),
    pl.col("Study Name").alias("name"),
).write_parquet("db/studies.pq")

df = pl.read_csv("codebook/centerCode.tsv", separator="\t")
df.select(
    pl.col("Code").alias("code"),
    pl.col("Short Name").alias("short_name"),
    pl.col("Center Name").alias("domain_name"),
    pl.col("Display Name").alias("display_name"),
    pl.col("Center Type").alias("center_type"),
).write_parquet("db/centers.pq")

df = pl.read_csv("codebook/tissueSourceSite.tsv", separator="\t")
df = df.select(
    pl.col("TSS Code").alias("code"),
    pl.col("Source Site").alias("name"),
    pl.col("Study Name").alias("study_name"),
    pl.col("BCR").alias("bcr_id"),
)
studies = pl.read_parquet("db/studies.pq").select(
    pl.col("name").alias("study_name"),
    pl.col("code").alias("study_code"),
)
df = df.join(
    studies,
    on="study_name",
    how="left"
).select(["code", "name", "study_code", "study_name", "bcr_id"])
df.write_parquet("db/tissue_source_sites.pq")

df = pl.read_csv("codebook/bcrBatchCode.tsv", separator="\t")
df.select(
    pl.col("BCR Batch").alias("code"),
    pl.col("Study Abbreviation").alias("study_code"),
    pl.col("Study Name").alias("study_name"),
    pl.col("BCR").alias("bcr_id"),
).write_parquet("db/bcr_batches.pq")
