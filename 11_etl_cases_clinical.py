import duckdb
import polars as pl

conn = duckdb.connect(':memory:')

# Cases core table
df = conn.read_json("download/cases.json", format="array").pl()
df.select(
    "id", # "case_id",
    "submitter_id",
    "disease_type",
    "primary_site",
    # "consent_type",
    # "days_to_consent",
    "lost_to_followup",
    "days_to_lost_to_followup",
    # "index_date",
    df['tissue_source_site'].struct['code'].alias("tissue_source_site_code"),
    df["demographic"].struct["demographic_id"],
    df["demographic"].struct["submitter_id"].alias("submitter_demographic_id"),
    pl.col("exposures").map_elements(
        lambda exposures: [exposure["exposure_id"] for exposure in exposures],
        return_dtype=pl.List(pl.Utf8)
    ).alias("exposure_ids"),
    pl.col("exposures").map_elements(
        lambda exposures: [exposure["submitter_id"] for exposure in exposures],
        return_dtype=pl.List(pl.Utf8)
    ).alias("submitter_exposure_ids"),
    "diagnosis_ids",
    "submitter_diagnosis_ids",
    "sample_ids",
    "submitter_sample_ids",
    "portion_ids",
    "submitter_portion_ids",
    "analyte_ids",
    "submitter_analyte_ids",
    "aliquot_ids",
    "submitter_aliquot_ids",
    "slide_ids",
    "submitter_slide_ids",
    df['summary'].struct['file_count'],
    df['summary'].struct['file_size'],
    # df['summary'].struct['data_categories'],
    # df['summary'].struct['experimental_strategies'],
    "created_datetime",
    "updated_datetime",
    "state",
).write_parquet("db/cases.pq")


# Case demographics
df.select(
    pl.col("id").alias("case_id"),
    pl.col("submitter_id").alias("submitter_case_id"),
    pl.col("demographic").struct.unnest(),
).select(
    "case_id",
    "submitter_case_id",
    "demographic_id",
    pl.col("submitter_id").alias("submitter_demographic_id"),
    *[field for field in sorted(df["demographic"].struct.fields) if field not in {"demographic_id", "submitter_id"}]
).write_parquet("db/demographics.pq")


# Case exposures
df.select(
    pl.col("id").alias("case_id"),
    pl.col("submitter_id").alias("submitter_case_id"),
    pl.col("exposures"),
).explode("exposures").drop_nulls("exposures").unnest("exposures").select(
    "case_id",
    "submitter_case_id",
    "exposure_id",
    pl.col("submitter_id").alias("submitter_exposure_id"),
   *[field for field in sorted(df["exposures"].explode().struct.fields) if field not in ["exposure_id", "submitter_id"]]
).write_parquet("db/exposures.pq")


# Case diagnoses
df.select(
    pl.col("id").alias("case_id"),
    pl.col("submitter_id").alias("submitter_case_id"),
    pl.col("diagnoses").explode(),
).drop_nulls("diagnoses").unnest("diagnoses").select(
    "case_id",
    "submitter_case_id",
    "diagnosis_id",
    pl.col("submitter_id").alias("submitter_diagnosis_id"),
    *[
        field for field in sorted(df["diagnoses"].explode().struct.fields)
        if field not in {"diagnosis_id", "submitter_id", "treatments"}
    ],
    pl.col("treatments").map_elements(
        lambda treatments: [treatment["treatment_id"] for treatment in treatments],
        return_dtype=pl.List(pl.Utf8)
    ).alias("treatment_ids"),
    pl.col("treatments").map_elements(
        lambda treatments: [treatment["submitter_id"] for treatment in treatments],
        return_dtype=pl.List(pl.Utf8))
    .alias("submitter_treatment_ids"),
).write_parquet("db/diagnoses.pq")


# Case diagnosis treatments
df2 = df.select(
    pl.col("id").alias("case_id"),
    pl.col("submitter_id").alias("submitter_case_id"),
    pl.col("diagnoses"),
).explode("diagnoses").drop_nulls("diagnoses").unnest("diagnoses").select(
    "case_id",
    "submitter_case_id",
    "diagnosis_id",
    pl.col("submitter_id").alias("submitter_diagnosis_id"),
    pl.col("treatments")
).explode("treatments").drop_nulls("treatments")
df2.unnest("treatments").select(
    "case_id",
    "submitter_case_id",
    "diagnosis_id",
    "submitter_diagnosis_id",
    "treatment_id",
    pl.col("submitter_id").alias("submitter_treatment_id"),
    *[field for field in sorted(df2["treatments"].struct.fields) if field not in ["treatment_id", "submitter_id"]]
).write_parquet("db/treatments.pq")
