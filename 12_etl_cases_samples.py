import duckdb
import polars as pl

conn = duckdb.connect(':memory:')
df = conn.read_json("download/cases_samples.json", format="array").pl()


# Case samples
cases = df.select([
    "case_id",
    pl.col("submitter_id").alias("submitter_case_id"),
    "samples"
])
sample_fields = [
    "id",
    "submitter_id",
    "case_id",
    "submitter_case_id",
    'sample_type_id',
    'sample_type',
    'tissue_type',
    'specimen_type',
    'pathology_report_uuid',
    'shortest_dimension',
    'intermediate_dimension',
    'longest_dimension',
    'initial_weight',
    'current_weight',
    'preservation_method',
    'freezing_method',
    'is_ffpe',
    'oct_embedded',
    'method_of_sample_procurement',
    'days_to_sample_procurement',
    'days_to_collection',
    'time_between_clamping_and_freezing',
    'time_between_excision_and_freezing',
    'biospecimen_anatomic_site',
    'biospecimen_laterality',
    'tumor_descriptor',
    'tumor_code_id',
    'tumor_code',
    'composition',
    'passage_count',
    'growth_rate',
    'diagnosis_pathologically_confirmed',
    'distance_normal_to_tumor',
    'distributor_reference',
    'catalog_reference',
    'created_datetime',
    'updated_datetime',
    'state',
]
samples = (
    cases
    .explode("samples")
    .with_columns(
        pl.col("samples").struct.unnest()
    )
).rename({
    "sample_id": "id",
}).select(
    sample_fields
)
samples.write_parquet("db/samples.pq")


# Case sample portions
cases = df.select([
    "case_id",
    pl.col("submitter_id").alias("submitter_case_id"),
    "samples"
])
samples = (
    cases
    .explode("samples")
    .with_columns(
        pl.col("samples").struct.unnest()
    )
).with_columns(
    pl.col("submitter_id").alias("submitter_sample_id")
).select([
    "sample_id",
    "submitter_sample_id",
    "case_id",
    "submitter_case_id",
    "portions"
])
portions = (
    samples
    .explode("portions")
    .with_columns(
        pl.col("portions").struct.unnest()
    )
).with_columns(
    pl.col("portion_id").alias("id"),
    pl.col("analytes").map_elements(
        lambda x: [y['analyte_id'] for y in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("analyte_ids"),
    pl.col("analytes").map_elements(
        lambda x: [y['submitter_id'] for y in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("submitter_analyte_ids"),
    pl.col("slides").map_elements(
        lambda x: [y['slide_id'] for y in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("slide_ids"),
    pl.col("slides").map_elements(
        lambda x: [y['submitter_id'] for y in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("submitter_slide_ids"),
    pl.col("center").struct["center_id"],
    pl.col("center").struct["code"].alias("center_code"),
).select([
    'id',
    'submitter_id',
    'sample_id',
    'submitter_sample_id',
    'case_id',
    'submitter_case_id',
    'portion_number',
    "analyte_ids",
    "submitter_analyte_ids",
    "slide_ids",
    "submitter_slide_ids",
    "center_id",
    "center_code",
    'weight',
    'is_ffpe',
    'creation_datetime',
    'created_datetime',
    'updated_datetime',
    'state',
])
portions.write_parquet("db/portions.pq")


# Case sample portion analytes
cases = df.select([
    "case_id",
    pl.col("submitter_id").alias("submitter_case_id"),
    "samples"
])
samples = (
    cases
    .explode("samples")
    .with_columns(
        pl.col("samples").struct.unnest()
    )
).filter(
    pl.col("sample_id").is_not_null()
).select([
    "sample_id",
    pl.col("submitter_id").alias("submitter_sample_id"),
    "case_id",
    "submitter_case_id",
    "portions",
])
portions = (
    samples
    .explode("portions")
    .with_columns(
        pl.col("portions").struct.unnest()
    )
).filter(
    pl.col("portion_id").is_not_null()
).select([
    'portion_id',
    pl.col('submitter_id').alias('submitter_portion_id'),
    'sample_id',
    'submitter_sample_id',
    'case_id',
    'submitter_case_id',
    'portion_number',
    pl.col("analytes").map_elements(
        lambda x: [y['analyte_id'] for y in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("analyte_ids"),
    pl.col("analytes").map_elements(
        lambda x: [y['submitter_id'] for y in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("submitter_analyte_ids"),
    pl.col("slides").map_elements(
        lambda x: [y['slide_id'] for y in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("slide_ids"),
    pl.col("slides").map_elements(
        lambda x: [y['submitter_id'] for y in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("submitter_slide_ids"),
    pl.col("center").struct["center_id"],
    pl.col("center").struct["code"].alias("center_code"),
    "analytes",
])
analytes = (
    portions
    .explode("analytes")
    .with_columns(
        pl.col("analytes").struct.unnest()
    )
).filter(
    pl.col("analyte_id").is_not_null()
).select([
    'analyte_id',
    'submitter_id',
    'portion_id',
    'submitter_portion_id',
    'sample_id',
    'submitter_sample_id',
    'case_id',
    'submitter_case_id',
    'analyte_type_id',
    'analyte_type',
    'analyte_quantity',
    'analyte_volume',
    'amount',
    'concentration',
    'well_number',
    'experimental_protocol_type',
    'spectrophotometer_method',
    'a260_a280_ratio',
    'ribosomal_rna_28s_16s_ratio',
    'rna_integrity_number',
    'normal_tumor_genotype_snp_match',
    'created_datetime',
    'updated_datetime',
    'state',
    pl.col('aliquots').map_elements(
        lambda x: [y['aliquot_id'] for y in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("aliquot_ids"),
    pl.col('aliquots').map_elements(
        lambda x: [y['submitter_id'] for y in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("submitter_aliquot_ids"),
])
analytes.write_parquet("db/analytes.pq")


# Case sample portion analyte aliquots
cases = df.select([
    "case_id",
    pl.col("submitter_id").alias("submitter_case_id"),
    pl.col('tissue_source_site').struct['code'].alias("tissue_source_site_code"),
    "samples"
])
samples = (
    cases
    .explode("samples")
    .with_columns(
        pl.col("samples").struct.unnest()
    )
).filter(
    pl.col("sample_id").is_not_null()
).select([
    "sample_id",
    pl.col("submitter_id").alias("submitter_sample_id"),
    "case_id",
    "submitter_case_id",
    "tissue_source_site_code",
    "portions",
])
portions = (
    samples
    .explode("portions")
    .with_columns(
        pl.col("portions").struct.unnest()
    )
).filter(
    pl.col("portion_id").is_not_null()
).select([
    'portion_id',
    pl.col('submitter_id').alias('submitter_portion_id'),
    'sample_id',
    'submitter_sample_id',
    'case_id',
    'submitter_case_id',
    "tissue_source_site_code",
    'portion_number',
    pl.col("analytes").map_elements(
        lambda x: [y['analyte_id'] for y in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("analyte_ids"),
    pl.col("analytes").map_elements(
        lambda x: [y['submitter_id'] for y in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("submitter_analyte_ids"),
    pl.col("slides").map_elements(
        lambda x: [y['slide_id'] for y in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("slide_ids"),
    pl.col("slides").map_elements(
        lambda x: [y['submitter_id'] for y in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("submitter_slide_ids"),
    "analytes",
])
analytes = (
    portions
    .explode("analytes")
    .with_columns(
        pl.col("analytes").struct.unnest()
    )
).filter(
    pl.col("analyte_id").is_not_null()
).select([
    'analyte_id',
    pl.col('submitter_id').alias('submitter_analyte_id'),
    'portion_id',
    'submitter_portion_id',
    'sample_id',
    'submitter_sample_id',
    'case_id',
    'submitter_case_id',
    "tissue_source_site_code",
    pl.col('aliquots').map_elements(
        lambda x: [y['aliquot_id'] for y in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("aliquot_ids"),
    pl.col('aliquots').map_elements(
        lambda x: [y['submitter_id'] for y in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("submitter_aliquot_ids"),
    "aliquots",
])
aliquots = (
    analytes
    .explode("aliquots")
    .with_columns(
        pl.col("aliquots").struct.unnest()
    )
).filter(
    pl.col("aliquot_id").is_not_null()
).select([
    'aliquot_id',
    'submitter_id',
    'analyte_id',
    'submitter_analyte_id',
    'portion_id',
    'submitter_portion_id',
    'sample_id',
    'submitter_sample_id',
    'case_id',
    'submitter_case_id',
    "tissue_source_site_code",
    pl.col("center").struct["center_id"],
    pl.col("center").struct["code"].alias("center_code"),
    "source_center",
    "aliquot_quantity",
    "aliquot_volume",
    "concentration",
    "amount",
    "selected_normal_low_pass_wgs",
    "selected_normal_targeted_sequencing",
    "selected_normal_wgs",
    "selected_normal_wxs",
    "no_matched_normal_low_pass_wgs",
    "no_matched_normal_targeted_sequencing",
    "no_matched_normal_wgs",
    "no_matched_normal_wxs",
    "created_datetime",
    "updated_datetime",
    "state",
])
aliquots.write_parquet("db/aliquots.pq")


# Case sample portion slides
cases = df.select([
    "case_id",
    pl.col("submitter_id").alias("submitter_case_id"),
    pl.col('tissue_source_site').struct['code'].alias("tissue_source_site_code"),
    "samples"
])
samples = (
    cases
    .explode("samples")
    .with_columns(
        pl.col("samples").struct.unnest()
    )
).filter(
    pl.col("sample_id").is_not_null()
).select([
    "sample_id",
    pl.col("submitter_id").alias("submitter_sample_id"),
    "case_id",
    "submitter_case_id",
    "tissue_source_site_code",
    "portions",
])
portions = (
    samples
    .explode("portions")
    .with_columns(
        pl.col("portions").struct.unnest()
    )
).filter(
    pl.col("portion_id").is_not_null()
).select([
    'portion_id',
    pl.col('submitter_id').alias('submitter_portion_id'),
    'sample_id',
    'submitter_sample_id',
    'case_id',
    'submitter_case_id',
    "tissue_source_site_code",
    'portion_number',
    pl.col("analytes").map_elements(
        lambda x: [y['analyte_id'] for y in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("analyte_ids"),
    pl.col("analytes").map_elements(
        lambda x: [y['submitter_id'] for y in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("submitter_analyte_ids"),
    pl.col("slides").map_elements(
        lambda x: [y['slide_id'] for y in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("slide_ids"),
    pl.col("slides").map_elements(
        lambda x: [y['submitter_id'] for y in x],
        return_dtype=pl.List(pl.Utf8),
    ).alias("submitter_slide_ids"),
    "slides",
])
slides = (
    portions
    .explode("slides")
    .with_columns(
        pl.col("slides").struct.unnest()
    )
).filter(
    pl.col("slide_id").is_not_null()
).select([
    'slide_id',
    'submitter_id',
    'portion_id',
    'submitter_portion_id',
    'sample_id',
    'submitter_sample_id',
    'case_id',
    'submitter_case_id',
    "tissue_source_site_code",
    'section_location',
    'number_proliferating_cells',
    'percent_eosinophil_infiltration',
    'percent_granulocyte_infiltration',
    'percent_inflam_infiltration',
    'percent_lymphocyte_infiltration',
    'percent_monocyte_infiltration',
    'percent_necrosis',
    'percent_neutrophil_infiltration',
    'percent_normal_cells',
    'percent_stromal_cells',
    'percent_tumor_cells',
    'percent_tumor_nuclei',
    'created_datetime',
    'updated_datetime',
    'state',
])
slides.write_parquet("db/slides.pq")
