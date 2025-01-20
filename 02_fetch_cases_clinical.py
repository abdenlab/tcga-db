import argparse
from gdcutil import GDCClient

# https://api.gdc.cancer.gov/cases/_mapping
ENDPOINT = '/cases'
fields = [
    "case_id",
    "submitter_id",

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

    "diagnosis_ids",
    "submitter_diagnosis_ids",

    "disease_type",
    "primary_site",
    "consent_type",
    "days_to_consent",
    "lost_to_followup",
    "days_to_lost_to_followup",
    "created_datetime",
    "updated_datetime",
    "index_date",
    "state",

    "files.file_id",
    "files.submitter_id",
]

expand = [
    "summary",
    "summary.data_categories",
    "summary.experimental_strategies",
    # "annotations",

    "tissue_source_site",

    "demographic",

    "diagnoses",
    "diagnoses.pathology_details",
    "diagnoses.treatments",
    # "diagnoses.annotations",

    "exposures",

    "family_histories",

    "follow_ups",
    "follow_ups.molecular_tests",
    "follow_ups.other_clinical_attributes",
]

filters = {
    "op": "=",
    "content": {
        "field": "project.program.name",
        "value": ["TCGA"]
    }
}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fetch case metadata from GDC API')
    parser.add_argument('-o', '--output', type=str, required=True, help='Output file path')
    args = parser.parse_args()
    client = GDCClient(ENDPOINT, fields, filters, expand)
    client.to_json(args.output)
