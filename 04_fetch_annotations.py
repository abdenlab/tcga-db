import argparse
from gdcutil import GDCClient

# https://api.gdc.cancer.gov/annotations/_mapping
ENDPOINT = '/annotations'
fields = [
    "annotation_id",
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
    "legacy_created_datetime",
    "legacy_updated_datetime",
    "state",
    "status",
    "annotation_autocomplete",
    "project.code",
    # "project.dbgap_accession_number",
    # "project.disease_type",
    # "project.intended_release_date",
    # "project.name",
    # "project.primary_site",
    # "project.program.dbgap_accession_number",
    # "project.program.name",
    # "project.program.program_id",
    # "project.project_id",
    # "project.releasable",
    # "project.released",
    # "project.state",
]

filters = {
    "op": "=",
    "content": {
        "field": "project.program.name",
        "value": ["TCGA"]
    }
}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fetch annotation metadata from GDC API')
    parser.add_argument('-o', '--output', type=str, required=True, help='Output file path')
    args = parser.parse_args()
    client = GDCClient(ENDPOINT, fields, filters)
    client.to_json(args.output)
