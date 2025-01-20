import argparse
from gdcutil import GDCClient

# https://api.gdc.cancer.gov/projects/_mapping
ENDPOINT = '/projects'
fields = [
    "project_id",
    "name",
    # "dbgap_accession_number",
    "program.program_id",
    "program.name",
    "program.dbgap_accession_number",
    "disease_type",
    "primary_site",
    "releasable",
    "released",
    "state",
    "summary.case_count",
    "summary.file_count",
    "summary.file_size",
    "summary.data_categories.case_count",
    "summary.data_categories.data_category",
    "summary.data_categories.file_count",
    "summary.experimental_strategies.case_count",
    "summary.experimental_strategies.experimental_strategy",
    "summary.experimental_strategies.file_count",
]

nested = [
    "summary.data_categories",
    "summary.experimental_strategies",
]

filters = {
    "op": "=",
    "content": {
        "field": "program.name",
        "value": ["TCGA"]
    }
}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fetch project metadata from GDC API')
    parser.add_argument('-o', '--output', type=str, required=True, help='Output file path')
    args = parser.parse_args()
    client = GDCClient(ENDPOINT, fields, filters)
    client.to_json(args.output)
