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

    # "tissue_source_site.tissue_source_site_id",
    "tissue_source_site.code",
    # "tissue_source_site.name",
    # "tissue_source_site.project",
    # "tissue_source_site.bcr_id",
]

expand = [
    "samples",
    # "samples.annotations",
    "samples.portions",
    "samples.portions.center",
    # "samples.portions.annotations",
    "samples.portions.analytes",
    "samples.portions.analytes.aliquots",
    # "samples.portions.analytes.annotations",
    "samples.portions.analytes.aliquots.annotations",
    "samples.portions.analytes.aliquots.center",
    "samples.portions.slides",
    # "samples.portions.slides.annotations",
]

filters = {
    "op": "=",
    "content": {
        "field": "project.program.name",
        "value": ["TCGA"]
    }
}


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fetch case sample metadata from GDC API')
    parser.add_argument('-o', '--output', type=str, required=True, help='Output file path')
    args = parser.parse_args()
    client = GDCClient(ENDPOINT, fields, filters, expand)
    client.to_json(args.output)
