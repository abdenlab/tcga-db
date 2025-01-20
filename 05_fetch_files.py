import argparse
from gdcutil import GDCClient

BASE_URL = 'https://api.gdc.cancer.gov'
ENDPOINT = '/files'
fields = [
    "file_id",
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
    # "tags",
    # "revision",
    "created_datetime",
    "updated_datetime",
    "state",

    "associated_entities.entity_submitter_id",
    "associated_entities.entity_type",
    "associated_entities.case_id",
    "associated_entities.entity_id",

    "cases.case_id",
    "cases.submitter_id",

    "analysis.analysis_id",
    "analysis.analysis_type",
    "analysis.submitter_id",
    "analysis.input_files.file_id",
    "analysis.input_files.file_name",
    "analysis.workflow_type",
    "analysis.workflow_version",
    "analysis.workflow_link",
    "analysis.state",

    "index_files.file_id",
    "index_files.file_name",

    # "downstream_analyses.analysis_id",
    # "downstream_analyses.analysis_type",
    # "downstream_analyses.submitter_id",
    # "downstream_analyses.output_files.file_id",
    # "downstream_analyses.output_files.file_name",
    # "downstream_analyses.workflow_type",
    # "downstream_analyses.workflow_version",
    # "downstream_analyses.workflow_link",
    # "downstream_analyses.state",

    # "center.center_id",  # This field is not available ???
    # "center.center_type",
    # "center.code",
    # "center.name",
    # "center.namespace",
    # "center.short_name",

    # "metadata_files.file_id",  # This field is not available ???
    # "metadata_files.file_name",
]


filters = {
    "op": "and",
    "content": [
        {
            "op": "=",
            "content": {
                "field": "cases.project.program.name",
                "value": ["TCGA"]
            }
        },
        {
            "op": "in",
            "content": {
                "field":"files.data_type",
                "value":[
                    "Annotated Somatic Mutation",
                    "Raw Simple Somatic Mutation",
                    "Aggregated Somatic Mutation",
                    "Copy Number Segment",
                    "Allele-specific Copy Number Segment",
                    "Masked Copy Number Segment",
                    "Masked Somatic Mutation",
                    "Structural Rearrangement",
                    "Simple Germline Variation",
                    "Methylation Beta Value",
                    "Gene Expression Quantification",
                    "miRNA Expression Quantification",
                    "Isoform Expression Quantification",
                    "Splice Junction Quantification",
                    "Transcript Fusion"
                ]
            }
        },
    ]
}

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fetch annotation metadata from GDC API')
    parser.add_argument('-o', '--output', type=str, required=True, help='Output file path')
    args = parser.parse_args()
    client = GDCClient(ENDPOINT, fields, filters)
    client.to_json(args.output)
