import json
import requests

for endpoint in ["projects", "cases", "annotations", "files"]:
    print(endpoint)
    schema = requests.get(f"https://api.gdc.cancer.gov/{endpoint}/_mapping").json()
    with open(f"schemas/{endpoint}.schema.json", 'w') as f:
        f.write(json.dumps(schema, indent=2))
