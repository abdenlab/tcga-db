import requests
import json
import time


class GDCClient:

    BASE_URL = 'https://api.gdc.cancer.gov'

    def __init__(self, endpoint, fields, filters, expand=None):
        self.url = self.BASE_URL + endpoint
        self.params = {
            'fields': ",".join(fields),
            'filters': json.dumps(filters),
        }
        if expand is not None:
            self.params['expand'] = ",".join(expand)

        # Initial dummy request to get the total number of records
        content = self._get(size=1)
        self.n_records = content['data']['pagination']['total']

    def _get(self, from_=0, size=1000, format='json'):
        response = requests.get(
            self.url,
            params={
                **self.params,
                'from': from_,
                'size': size,
                'format': format,
            }
        )
        try:
            if format == 'json':
                content = response.json()
            elif format == 'TSV':
                content = response.text
            else:
                raise ValueError(f"Unsupported format: {format}")
        except json.JSONDecodeError:
            err = response.text if response.ok else str(response)
            raise RuntimeError(err)
        return content

    def to_json(self, out_path, start=0, step=1000):
        results = []
        for i in range(start, self.n_records, step):
            content = self._get(from_=i, size=step)
            results.extend(content['data']['hits'])
            print(content['data']['pagination'], len(results))
            time.sleep(0.5)
        with open(out_path, 'w') as f:
            f.write(json.dumps(results, indent=2))
        print('Done')

    def to_tsv(self, out_path, start=0, step=1000):
        with open(out_path, 'w') as f:
            for i in range(start, self.n_records, step):
                content = self._get(from_=i, size=step, format='TSV')
                f.write(content)
                time.sleep(0.5)
        print('Done')
