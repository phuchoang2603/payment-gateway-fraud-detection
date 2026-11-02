import json
import urllib.request


class ServeClient:
    def __init__(self, address, model):
        self.url = f"{address.rstrip('/')}/v1/models/{model}:predict"

    def score(self, rows, timeout=30):
        request = urllib.request.Request(
            self.url,
            data=json.dumps({"instances": rows}).encode(),
            headers={"Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.load(response)
