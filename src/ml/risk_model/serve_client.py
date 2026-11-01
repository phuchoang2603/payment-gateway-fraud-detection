import json
import urllib.request


class ServeClient:
    def __init__(self, gateway, host, model):
        self.url = f"http://{gateway}/v1/models/{model}:predict"
        self.host = host

    def score(self, rows, timeout=30):
        request = urllib.request.Request(
            self.url,
            data=json.dumps({"instances": rows}).encode(),
            headers={"Host": self.host, "Content-Type": "application/json"},
        )
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return json.load(response)
