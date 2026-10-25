import json
import os
import ssl
import time
from urllib import error, parse, request

import certifi


class ApiError(RuntimeError):
    def __init__(self, status, method, path):
        super().__init__(f"{method} {path} returned {status}")
        self.status = status


class Client:
    def __init__(self, url=None, token=None, opener=None, retries=2, sleep=time.sleep):
        self.url = (url or os.environ["OPENMETADATA_URL"]).rstrip("/")
        self.token = token or os.environ["OPENMETADATA_TOKEN"]
        self.retries = retries
        self.sleep = sleep
        self.opener = (
            opener
            or request.build_opener(
                request.HTTPSHandler(context=ssl.create_default_context(cafile=certifi.where()))
            ).open
        )

    def call(self, method, path, body=None, query=None, content_type="application/json"):
        url = self.url + path
        if query:
            url += "?" + parse.urlencode(query)
        data = body if isinstance(body, (bytes, type(None))) else json.dumps(body).encode()
        for attempt in range(self.retries + 1):
            req = request.Request(
                url,
                data=data,
                method=method,
                headers={
                    "Authorization": "Bearer " + self.token,
                    "Content-Type": content_type,
                    "Accept": "application/json",
                },
            )
            try:
                with self.opener(req, timeout=15) as response:
                    payload = response.read()
                return json.loads(payload) if payload else None
            except error.HTTPError as failure:
                if failure.code < 500 or attempt == self.retries:
                    raise ApiError(failure.code, method, path) from None
            except (error.URLError, TimeoutError, ConnectionError):
                if attempt == self.retries:
                    raise
            self.sleep(2**attempt)

    def get(self, path, query=None):
        return self.call("GET", path, query=query)

    def put(self, path, body, query=None, content_type="application/json"):
        return self.call("PUT", path, body, query, content_type)

    def post(self, path, body):
        return self.call("POST", path, body)

    def patch(self, path, operations):
        return self.call("PATCH", path, operations, content_type="application/json-patch+json")

    def find(self, path, query=None):
        try:
            return self.get(path, query)
        except ApiError as failure:
            if failure.status == 404:
                return None
            raise
