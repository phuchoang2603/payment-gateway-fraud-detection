import json
import re
from hashlib import sha256


def identity(value):
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}", value):
        raise ValueError("invalid landing epoch")
    return value


def state_key(epoch):
    return f"landing/{identity(epoch)}/progress.json"


def selection(store, version):
    encoded = store.read(f"selections/{version}.json", max_bytes=64 * 1024 * 1024)
    manifest = json.loads(encoded)
    if manifest.get("dataset_version") != version:
        raise ValueError("immutable landing selection is missing")
    return manifest, sha256(encoded).hexdigest()
