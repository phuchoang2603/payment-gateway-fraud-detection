import json
from hashlib import sha256

from simulator.config import HistoricalConfig

PUBLICATION_KEY = "_run/publication.json"


def publication_receipt(store, manifest):
    return {
        "manifest": store.uri("manifest.json"),
        "manifest_sha256": sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest(),
        "cleanup": "skipped",
    }


def complete_publication(store, manifest, *, cleanup_intermediates=False):
    if any(item["key"].startswith("_run/") for item in manifest["inventory"]):
        raise ValueError("published source files must not be disposable run intermediates")
    expected = publication_receipt(store, manifest)
    try:
        publication = store.json(PUBLICATION_KEY)
    except FileNotFoundError:
        publication = expected
    if publication.get("manifest_sha256", expected["manifest_sha256"]) != expected["manifest_sha256"]:
        raise ValueError("publication receipt belongs to a different manifest")
    publication.update({key: expected[key] for key in ("manifest", "manifest_sha256")})
    store.put_json(PUBLICATION_KEY, publication)
    if cleanup_intermediates:
        try:
            removed = store.purge("_run/", keep=(PUBLICATION_KEY,))
        except OSError as error:
            for key, value in getattr(error, "removed", {}).items():
                publication[key] = publication.get(key, 0) + value
            publication.update(cleanup="failed", cleanup_error=str(error))
            store.put_json(PUBLICATION_KEY, publication)
            raise OSError("intermediate cleanup is incomplete; rerun the same historical command") from error
        for key, value in removed.items():
            publication[key] = publication.get(key, 0) + value
        publication["cleanup"] = "removed"
        publication.pop("cleanup_error", None)
    store.put_json(PUBLICATION_KEY, publication)


def finish_publication(config, store, *, cleanup_intermediates=False):
    manifest = store.json("manifest.json")
    if not isinstance(manifest.get("inventory"), list):
        raise ValueError("completed manifest lacks an integrity inventory; publication recovery refused")
    published = HistoricalConfig.from_manifest(manifest)
    if config.logical_configuration != published.logical_configuration:
        raise ValueError("publication configuration differs from the completed manifest")
    if not all(store.verify(item) for item in manifest["inventory"]):
        raise ValueError("completed object integrity mismatch; publication recovery refused")
    complete_publication(store, manifest, cleanup_intermediates=cleanup_intermediates)
    return manifest
