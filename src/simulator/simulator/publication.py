import json
from hashlib import sha256

from simulator.config import load_published_config

PUBLICATION_KEY = "_run/publication.json"


def publication_receipt(store, manifest):
    return {
        "manifest": store.uri("manifest.json"),
        "manifest_sha256": sha256(json.dumps(manifest, sort_keys=True).encode()).hexdigest(),
        "accepted_version": manifest["configuration"].get("accepted_version", False),
        "tagging": "pending" if store.remote and manifest["configuration"].get("accepted_version") else "skipped",
        "cleanup": "skipped",
    }


def complete_publication(store, manifest, *, cleanup_intermediates=False):
    expected = publication_receipt(store, manifest)
    try:
        publication = store.json(PUBLICATION_KEY)
    except FileNotFoundError:
        publication = expected
    if publication.get("manifest_sha256", expected["manifest_sha256"]) != expected["manifest_sha256"]:
        raise ValueError("publication receipt belongs to a different manifest")
    publication.update({key: expected[key] for key in ("manifest", "manifest_sha256", "accepted_version")})
    store.put_json(PUBLICATION_KEY, publication)
    if store.remote and publication["accepted_version"]:
        try:
            for item in manifest["inventory"]:
                store.tag(item["key"], {"accepted": "true"})
            store.tag("manifest.json", {"accepted": "true"})
        except OSError as error:
            publication.update(tagging="failed", tagging_error=str(error))
            store.put_json(PUBLICATION_KEY, publication)
            raise OSError("accepted-object protection is incomplete; rerun the same historical command") from error
        publication.update(tagging="tagged", tagged_objects=len(manifest["inventory"]) + 1)
        publication.pop("tagging_error", None)
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
    published = load_published_config(manifest["configuration"])
    if (
        config.logical_configuration != published.logical_configuration
        or config.accepted_version != published.accepted_version
    ):
        raise ValueError("publication configuration differs from the completed manifest")
    if not all(store.verify(item) for item in manifest["inventory"]):
        raise ValueError("completed object integrity mismatch; publication recovery refused")
    complete_publication(store, manifest, cleanup_intermediates=cleanup_intermediates)
    return manifest
