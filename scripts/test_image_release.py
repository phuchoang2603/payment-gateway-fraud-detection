import importlib.util
import json
import tempfile
import unittest
from copy import deepcopy
from pathlib import Path


def load_script(name):
    spec = importlib.util.spec_from_file_location(
        name, Path(__file__).with_name(f"{name}.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


selector = load_script("select-service-images")
pinner = load_script("pin-service-images")
MANIFEST = json.loads(
    Path(__file__)
    .resolve()
    .parents[1]
    .joinpath(".github/service-images.json")
    .read_text()
)


class ImageReleaseTests(unittest.TestCase):
    def selected(self, *paths, current=None):
        return {
            image["service"]
            for image in selector.selected_images(current or MANIFEST, MANIFEST, paths)[
                "include"
            ]
        }

    def test_affected_sources(self):
        self.assertEqual(self.selected("docs/deployment/gitops.md"), set())
        self.assertEqual(self.selected("infra/charts/kafka/values-prod.yaml"), set())
        self.assertEqual(
            self.selected("src/payment-gateway/payments/internal/payment.go"),
            {"payments"},
        )
        self.assertEqual(
            self.selected("src/payment-gateway/shared/go.mod"),
            {
                "fraud",
                "edge",
                "payments",
                "notifier",
                "vault-simulator",
                "processor-simulator",
            },
        )
        self.assertEqual(
            self.selected("contracts/features/payment.yaml"),
            {"data-platform-spark", "data-platform-metadata"},
        )

    def test_build_configuration_changes(self):
        all_images = {image["service"] for image in MANIFEST["include"]}
        self.assertEqual(self.selected(".dockerignore"), all_images)
        self.assertEqual(self.selected(".github/workflows/build-image.yml"), all_images)
        self.assertEqual(
            self.selected(
                "infra/docker/data-platform-metadata.Dockerfile.dockerignore"
            ),
            {"data-platform-metadata"},
        )
        current = deepcopy(MANIFEST)
        current["include"][0]["build-args"] += "OTHER=value\n"
        self.assertEqual(
            self.selected(".github/service-images.json", current=current), {"fraud"}
        )

    def test_repeated_images_preserve_unrelated_values(self):
        old_digest, digest, revision = (
            "sha256:" + "a" * 64,
            "sha256:" + "b" * 64,
            "c" * 40,
        )
        image = {
            "repository": "ghcr.io/example/app",
            "digest": digest,
            "revision": revision,
        }
        original = f'''# keep this comment
images:
  first:
    repository: ghcr.io/example/app
    digest: "{old_digest}" # pinned
    revision: old
  second:
    repository: ghcr.io/example/app
    digest: {old_digest}
    revision: old
other:
  repository: ghcr.io/example/other
  digest: {old_digest}
  revision: unchanged
'''
        expected = (
            original.replace(f'"{old_digest}"', digest)
            .replace(f"    digest: {old_digest}", f"    digest: {digest}")
            .replace("revision: old", f"revision: {revision}")
        )
        updated = pinner.update_values(original, {image["repository"]: image})
        self.assertEqual(updated, expected)
        image["revision"] = "d" * 40
        self.assertEqual(
            pinner.update_values(updated, {image["repository"]: image}), updated
        )

    def test_bad_artifacts_and_duplicate_keys_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            with self.assertRaises(ValueError):
                pinner.published_images(path)
            image = {
                "repository": "ghcr.io/example/app",
                "digest": "sha256:" + "a" * 64,
                "revision": "b" * 40,
            }
            (path / "one.json").write_text(json.dumps(image))
            self.assertEqual(
                pinner.published_images(path), {image["repository"]: image}
            )
            image["digest"] = "sha256:" + "c" * 64
            (path / "two.json").write_text(json.dumps(image))
            with self.assertRaises(ValueError):
                pinner.published_images(path)
            image["digest"] = "latest"
            (path / "two.json").write_text(json.dumps(image))
            with self.assertRaises(ValueError):
                pinner.published_images(path)
        with self.assertRaises(ValueError):
            pinner.update_values("key: one\nkey: two\n", {})


if __name__ == "__main__":
    unittest.main()
