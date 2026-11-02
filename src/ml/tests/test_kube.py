from types import SimpleNamespace

import pytest

from risk_model.kube import SOURCE_URI, rolled_out

URI = "model-registry://payment-risk/v2"


def deployment(uri=URI, generation=3, observed=3, replicas=1, updated=1, ready=1, total=1):
    return SimpleNamespace(
        metadata=SimpleNamespace(generation=generation),
        spec=SimpleNamespace(
            replicas=replicas,
            template=SimpleNamespace(metadata=SimpleNamespace(annotations={SOURCE_URI: uri})),
        ),
        status=SimpleNamespace(
            observed_generation=observed,
            replicas=total,
            updated_replicas=updated,
            ready_replicas=ready,
        ),
    )


def test_a_finished_rollout_to_the_requested_model_is_done():
    assert rolled_out(deployment(), URI)


@pytest.mark.parametrize(
    "stale",
    [
        deployment(uri="model-registry://payment-risk/v1"),
        deployment(observed=2),
        deployment(updated=0, total=2),
        deployment(ready=0),
        deployment(replicas=0, updated=0, ready=0, total=0),
    ],
)
def test_anything_short_of_a_finished_rollout_is_not_done(stale):
    assert not rolled_out(stale, URI)
