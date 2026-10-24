import json
from pathlib import Path
from uuid import UUID

from simulator.identity import IDENTITY_NAMESPACE, entity_id, fingerprint

VECTORS = json.loads((Path(__file__).resolve().parents[4] / "contracts/synthetic/identity-vectors.json").read_text())


def test_identity_vectors():
    assert UUID(VECTORS["uuid_namespace"]) == IDENTITY_NAMESPACE
    integration = UUID(VECTORS["integration_id"])
    for case in VECTORS["ids"]:
        assert entity_id(integration, case["kind"], case["external_id"]) == case["expected"]
    for case in VECTORS["fingerprints"]:
        assert fingerprint(VECTORS["synthetic_key"], case["kind"], case["input"]) == case["expected"]


def luhn_valid(number: str) -> bool:
    total = 0
    for index, digit in enumerate(reversed(number)):
        value = int(digit)
        if index % 2 == 1:
            value *= 2
            if value > 9:
                value -= 9
        total += value
    return total % 10 == 0


def test_test_card_vectors():
    cards = VECTORS["test_cards"]
    assert {card["behavior"] for card in cards} == {"ok", "nsf", "issuer", "invalid"}
    for card in cards:
        assert len(card["number"]) == 16
        assert luhn_valid(card["number"])
        assert (
            fingerprint(VECTORS["synthetic_key"], "payment_method", f"tok_{card['card_ref']}_{card['behavior']}")
            == card["expected"]
        )
