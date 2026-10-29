from datetime import datetime, time, timezone

from simulator.world import World, WorldConfig, stage_rng

LIVE_STAGE = 100
BEHAVIOR_BY_OUTCOME = {"succeeded": "ok", "issuer_declined": "issuer", "insufficient_funds": "nsf"}


class HistoricalModel:
    def __init__(self, manifest):
        self.manifest = manifest
        self.world = World.build(WorldConfig.model_validate(manifest["configuration"]["world"]))
        if self.world.digest != manifest["world_digest"]:
            raise ValueError("world digest differs from the manifest")
        self.config = self.world.config
        self.anchor = datetime.combine(self.config.start_date, time.min, timezone.utc)

    def purchase(self, seed, ordinal, now):
        rng = stage_rng(seed, LIVE_STAGE, now.date().toordinal(), ordinal)
        customer = int(rng.integers(self.config.counts.customers))
        purchase = self.world.purchase(rng, now.date(), customer)
        behavior = BEHAVIOR_BY_OUTCOME[purchase.processor_outcome]
        order = {
            "seller": {
                "external_id": purchase.merchant_external_id,
                "account_created_at": purchase.merchant_created_at.isoformat(),
            },
            "buyer": {
                "external_id": purchase.customer_external_id,
                "account_created_at": purchase.customer_created_at.isoformat(),
            },
            "amount": {"minor_units": purchase.unit_price * purchase.quantity, "currency": "USD"},
            "items": [
                {
                    "product_id": f"sku-{purchase.category}-001",
                    "category": purchase.category,
                    "condition": purchase.condition,
                    "quantity": purchase.quantity,
                    "unit_price": purchase.unit_price,
                }
            ],
            "shipping_address": {
                "name": "Synthetic buyer",
                "line1": f"{purchase.shipping_line} {purchase.shipping_city} {purchase.shipping_region} "
                f"{purchase.postal_code} US",
                "city": purchase.shipping_city,
                "country": "US",
                "region": purchase.shipping_region,
                "postal_code": purchase.postal_code,
            },
        }
        signals = {
            "device_fingerprint": purchase.device,
            "user_agent_family": purchase.browser,
            "simulated_client": {"ip_country": purchase.ip_country, "ip_network": purchase.network_address},
        }
        fraud = purchase.scenario != "legitimate"
        delay = purchase.label_delay_days * 86400
        return (
            order,
            signals,
            purchase.payment_method_index,
            behavior,
            purchase.scenario,
            fraud,
            purchase.friendly_fraud,
            delay,
        )


def card_number(index, behavior):
    if not 0 <= index < 10**11:
        raise ValueError("card index outside synthetic encoding")
    prefix = {"ok": "4111", "nsf": "4112", "issuer": "4113", "invalid": "4114"}[behavior]
    body = f"{prefix}{index:011d}"
    total = 0
    for i, digit in enumerate(reversed(body)):
        value = int(digit) * (2 if i % 2 == 0 else 1)
        total += value - 9 if value > 9 else value
    return body + str(-total % 10)
