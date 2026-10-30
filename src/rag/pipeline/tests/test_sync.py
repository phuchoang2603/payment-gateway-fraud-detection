import re

from fraud_rag_pipeline import corpus


def document(repo, doc_id):
    return next(item for item in corpus.load(repo) if item.doc_id == doc_id)


def risk_reasons(repo):
    policy = (repo / "src/payment-gateway/fraud/internal/domain/policy.go").read_text()
    return set(re.findall(r'append\(result\.Reasons, "([A-Z_]+)"\)', policy))


def processor_reasons(repo):
    sale = (repo / "src/payment-gateway/processor-simulator/internal/domain/sale.go").read_text()
    return set(re.findall(r'Reason\w+\s*=\s*"([A-Z_]+)"', sale))


def proto_enum(repo, path, prefix):
    text = (repo / path).read_text()
    return {name for name in re.findall(rf"\b({prefix}_[A-Z_]+)\s*=\s*[1-9]", text)}


def test_decline_glossary_lists_exactly_the_codes_in_code(repo):
    glossary = document(repo, "decline-codes").text
    headings = set(re.findall(r"^## ([A-Z_]+)$", glossary, re.M))

    assert risk_reasons(repo) and processor_reasons(repo)
    assert headings == risk_reasons(repo) | processor_reasons(repo)


def test_decline_glossary_names_every_decline_source(repo):
    glossary = document(repo, "decline-codes").text

    for source in proto_enum(repo, "contracts/payments/v1/events.proto", "DECLINE_SOURCE"):
        assert f"`{source}`" in glossary


def test_risk_policy_matches_the_enforced_version_and_thresholds(repo):
    policy = (repo / "src/payment-gateway/fraud/internal/domain/policy.go").read_text()
    text = document(repo, "risk-policy").text
    version = re.search(r'PolicyVersion = "([^"]+)"', policy).group(1)
    thresholds = re.findall(r"AmountMinor > (\d+)", policy)

    assert f"`{version}`" in text
    assert thresholds and all(f"{threshold} minor units" in text for threshold in thresholds)
    assert "7*24*time.Hour" in policy and "less than 7 days old" in text
    assert risk_reasons(repo) == set(re.findall(r"`([A-Z_]{5,})`: the", text))


def test_chargeback_policy_names_every_label_source(repo):
    text = document(repo, "chargebacks").text

    for source in proto_enum(repo, "contracts/payments/v1/labels.proto", "LABEL_SOURCE"):
        assert f"`{source}`" in text


def test_consistency_sources_exist(repo):
    for item in corpus.load(repo):
        for source in item.sources:
            assert (repo / source).exists(), f"{item.doc_id}: {source}"
