import base64
import re
import urllib.error
import urllib.request
from pathlib import Path
from xml.sax.saxutils import escape

import pytest
import yaml

INSTALLATION = Path(__file__).resolve().parents[5] / "infra/charts/clickhouse/templates/installation.yaml"
USER = "grafana"


def chart_keys(section: str) -> dict:
    """The chart's `grafana/...` keys under `profiles:` or `users:`, which hold no template expressions."""
    text = INSTALLATION.read_text()
    block = re.search(rf"^    {section}:\n((?:      .*\n|        .*\n)+)", text, re.MULTILINE).group(1)
    parsed = yaml.safe_load(block)
    return {key.split("/", 1)[1]: value for key, value in parsed.items() if key.startswith(f"{USER}/")}


def nest(flat: dict) -> dict:
    tree = {}
    for path, value in flat.items():
        node = tree
        *parents, leaf = path.split("/")
        for part in parents:
            node = node.setdefault(part, {})
        node[leaf] = value
    return tree


def xml(tag: str, value) -> str:
    if isinstance(value, dict):
        return f"<{tag}>{''.join(xml(key, item) for key, item in value.items())}</{tag}>"
    if isinstance(value, list):
        return "".join(xml(tag, item) for item in value)
    return f"<{tag}>{escape(str(value))}</{tag}>"


def users_xml() -> str:
    profile = nest(chart_keys("profiles"))
    user = nest(chart_keys("users"))
    user["password"] = USER
    user["networks"] = {"ip": "::/0"}
    return f"<clickhouse><profiles>{xml(USER, profile)}</profiles><users>{xml(USER, user)}</users></clickhouse>"


@pytest.fixture(scope="module")
def server(tmp_path_factory):
    from testcontainers.core.container import DockerContainer
    from testcontainers.core.wait_strategies import HttpWaitStrategy

    config = tmp_path_factory.mktemp("users") / "grafana.xml"
    config.write_text(users_xml())
    config.chmod(0o644)
    container = (
        DockerContainer("clickhouse/clickhouse-server:26.10.2.103")
        .with_exposed_ports(8123)
        .with_env("CLICKHOUSE_USER", "test")
        .with_env("CLICKHOUSE_PASSWORD", "test")
        .with_volume_mapping(str(config), "/etc/clickhouse-server/users.d/grafana.xml", "ro")
    )
    with container:
        HttpWaitStrategy(8123, "/ping").for_response_predicate(lambda body: "Ok" in body).wait_until_ready(container)
        url = f"http://{container.get_container_host_ip()}:{container.get_exposed_port(8123)}/"
        for sql in (
            "CREATE DATABASE marts",
            "CREATE DATABASE lakehouse",
            "CREATE TABLE marts.fact_merchant_day (day Date) ENGINE = MergeTree ORDER BY day",
            "CREATE TABLE lakehouse.`silver.attempts` (attempt_id String) ENGINE = MergeTree ORDER BY attempt_id",
        ):
            assert send(url, "test", sql) is None
        yield url


def send(url, user, sql):
    auth = "Basic " + base64.b64encode(f"{user}:{user}".encode()).decode()
    request = urllib.request.Request(url, data=sql.encode(), method="POST", headers={"Authorization": auth})
    try:
        with urllib.request.urlopen(request, timeout=60):
            return None
    except urllib.error.HTTPError as error:
        return error.read().decode(errors="replace")


def test_the_dashboard_user_reads_the_marts(server):
    assert send(server, USER, "SELECT count() FROM marts.fact_merchant_day") is None


def test_the_dashboard_user_cannot_read_the_lakehouse(server):
    assert "ACCESS_DENIED" in send(server, USER, "SELECT count() FROM lakehouse.`silver.attempts`")


def test_the_dashboard_user_cannot_write(server):
    assert "READONLY" in send(server, USER, "INSERT INTO marts.fact_merchant_day VALUES ('2026-01-01')")


def test_the_dashboard_user_cannot_raise_its_limits(server):
    error = send(server, USER, "SELECT 1 SETTINGS max_execution_time = 600")

    assert "SETTING_CONSTRAINT_VIOLATION" in error
