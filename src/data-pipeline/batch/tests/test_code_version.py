import re

from batch.lakehouse import code_version


def test_the_code_version_is_a_stable_digest_of_the_package_source():
    assert re.fullmatch(r"[0-9a-f]{16}", code_version())
    assert code_version() == code_version()
