from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[4]


@pytest.fixture
def repo() -> Path:
    return REPO
