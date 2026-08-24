import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from mercury.core.config import load_config  # noqa: E402
from mercury.core.db import Database  # noqa: E402


@pytest.fixture()
def settings():
    return load_config()


@pytest.fixture()
def paper_settings():
    """The paper-broker profile (``development`` per config/environments.yaml).

    Tests that exercise paper-fill/order-routing behaviour must use this
    profile; the default deployment profile is MT5-backed.
    """
    return load_config(environment="development")


@pytest.fixture()
def db():
    database = Database.in_memory()
    database.create_tables()
    yield database
    database.dispose()
