import pytest

from spark.jobs.session import build_spark


@pytest.fixture(scope="session")
def spark():
    session = build_spark("rees46-unit-tests", driver_memory="1g")
    yield session
    session.stop()
