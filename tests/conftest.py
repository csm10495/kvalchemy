import atexit
import logging
import subprocess
import time
from functools import lru_cache

import backoff
import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import OperationalError, ProgrammingError, SQLAlchemyError
from sqlalchemy.orm import scoped_session, sessionmaker

from kvalchemy import KVAlchemy, KVStore

log = logging.getLogger(__name__)

DOCKER_MYSQL_URL = "mysql+pymysql://test:test@localhost:52000/test?charset=utf8mb4"


@lru_cache(maxsize=None)
def has_docker() -> bool:
    """
    Checks if docker is available and ready to be used
    """
    proc = subprocess.run(
        ["docker", "ps"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    return proc.returncode == 0


@lru_cache(maxsize=None)
def _start_mysqld_docker() -> None:
    """
    Starts the mysqld docker container
    """
    # start container
    subprocess.run(
        'docker run --name kvalchemy-mysqld -e "MYSQL_ALLOW_EMPTY_PASSWORD=1" -e "MYSQL_PASSWORD=test" -e "MYSQL_USER=test" -e "MYSQL_DATABASE=test" -d -p 52000:3306 mysql:8',
        shell=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    ).check_returncode()
    atexit.register(_stop_mysqld_docker)
    _wait_for_mysql_stability()


def _stop_mysqld_docker() -> None:
    """
    Stops the mysqld docker container if it is running
    """
    subprocess.run(
        "docker remove kvalchemy-mysqld -f",
        shell=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


@backoff.on_exception(
    backoff.constant, (OperationalError), max_tries=100, jitter=None, interval=0.5
)
def _wait_for_mysql_stability():
    """
    Continually tries to connect to the mysql server unitl its fully ready
    """
    engine = create_engine(DOCKER_MYSQL_URL)
    inspect(engine).get_table_names()


@lru_cache(maxsize=None)
def get_sqlalchemy_urls(pytestconfig) -> list[str]:
    """
    Used to parameterize our testing.
    If we have docker available, sets up a mysqld container for our testing.

    Returns a list of sqlalchemy urls to test against.
    """
    urls = ["sqlite:///:memory:"]

    if pytestconfig.getoption("--sqlite-only"):
        log.info("Skipping mysql setup because of sqlite-only")
    elif not has_docker():
        log.warning("Docker not available, not adding tests that require it.")
    else:
        # don't care if this succeeds or not
        _stop_mysqld_docker()
        _start_mysqld_docker()
        urls.append(DOCKER_MYSQL_URL)

    return urls


@pytest.fixture(scope="function")
def kvalchemy(request):
    """
    Fixture to get a kvalchemy instance for testing.

    After testing, cleans the database.
    """
    try:
        kva = KVAlchemy(request.param)
        yield kva
    finally:
        # Ensure that each test ends with a clean db.
        with kva.session() as session:
            session.query(KVStore).delete()


@pytest.fixture(scope="function")
def kvstore():
    """
    An example KVStore instance for testing.
    """
    yield KVStore(
        key="key",
        value="value",
        tag="",
    )


def pytest_generate_tests(metafunc):
    if "kvalchemy" in metafunc.fixturenames:
        metafunc.parametrize(
            "kvalchemy", get_sqlalchemy_urls(metafunc.config), indirect=True
        )


def pytest_addoption(parser):
    """
    Used to add options to pytest via the command line.
    """
    parser.addoption("--sqlite-only", action="store_true", default=False)
