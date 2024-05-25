import atexit
import logging
import subprocess
import time

import backoff
import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import OperationalError, ProgrammingError, SQLAlchemyError
from sqlalchemy.orm import scoped_session, sessionmaker

from kvalchemy import KVAlchemy, KVStore

log = logging.getLogger(__name__)

DOCKER_MYSQL_URL = "mysql+pymysql://test:test@localhost:52000/test?charset=utf8mb4"


def has_docker() -> bool:
    """
    Checks if docker is available and ready to be used
    """
    proc = subprocess.run(
        ["docker", "ps"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
    )
    return proc.returncode == 0


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


def get_sqlalchemy_urls() -> list[str]:
    """
    Used to parameterize our testing.
    If we have docker available, sets up a mysqld container for our testing.

    Returns a list of sqlalchemy urls to test against.
    """
    urls = ["sqlite:///:memory:"]

    if has_docker():
        # don't care if this succeeds or not
        _stop_mysqld_docker()

        # start container
        subprocess.run(
            'docker run --name kvalchemy-mysqld -e "MYSQL_ALLOW_EMPTY_PASSWORD=1" -e "MYSQL_PASSWORD=test" -e "MYSQL_USER=test" -e "MYSQL_DATABASE=test" -d -p 52000:3306 mysql:8',
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        ).check_returncode()
        atexit.register(_stop_mysqld_docker)
        _wait_for_mysql_stability()
        urls.append(DOCKER_MYSQL_URL)
    else:
        log.warning("Docker not available, not adding tests that require it.")

    return urls


@pytest.fixture(scope="function", params=get_sqlalchemy_urls())
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
