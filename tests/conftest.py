import atexit
import logging
import shutil
import socket
import subprocess
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache
from typing import List, Optional

import backoff
import pytest
from func_timeout import FunctionTimedOut, func_set_timeout
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import DatabaseError, OperationalError

from kvalchemy import KVAlchemy, KVStore

log = logging.getLogger(__name__)


@lru_cache(maxsize=None)
def has_docker() -> bool:
    """
    Checks if docker is available and ready to be used
    """
    if shutil.which("docker"):
        proc = subprocess.run(
            ["docker", "info", "-f", "{{ .OSType }}"], capture_output=True, text=True
        )

        if proc.stdout.strip() == "linux":
            proc = subprocess.run(
                ["docker", "ps"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            )
            return proc.returncode == 0

    return False


class StartOnceStopAtExitDockerContainer:
    RUN_CMD: str
    SQL_URL: str

    def __init__(self) -> None:
        """Initializer. Gets a likely free port, updates RUN_CMD/SQL_URL with it."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", 0))
            self._port = s.getsockname()[1]

        self.RUN_CMD = self.RUN_CMD.format(port=self._port)
        self.SQL_URL = self.SQL_URL.format(port=self._port)

    def _start_container(self) -> None:
        """Starts the docker container. Raises if it doesn't start correctly"""

        result = subprocess.run(
            f"docker run {self.RUN_CMD}", shell=True, capture_output=True
        )
        result.check_returncode()
        self._container_id = result.stdout.decode().strip()

    def _stop_container(self) -> None:
        """
        Stops the docker container if it is running
        """
        subprocess.run(
            f"docker remove {self._container_id} -f",
            shell=True,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

    @backoff.on_exception(
        backoff.constant,
        (DatabaseError, OperationalError, ConnectionAbortedError, FunctionTimedOut),
        max_tries=200,
        jitter=None,
        interval=0.5,
        logger=log,
        backoff_log_level=logging.DEBUG,
    )
    @func_set_timeout(3)
    def _wait_for_sql_stability(self):
        """
        Continually tries to connect to the mysql server until its fully ready

        Specifically wrapping with func_set_timeout because inspect() can hang if the server isn't ready.. in mssql for some reason.
        """
        engine = create_engine(self.SQL_URL)
        inspect(engine).get_table_names()

    def start(self) -> Optional[str]:
        """
        Returns sqlalchemy url after starting the container and waiting for stability.
        Returns None if we can't use docker.
        """
        if not has_docker():
            return None

        self._start_container()
        atexit.register(self._stop_container)
        self._wait_for_sql_stability()
        return self.SQL_URL


class DockerMySQL(StartOnceStopAtExitDockerContainer):
    RUN_CMD = '-e "MYSQL_ALLOW_EMPTY_PASSWORD=1" -e "MYSQL_PASSWORD=test" -e "MYSQL_USER=test" -e "MYSQL_DATABASE=test" -d -p {port}:3306 mysql:8'
    SQL_URL = "mysql+pymysql://test:test@localhost:{port}/test?charset=utf8mb4"


class DockerPostgres(StartOnceStopAtExitDockerContainer):
    RUN_CMD = '-e "POSTGRES_PASSWORD=test" -e "POSTGRES_USER=test" -e "POSTGRES_DB=test" -d -p {port}:5432 postgres:16'
    SQL_URL = "postgresql+psycopg2://test:test@localhost:{port}/test"


class DockerMSSQL(StartOnceStopAtExitDockerContainer):
    RUN_CMD = '-e "ACCEPT_EULA=Y" -e "MSSQL_SA_PASSWORD=testTest1" -p {port}:1433 -d mcr.microsoft.com/mssql/server:2022-latest'
    SQL_URL = "mssql+pymssql://sa:testTest1@localhost:{port}/?charset=utf8"


class DockerOracle(StartOnceStopAtExitDockerContainer):
    RUN_CMD = '-e "ORACLE_PASSWORD=test" -e "ORACLE_DATABASE=test" -e "APP_USER=test" -e "APP_USER_PASSWORD=test" -p {port}:1521 -d gvenzl/oracle-free:23-faststart'
    SQL_URL = "oracle+oracledb://test:test@localhost:{port}/?service_name=test"


@lru_cache(maxsize=None)
def _get_sqlalchemy_urls(sqlite_only: bool = False) -> List[str]:
    """
    Used to parameterize our testing.
    If we have docker available, sets up a mysqld container for our testing.

    Returns a list of sqlalchemy urls to test against.
    """
    urls = ["sqlite:///:memory:"]

    if sqlite_only:
        log.info("Skipping mysql setup because of sqlite-only")
    elif not has_docker():
        log.warning("Docker not available, not adding tests that require it.")
    else:
        dbs = [DockerMySQL, DockerPostgres, DockerMSSQL, DockerOracle]
        results = []

        with ThreadPoolExecutor() as executor:
            for db in dbs:
                d = db()
                results.append(executor.submit(d.start))

        urls += [r.result() for r in results if r.result()]

    assert None not in urls, "Something went wrong with setup"

    return urls


@pytest.fixture(scope="function")
def kvalchemy(request):
    """
    Fixture to get a kvalchemy instance for testing.

    After testing, cleans the database.
    """
    kva = None
    try:
        kva = KVAlchemy(request.param)
        yield kva
    finally:
        if kva:
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
        tag=" ",
    )


def pytest_generate_tests(metafunc):
    sqlite_only = metafunc.config.getoption("--sqlite-only")
    urls = _get_sqlalchemy_urls(sqlite_only)

    if "kvalchemy" in metafunc.fixturenames:
        metafunc.parametrize("kvalchemy", urls, indirect=True)


def pytest_addoption(parser):
    """
    Used to add options to pytest via the command line.
    """
    parser.addoption("--sqlite-only", action="store_true", default=False)
