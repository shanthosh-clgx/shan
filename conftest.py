import json
import logging
import os
from typing import Any, AsyncGenerator

import aiohttp
import pytest
import pytest_asyncio
from aiohttp import ClientSession
from dotenv import dotenv_values

logger = logging.getLogger(__name__)
dotenv_config = dotenv_values(".env")


@pytest.fixture(scope="session")
def airflow_logins() -> dict[str, Any]:
    """Airflow URL and login credentials.

    The default Airflow URL is 'http://localhost:8080/'.
    To override it, specify the URL in a .env file.

    Returns:
        dict[str, Any]: Airflow URL and login credentials
    """
    airflow_url = dotenv_config.get("AIRFLOW_URL", os.getenv("AIRFLOW_URL"))
    airflow_username = os.getenv(
        "AIRFLOW_USERNAME", dotenv_config.get("AIRFLOW_USERNAME")
    )
    airflow_password = os.getenv(
        "AIRFLOW_PASSWORD", dotenv_config.get("AIRFLOW_PASSWORD")
    )
    return {
        "airflow_url": airflow_url,
        "http_basic_auth": aiohttp.BasicAuth(
            login=airflow_username,
            password=airflow_password,
        ),
    }


@pytest.fixture(scope="session")
def trigger_payload() -> dict:
    """
    Reads the trigger payload from 'tests/integration/trigger_payload.json'.
    """
    with open("tests/integration/trigger_payload.json") as f:
        payload = json.load(f)
        return payload


def pytest_addoption(parser):
    parser.addoption("--branch-name", action="store", default="dev")
    parser.addoption("--dag", action="store")


@pytest.fixture(scope="session")
def branch_name(request):
    branch_name = request.config.getoption("--branch-name")
    yield branch_name

@pytest_asyncio.fixture
async def airflow_session(
    airflow_logins: dict,
) -> AsyncGenerator[ClientSession, None]:
    """Creates an aiohttp session with the Airflow instance.

    Args:
        airflow_logins: Airflow URL and login credentials.

    Yields:
        An aiohttp session.
    """
    async with ClientSession(auth=airflow_logins["http_basic_auth"]) as session:
        yield session
