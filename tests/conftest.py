import pytest

from prefect_census.credentials import CensusCredentials

@pytest.fixture
def census_credentials():
    return CensusCredentials(api_key="my_api_key")