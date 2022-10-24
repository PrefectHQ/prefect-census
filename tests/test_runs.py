import pytest
from httpx import Response
from prefect import flow

from prefect_census.runs import CensusGetSyncRunInfoFailed, get_census_sync_run_info


class TestGetCensusSyncRunInfo:
    async def test_get_census_sync_run_info(self, respx_mock, census_credentials):
        respx_mock.get(
            "https://app.getcensus.com/api/v1/sync_runs/42",  # noqa
            headers={"Authorization": f"Bearer my_api_key"},
        ).mock(return_value=Response(200, json={"data": {"sync_run_id": 42}}))

        response = await get_census_sync_run_info.fn(
            credentials=census_credentials, run_id=42
        )

    async def test_get_nonexistent_run(self, respx_mock, census_credentials):
        respx_mock.get(
            "https://app.getcensus.com/api/v1/sync_runs/4",  # noqa
            headers={"Authorization": f"Bearer my_api_key"},
        ).mock(
            return_value=Response(404, json={"status": {"user_message": "Not found!"}})
        )
        with pytest.raises(CensusGetSyncRunInfoFailed, match="Not found!"):
            await get_census_sync_run_info.fn(credentials=census_credentials, run_id=4)
