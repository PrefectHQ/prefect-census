import pytest
from httpx import Response
from prefect import flow

from prefect_census.credentials import CensusCredentials
from prefect_census.runs import (
    CensusSyncRunCancelled,
    CensusSyncRunFailed,
    CensusSyncRunTimeout,
)
from prefect_census.syncs import (
    CensusSyncTriggerFailed,
    trigger_census_sync,
    trigger_census_sync_run_and_wait_for_completion,
)


@pytest.fixture
def census_credentials():
    return CensusCredentials(api_key="my_api_key")


class TestTriggerCensusSyncRun:
    async def test_trigger_sync(self, respx_mock, census_credentials):
        respx_mock.post(
            "https://app.getcensus.com/api/v1/syncs/45/trigger",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(
            return_value=Response(
                200, json={"data": {"sync_run_id": 45, "project_id": 12345}}
            )
        )

        @flow
        async def test_flow():
            return await trigger_census_sync(credentials=census_credentials, sync_id=45)

        result = await test_flow()
        assert result == 45

    async def test_trigger_nonexistent_sync(self, respx_mock, census_credentials):
        respx_mock.post(
            "https://app.getcensus.com/api/v1/syncs/46/trigger",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(
            return_value=Response(404, json={"status": {"user_message": "Not found!"}})
        )

        @flow
        async def test_trigger_nonexistent_job():
            await trigger_census_sync(census_credentials, sync_id=46)

        with pytest.raises(CensusSyncTriggerFailed, match="Not found!"):
            await test_trigger_nonexistent_job()


class TestTriggerCensusSyncRunAndWaitForCompletion:
    async def test_run_success(self, respx_mock, census_credentials):
        respx_mock.post(
            "https://app.getcensus.com/api/v1/syncs/5/trigger",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(
            return_value=Response(200, json={"data": {"id": 5, "sync_run_id": 12345}})
        )

        respx_mock.get(
            "https://app.getcensus.com/api/v1/sync_runs/12345",  # noqa
            headers={"Authorization": f"Bearer my_api_key"},
        ).mock(
            return_value=Response(
                200,
                json={"data": {"id": 5, "sync_run_id": 12345, "status": "completed"}},
            )
        )

        result = await trigger_census_sync_run_and_wait_for_completion(
            credentials=census_credentials, sync_id=5
        )

        assert result == {"id": 5, "status": "completed", "sync_run_id": 12345}

    def test_run_in_sync_flow(self, respx_mock, census_credentials):
        respx_mock.post(
            "https://app.getcensus.com/api/v1/syncs/5/trigger",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(
            return_value=Response(200, json={"data": {"id": 5, "sync_run_id": 12345}})
        )

        respx_mock.get(
            "https://app.getcensus.com/api/v1/sync_runs/12345",  # noqa
            headers={"Authorization": f"Bearer my_api_key"},
        ).mock(
            return_value=Response(
                200,
                json={"data": {"id": 5, "sync_run_id": 12345, "status": "completed"}},
            )
        )

        @flow
        def test_sync_flow():
            return trigger_census_sync_run_and_wait_for_completion(
                credentials=census_credentials, sync_id=5
            )

        result = test_sync_flow()

        assert result == {"id": 5, "status": "completed", "sync_run_id": 12345}

    async def test_run_success_with_wait(self, respx_mock, census_credentials):
        respx_mock.post(
            "https://app.getcensus.com/api/v1/syncs/5/trigger",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(
            return_value=Response(200, json={"data": {"id": 5, "sync_run_id": 12345}})
        )

        respx_mock.get(
            "https://app.getcensus.com/api/v1/sync_runs/12345",  # noqa
            headers={"Authorization": f"Bearer my_api_key"},
        ).mock(
            side_effect=[
                Response(
                    200,
                    json={"data": {"id": 5, "sync_run_id": 12345, "status": "working"}},
                ),
                Response(
                    200,
                    json={"data": {"id": 5, "sync_run_id": 12345, "status": "working"}},
                ),
                Response(
                    200,
                    json={
                        "data": {"id": 5, "sync_run_id": 12345, "status": "completed"}
                    },
                ),
            ]
        )

        result = await trigger_census_sync_run_and_wait_for_completion(
            credentials=census_credentials, sync_id=5, poll_frequency_seconds=2
        )

        assert result == {"id": 5, "status": "completed", "sync_run_id": 12345}

    async def test_run_failure_with_wait(self, respx_mock, census_credentials):
        respx_mock.post(
            "https://app.getcensus.com/api/v1/syncs/5/trigger",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(
            return_value=Response(200, json={"data": {"id": 5, "sync_run_id": 12345}})
        )

        respx_mock.get(
            "https://app.getcensus.com/api/v1/sync_runs/12345",  # noqa
            headers={"Authorization": f"Bearer my_api_key"},
        ).mock(
            side_effect=[
                Response(
                    200,
                    json={"data": {"id": 5, "sync_run_id": 12345, "status": "working"}},
                ),
                Response(
                    200,
                    json={"data": {"id": 5, "sync_run_id": 12345, "status": "working"}},
                ),
                Response(
                    200,
                    json={"data": {"id": 5, "sync_run_id": 12345, "status": "failed"}},
                ),
            ]
        )

        with pytest.raises(CensusSyncRunFailed):
            await trigger_census_sync_run_and_wait_for_completion(
                credentials=census_credentials, sync_id=5, poll_frequency_seconds=2
            )

    async def test_run_failure_no_run_id(self, respx_mock, census_credentials):
        respx_mock.post(
            "https://app.getcensus.com/api/v1/syncs/5/trigger",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(
            return_value=Response(200, json={"data": {"id": 5, "status": "completed"}})
        )

        with pytest.raises(KeyError, match="sync_run_id"):
            await trigger_census_sync_run_and_wait_for_completion(
                credentials=census_credentials, sync_id=5, poll_frequency_seconds=2
            )

    async def test_run_cancelled_with_wait(self, respx_mock, census_credentials):
        respx_mock.post(
            "https://app.getcensus.com/api/v1/syncs/5/trigger",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(
            return_value=Response(200, json={"data": {"id": 5, "sync_run_id": 12345}})
        )

        respx_mock.get(
            "https://app.getcensus.com/api/v1/sync_runs/12345",  # noqa
            headers={"Authorization": f"Bearer my_api_key"},
        ).mock(
            side_effect=[
                Response(
                    200,
                    json={"data": {"id": 5, "sync_run_id": 12345, "status": "working"}},
                ),
                Response(
                    200,
                    json={"data": {"id": 5, "sync_run_id": 12345, "status": "working"}},
                ),
                Response(
                    200,
                    json={
                        "data": {"id": 5, "sync_run_id": 12345, "status": "cancelled"}
                    },
                ),
            ]
        )

        with pytest.raises(CensusSyncRunCancelled):
            await trigger_census_sync_run_and_wait_for_completion(
                credentials=census_credentials, sync_id=5, poll_frequency_seconds=2
            )

    async def test_run_timed_out(self, respx_mock, census_credentials):
        respx_mock.post(
            "https://app.getcensus.com/api/v1/syncs/5/trigger",
            headers={"Authorization": "Bearer my_api_key"},
        ).mock(
            return_value=Response(200, json={"data": {"id": 5, "sync_run_id": 12345}})
        )

        respx_mock.get(
            "https://app.getcensus.com/api/v1/sync_runs/12345",  # noqa
            headers={"Authorization": f"Bearer my_api_key"},
        ).mock(
            side_effect=[
                Response(
                    200,
                    json={"data": {"id": 5, "sync_run_id": 12345, "status": "working"}},
                ),
                Response(
                    200,
                    json={"data": {"id": 5, "sync_run_id": 12345, "status": "working"}},
                ),
                Response(
                    200,
                    json={"data": {"id": 5, "sync_run_id": 12345, "status": "working"}},
                ),
                Response(
                    200,
                    json={"data": {"id": 5, "sync_run_id": 12345, "status": "working"}},
                ),
            ]
        )

        with pytest.raises(CensusSyncRunTimeout):
            await trigger_census_sync_run_and_wait_for_completion(
                credentials=census_credentials,
                sync_id=5,
                poll_frequency_seconds=1,
                max_wait_seconds=3,
            )
