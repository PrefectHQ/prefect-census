# add wait_census_sync_completion flow
# https://github.com/PrefectHQ/prefect-dbt/blob/e3ff884ec348aaf95b8dd842dd90bf7276011db4/prefect_dbt/cloud/runs.py#L267

# trigger_census_sync_and_wait_for_completion flow
# https://github.com/PrefectHQ/prefect-dbt/blob/e3ff884ec348aaf95b8dd842dd90bf7276011db4/prefect_dbt/cloud/jobs.py#L179

"""Module containing tasks and flows for interacting with Census sync runs"""
from enum import Enum
from httpx import HTTPStatusError
from prefect import flow, task
from prefect.logging import get_run_logger
from credentials import CensusCredentials
from utils import extract_user_message


class CensusSyncRunFailed(RuntimeError):
    """Raised when unable to retrieve dbt Cloud run"""

    pass


class CensusSyncRunTimeout(RuntimeError):
    """
    Raised when a triggered job run does not complete in the configured max
    wait seconds
    """

    pass


class CensusGetSyncRunInfoFailed(RuntimeError):
    """Used to idicate retrieve sync run info."""

    pass


class CensusSyncRunStatus(Enum):
    """Census Sync statuses."""

    @classmethod
    def is_terminal_status_code(self, status_code: str) -> bool:
        """
        Returns True if a status code is terminal for a job run.
        Returns False otherwise.
        """
        return status_code in "completed", "failed"


@task(
    name="Get Census sync run details",
    description="Retrieves details of a Census sync run"
    "for the sync with the given sync_id.",
    retries=3,
    retry_delay_seconds=10
)
async def get_census_sync_run_info(credentials: CensusCredentials, run_id: int):
    """
    A task to retrieve information a Census sync run.
    
    Args:
        credentials: Credentials for authenticating with Census.
        run_id: The ID of the run of the sync to trigger.
        
    Returns:
        The run data returned by the Census API.
        
    Example:
        Get Census sync run info:
        ```python
        from prefect import flow
        
        from prefect_census.credentials import CensusCredentials
        from prefect_census.syncs import get_census_sync_run_info

        @flow
        def get_sync_run_info_flow():
            credentials = CensusCredentials(api_key="my_api")
            
            return get_census_sync_run_info(
                credentials=credentials,
                run_id=424242
            )

        get_sync_run_info_flow()
        ```
    """  # noqa
    try:
        async with credentials.get_client() as client:
            response = await client.get_run_info(run_id)
    except HTTPStatusError as e:
        raise CensusGetSyncRunInfoFailed(extract_user_message(e)) from e

    return response.json()["data"]


@flow(
    name="Wait for Census sync run",
    description="Waits for the Census sync run to finish running.",
)
async def wait_census_sync_completion(
    run_id: int,
    credentials: CensusCredentials,
    max_wait_seconds: int = 1, # start at 900
    poll_frequency_seconds: int = 1,
): # what will it return?
    """
    Wait for the given Census sync run to finish running.
    
    Args:
        run_id: The ID of the sync run to wait for.
        credentials: Credentials for authenticating with Census.
        max_wait_seconds: Maximum number of seconds to wait for sync to complete.
        poll_frequency_seconds: Number of seconds to wait in between checks for run completion.
        
    Raises:
        CensusSyncRunTimeout: When the elapsed wait time exceeds `max_wait_seconds`.
    
    Returns:
        run_data: A dictionary containing information about the run after completion.
    """
    logger = get_run_logger()
    seconds_waited_for_run_completion = 1
    while seconds_waited_for_run_completion <= max_wait_seconds:
        run_data_future = await get_census_sync_run_info.submit(
            credentials=credentials,
            run_id=run_id,
        )
        run_data = await run_data_future.result()
        run_status_code = run_data.get("status")

        if CensusSyncRunStatus.is_terminal_status_code(status_code=run_status_code):
            return run_data
        
        logger.info(
            f"Census sync run with ID %i has status %s. Waiting for %i seconds.",
            run_id,
            CensusSyncRunStatus(run_status_code).name,
            poll_frequency_seconds,
        )
        await asyncio.sleep(poll_frequency_seconds)
        seconds_waited_for_run_completion += poll_frequency_seconds
    
    raise CensusSyncRunTimeout(
        f"Max wait time of {max_wait_seconds} seconds exceeded while waiting "
        f"for sync run with ID {run_id}"
    )

if __name__ == "__main__":
    import asyncio
    credentials = CensusCredentials(api_key="secret-token:94Vh6rAP3TxW9sJrbftX96K9")
    print(asyncio.run(wait_census_sync_completion(run_id=69786658, credentials=credentials)))
