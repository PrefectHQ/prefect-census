"""Module containing tasks and flows for interacting with Census syncs"""
from prefect import task
from credentials import CensusCredentials
from httpx import HTTPStatusError
from prefect.logging import get_run_logger
from utils import extract_user_message

class CensusGetSyncRunInfoFailed(RuntimeError):
    """Used to idicate retrieve sync run info."""

    pass


class CensusSyncTriggerFailed(RuntimeError):
    """Used to indicate sync triggered."""

    pass


@task(
    name="Get Census sync run details",
    description="Retrieves details of a Census sync run"
    "for the sync with the given sync_id.",
    retries=3,
    retry_delay_seconds=10
)
async def get_census_sync_run_info(credentials: CensusCredentials, sync_id: int):
    """
    A task to retrieve information a Census sync run.
    
    Args:
        credentials: Credentials for authenticating with Census.
        sync_id: The ID of the sync to trigger.
        
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
                sync_id=42
            )

        get_sync_run_info_flow()
        ```
    """  # noqa
    try:
        async with credentials.get_client() as client:
            response = await client.get_run_info(sync_id)
    except HTTPStatusError as e:
        raise CensusGetSyncRunInfoFailed(extract_user_message(e)) from e

    return response.json()["data"]


@task(
    name="Trigger Census sync run",
    description="Triggers a Census sync run for the sync "
    "with the given sync_id.",
    retries=3,
    retry_delay_seconds=10
)
async def trigger_census_sync(credentials: CensusCredentials, sync_id: int) -> str: # is it a str?
    """
    A task to trigger a Census sync run.
    
    Args:
        credentials: Credentials for authenticating with Census.
        sync_id: The ID of the sync to trigger.
        
    Returns:
        The run data returned from the Census API.
    
    Examples:
        Trigger a Census sync run:
        ```python
        from prefect import flow

        from prefect_census.credentials import CensusCredentials
        from prefect_census.syncs import trigger_census_sync

        @flow
        def trigger_census_sync_flow():
            credentials = CensusCredentials(api_key="my_api_key")
            trigger_census_sync(credentials=credentials, sync_id=42)

        trigger_census_sync_flow()
        ```
    """  # noqa
    logger = get_run_logger()

    logger.info(f"Triggering Census sync run for sync with ID {sync_id}")
    try:
        async with credentials.get_client() as client:
            response = await client.trigger_sync_run(sync_id=sync_id)
    except HTTPStatusError as e:
        raise CensusSyncTriggerFailed(extract_user_message(e)) from e

    run_data = response.json()["data"]
    
    if "id" in run_data:
        logger.info(
            f"Census sync run successfully triggered for sync with ID {id}. "
            "You can view the status of this sync run at "
            f"https://app.getcensus.com/sync/{id}/sync-history"
        )

    return run_data["sync_run_id"]
