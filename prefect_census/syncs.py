"""Module containing tasks and flows for interacting with Census syncs."""
from typing import Any, Dict

from httpx import HTTPStatusError
from prefect import flow, task
from prefect.logging import get_run_logger

from prefect_census.credentials import CensusCredentials
from prefect_census.runs import (
    CensusSyncRunCancelled,
    CensusSyncRunFailed,
    CensusSyncRunStatus,
    wait_census_sync_completion,
)
from prefect_census.utils import extract_user_message


class CensusSyncTriggerFailed(RuntimeError):
    """Used to indicate sync triggered."""


@task(
    name="Trigger Census sync run",
    description="Triggers a Census sync run for the sync with the given sync_id.",
    retries=3,
    retry_delay_seconds=10,
)
async def trigger_census_sync(
    credentials: CensusCredentials, sync_id: int
) -> Dict[str, Any]:
    """
    A task to trigger a Census sync run.

    Args:
        credentials: Credentials for authenticating with Census.
        sync_id: The ID of the sync to trigger.

    Returns:
        The run data returned from the Census API.
        Keys:
            - status
            - data
                - id
                - sync_id
                - source_record_count
                - records_processed
                - records_updated
                - records_failed
                - records_invalid
                - created_at
                - updated_at
                - completed_at
                - scheduled_execution_time
                - error_code
                - error_message
                - error_detail
                - status
                - canceled
                - full_sync
                - sync_trigger_reason
                - ui_tag
                - ui_detail
                
    Examples:
        Trigger a Census sync run:
        ```python
        from prefect import flow

        from prefect_census import CensusCredentials
        from prefect_census.syncs import trigger_census_sync

        @flow
        def trigger_census_sync_flow():
            credentials = CensusCredentials(api_key="my_api_key")
            trigger_census_sync(credentials=credentials, sync_id=42)

        trigger_census_sync_flow()
        ```
    """
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


@task(
    name="Get Census sync run id",
    description="Extracts the run ID from a trigger sync run API response",
)
def get_run_id(obj: Dict[str, Any]) -> int:
    """
    Task that extracts the run ID from a trigger sync run API response.

    This task is mainly used to maintain dependency tracking between the
    `trigger_census_sync_run` task and downstream task/flows that use the run ID.

    Args:
        obj: The JSON body from the trigger sync run response.

    Example:
        ```python
        from prefect import flow
        from prefect_census import CensusCredentials
        from prefect_census.syncs import trigger_census_sync_run, get_run_id


        @flow
        def trigger_sync_run_and_get_id():
            credentials = CensusCredentials(
                api_key="my_api_key"
            )

            triggered_run_data = trigger_census_sync_run(
                credentials=credentials,
                run_id=run_id
            )
            run_id = get_run_id.submit(triggered_run_data)
            return run_id

        trigger_sync_run_and_get_id()
        ```
    """
    id = obj.get("id")
    if id is None:
        raise RuntimeError("Unable to determine run ID for triggered sync.")
    return id


@flow(
    name="Trigger Census sync run and wait for completion",
    description="Triggers a Census sync run and waits for the"
    "triggered run to complete.",
)
async def trigger_census_sync_run_and_wait_for_completion(
    credentials: CensusCredentials,
    sync_id: int,
    max_wait_seconds: int = 900,
    poll_frequency_seconds: int = 10,
) -> dict:
    """
    Flow that triggers a sync run and waits for the triggered run to complete.

    Args:
        credentials: Credentials for authenticating with Census.
        sync_id: The ID of the sync to trigger.
        max_wait_seconds: Maximum number of seconds to wait for sync to complete
        poll_frequency_seconds: Number of seconds to wait in between checks for run completion.

    Raises:
        CensusSyncRunCancelled: The triggered Census sync run was cancelled.
        CensusSyncRunFailed: The triggered Census sync run failed.
        RuntimeError: The triggered Census sync run ended in an unexpected state.

    Returns:
        The run data returned by the Census API.
        Keys: 
            - status
            - data
                sync_run_id

    Examples:
        Trigger a Census sync using CensusCredentials instance and wait
        for completion as a standalone flow:
        ```python
        import asyncio

        from prefect_census import CensusCredentials
        from prefect_census.syncs import trigger_census_sync_run_and_wait_for_completion

        asyncio.run(
            trigger_census_sync_run_and_wait_for_completion(
                credentials=CensusCredentials(
                    api_key="my_api_key"
                ),
                sync_id=42
            )
        )
        ```

        Trigger a Census sync and wait for completion as a subflow:
        ```python
        from prefect import flow

        from prefect_census import CensusCredentials
        from prefect_census.syncs import trigger_census_sync_run_and_wait_for_completion

        @flow
        def my_flow():
            ...
            creds = CensusCredentials(api_key="my_api_key")
            run_result = trigger_census_sync_run_and_wait_for_completion(
                credentials=creds,
                sync_id=42
            )
            ...

        my_flow()
        ```
    """  # noqa
    logger = get_run_logger()

    triggered_run_data_future = await trigger_census_sync.submit(
        credentials=credentials, sync_id=sync_id
    )

    run_id = await triggered_run_data_future.result()
    if run_id is None:
        raise RuntimeError("Unable to determine run ID for triggered sync.")

    final_run_status, run_data = await wait_census_sync_completion(
        run_id=run_id,
        credentials=credentials,
        max_wait_seconds=max_wait_seconds,
        poll_frequency_seconds=poll_frequency_seconds,
    )

    if final_run_status == CensusSyncRunStatus.COMPLETED:
        logger.info(
            "Census sync run with ID %s completed successfully!",
            run_id,
        )
        return run_data

    elif final_run_status == CensusSyncRunStatus.CANCELLED:
        raise CensusSyncRunCancelled(
            f"Triggered sync run with ID {run_id} was cancelled."
        )
    elif final_run_status == CensusSyncRunStatus.FAILED:
        raise CensusSyncRunFailed(f"Triggered sync run with ID: {run_id} failed.")
    else:
        raise RuntimeError(
            f"Triggered sync run with ID: {run_id} ended with unexpected"
            f"status {final_run_status}"
        )
