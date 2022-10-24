"""Module containing client for interacting with the Census API"""
from httpx import AsyncClient, Response
from typing import Any, Optional


class CensusClient:
    """
    Client for interacting with the Census API.
    
    Args:
        api_key: API key to authenticate with the Census API.
    """
    def __init__(self, api_key: str):
        self._closed = False
        self._started = False

        self.client = AsyncClient(
            base_url=f"https://app.getcensus.com", 
            headers={"Authorization": f"Bearer {api_key}"})

    async def call_endpoint(
        self,
        http_method: str,
        path: str,
        headers: Optional[dict[str, Any]] = None,
    ) -> Response:
        """
        Call an endpoint in the Census API.
        
        Args:
            http_method: HTTP method to call on the endpoint.
            path: The partial path for request (e.g. //api/v1/syncs/42). Will be
            appended onto the base URL as determined by the client configuration. 
            params: Query params to include in the request.
            
        Returns:
            The response from the Census API.
        """

        response = await self.client.request(
            method=http_method, url=path, headers=headers,
        )
        response.raise_for_status()
        return response

    async def get_run_info(self, run_id: int):
        """
        Sends a request to the [get sync id info endpoint]
        (https://docs.getcensus.com/basics/api/syncs#get-syncs-id)
        
        Args:
            run_id: The ID of the sync run to get details for.
            
        Returns:
            The response from the Census API.
        """  # noqa
        return await self.call_endpoint(http_method="GET", path=f"/api/v1/sync_runs/{run_id}")

    async def trigger_sync_run(self, sync_id: int):
        """
        Sends a request to the [trigger sync run endpoint]
        (https://docs.getcensus.com/basics/api/sync-runs)
        to initiate a sync run.
        
        Args:
            sync_id: The ID of the sync to trigger.
            
        Returns:
            The response from the Census API.
        """  # noqa

        return await self.call_endpoint(
            http_method="POST",
            path=f"/api/v1/syncs/{sync_id}/trigger"
        )

    async def __aenter__(self):
        if self._closed:
            raise RuntimeError(
                "The client cannot be started again after it has been closed."
            )
        if self._started:
            raise RuntimeError("The client cannot be started more than once.")
        
        self._started = True
        return self

    async def __aexit__(self, *exc):
        self._closed = True
        await self.client.__aexit__()
