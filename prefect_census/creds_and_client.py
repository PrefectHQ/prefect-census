import os
from typing import Any, Dict, Optional

from httpx import AsyncClient, Response
from prefect.blocks.core import Block
from pydantic import SecretStr


class CensusClient:
    def __init__(
        self,
        token,
    ):

        self.client = AsyncClient(base_url=f"https://bearer:{token}@app.getcensus.com")

    async def call_endpoint(
        self,
        http_method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json: Optional[Dict[str, Any]] = None,
    ) -> Response:
        """Generic HTTP call against Census API."""
        response = await self.client.request(
            method=http_method, url=path, params=params, json=json
        )
        response.raise_for_status()
        return response


class CensusCredentials(Block):
    secret_token: SecretStr

    def get_client(self) -> AsyncClient:

        return CensusClient(token=self.secret_token.get_secret_value())


class CensusGetSyncRunInfoFailed(RuntimeError):
    """Used to idicate retrieve sync run info."""


class CensusSyncTriggerFailed(RuntimeError):
    """Used to indicate sync triggered."""


async def get_census_sync_run_info(creds_block, sync_id):
    try:
        endpoint = f"/api/v1/syncs/{sync_id}"
        method = "GET"

        client = creds_block.get_client()
        response = await client.call_endpoint(method, endpoint)
    except:
        raise CensusGetSyncRunInfoFailed

    return response.json()["data"]


async def trigger_census_sync(creds_block, sync_id):
    try:
        endpoint = f"/api/v1/syncs/{sync_id}/trigger"
        method = "POST"

        client = creds_block.get_client()
        response = await client.call_endpoint(method, endpoint)
    except:
        raise CensusSyncTriggerFailed

    return response.json()["data"]["sync_run_id"]


if __name__ == "__main__":
    import asyncio

    census_creds_block = CensusCredentials(secret_token=os.environ["CENSUS_API_KEY"])

    c = census_creds_block.get_client()
    # r = asyncio.run(c.call_endpoint(http_method="GET", path=f"/api/v1/syncs/38417"))
    # asyncio.run(get_census_sync_run_info(census_creds_block, sync_id=38417))
    asyncio.run(trigger_census_sync(census_creds_block, sync_id=38417))
