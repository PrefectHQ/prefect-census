"""Module containing credentials for interacting with Census."""
from httpx import AsyncClient
from prefect.blocks.core import Block
from pydantic import SecretStr

from prefect_census.client import CensusClient


class CensusCredentials(Block):
    """
    Credentials block for credential use across Census tasks and flows.

    Attributes:
        api_key: API key to authenticate with the Census
            API. Refer to the [Authentication docs](
            https://docs.getcensus.com/basics/api#getting-api-access)
            for retrieving the API key.

    Examples:
        Load stored Census credentials:
        ```python
        from prefect_census.client import CensusCredentials

        census_creds = CensusCredentials.load("BLOCK_NAME")
        ```

        Use CensusCredentials instance to trigger a sync run:
        ```python
        import asyncio
        from prefect import flow
        from prefect_census.credentials import CensusCredentials

        credentials = CensusCredentials(api_key="my_api_key")

        @flow
        async def trigger_sync_run_flow():
            async with credentials.get_client() as client:
                await client.trigger_sync_run(sync_id=42)

        asyncio.run(trigger_sync_run_flow())
        ```

        Load saved Census credentials within a flow:
        ```python
        from prefect import flow

        from prefect_census.credentials import CensusCredentials
        from prefect_census.syncs import trigger_census_sync

        @flow
        def trigger_census_sync_run_flow():
            credentials = CensusCredentials.load("my-census-credentials")
            trigger_census_sync(credentials=credentials, sync_id=42)

        trigger_census_sync_run_flow()
        ```
    """

    _block_type_name = "Census Credentials"
    _logo_url = "https://res.cloudinary.com/crunchbase-production/image/upload/c_lpad,f_auto,q_auto:eco,dpr_1/llmjpn8a0pgu8szjmnyi"  # noqa

    api_key: SecretStr

    def get_client(self) -> AsyncClient:
        """
        Returns a newly instantiated client for working with the Census API.
        """
        return CensusClient(api_key=self.api_key.get_secret_value())
