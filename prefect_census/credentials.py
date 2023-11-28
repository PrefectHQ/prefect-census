"""Module containing credentials for interacting with Census."""
from prefect.blocks.abstract import CredentialsBlock
from pydantic import VERSION as PYDANTIC_VERSION

if PYDANTIC_VERSION.startswith("2."):
    from pydantic.v1 import Field, SecretStr
else:
    from pydantic import Field, SecretStr

from prefect_census.client import CensusClient


class CensusCredentials(CredentialsBlock):
    """
    Credentials block for credential use across Census tasks and flows.

    Attributes:
        api_key: API key to authenticate with the Census
            API. Refer to the [Census authentication docs](
            https://docs.getcensus.com/basics/api#getting-api-access)
            for retrieving the API key.

    Examples:
        Load stored Census credentials:
        ```python
        from prefect_census import CensusCredentials

        census_creds = CensusCredentials.load("BLOCK_NAME")
        ```

        Use CensusCredentials instance to trigger a sync run:
        ```python
        import asyncio
        from prefect import flow
        from prefect_census import CensusCredentials

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

        from prefect_census import CensusCredentials
        from prefect_census.syncs import trigger_census_sync

        @flow
        def trigger_census_sync_run_flow():
            credentials = CensusCredentials.load("my-census-credentials")
            trigger_census_sync(credentials=credentials, sync_id=42)

        trigger_census_sync_run_flow()
        ```
    """

    _block_type_name = "Census Credentials"
    _documentation_url = "https://prefecthq.github.io/prefect-census/credentials/"
    _logo_url = "https://cdn.sanity.io/images/3ugk85nk/production/b2f805555c778f37f861b67c4a861908f66a6e35-700x700.png"  # noqa

    api_key: SecretStr = Field(
        ..., title="API Key", description="API key to authenticate with the Census API."
    )

    def get_client(self) -> CensusClient:
        """
        Provides an authenticated client for working with the Census API.

        Returns:
            A authenticated Census API client
        """
        return CensusClient(api_key=self.api_key.get_secret_value())
