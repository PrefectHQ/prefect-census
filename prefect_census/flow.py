

# import asyncio
# import os
# census_creds_block = CensusCredentials(api_key=os.environ["CENSUS_API_KEY"])

# c = census_creds_block.get_client()
# print("Call endpoint:", asyncio.run(c.call_endpoint(http_method="GET", path=f"/api/v1/syncs/38417")))

# print("Trigger_census_sync_run:", asyncio.run(trigger_census_sync_run.fn(census_creds_block, sync_id=38417)))
# print("Get census sync run info:", asyncio.run(get_census_sync_run_info.fn(census_creds_block, sync_id=38417)))

# from prefect import flow
    
# from prefect_census.credentials import CensusCredentials
# from prefect_census.syncs import trigger_census_sync

# @flow
# def trigger_census_sync_run_flow():
#     credentials = CensusCredentials.load("my-census-credentials")
#     trigger_census_sync(credentials=credentials, sync_id=38417)
    
# trigger_census_sync_run_flow()

# import asyncio
# from prefect import flow
# from prefect_census.credentials import CensusCredentials

# credentials = CensusCredentials(api_key="my-api-key")

# @flow
# async def trigger_sync_run_flow(): 
#     async with credentials.get_client() as client:
#         await client.trigger_sync_run(sync_id=42)

# asyncio.run(trigger_sync_run_flow())



# from prefect import flow

# from prefect_census.credentials import CensusCredentials
# from prefect_census.syncs import trigger_census_sync

# @flow
# def trigger_census_sync_run_flow():
#     credentials = CensusCredentials.load("my-census-credentials")
#     trigger_census_sync(credentials=credentials, sync_id=42)
    
# trigger_census_sync_run_flow()

# from prefect import flow
        
# from prefect_census.credentials import CensusCredentials
# from prefect_census.syncs import get_census_sync_run_info

# @flow
# def get_sync_run_info_flow():
#     credentials = CensusCredentials(api_key="my_api")
    
#     return get_census_sync_run_info(
#         credentials=credentials,
#         sync_id=42
#     )

# get_sync_run_info_flow()

# from prefect import flow

# from prefect_census.credentials import CensusCredentials
# from prefect_census.syncs import trigger_census_sync

# @flow
# def trigger_census_sync_flow():
#     credentials = CensusCredentials(api_key="my_api_key")
#     trigger_census_sync(credentials=credentials, sync_id=42)

# trigger_census_sync_flow()

# import os
# from prefect import flow
# from prefect_census.credentials import CensusCredentials
# from prefect_census.syncs import trigger_census_sync_run_and_wait_for_completion

# @flow
# def my_flow():
#     creds = CensusCredentials(api_key=os.environ["CENSUS_API_KEY"])
#     run_result = trigger_census_sync_run_and_wait_for_completion(
#         credentials=creds,
#         sync_id=38417
#     )
#     print("run_result:", run_result)
# my_flow()

import os

# import asyncio
        
# from prefect_census.credentials import CensusCredentials
# from prefect_census.syncs import trigger_census_sync_run_and_wait_for_completion

# asyncio.run(
#     trigger_census_sync_run_and_wait_for_completion(
#         credentials=CensusCredentials(
#             api_key=os.environ["CENSUS_API_KEY"]
#         ),
#         sync_id=38417
#     )
# )


from prefect import flow

from prefect_census.credentials import CensusCredentials
from prefect_census.runs import get_census_sync_run_info

@flow
def get_sync_run_info_flow():
    credentials = CensusCredentials(api_key=os.environ["CENSUS_API_KEY"])
    
    return get_census_sync_run_info(
        credentials=credentials,
        run_id=69834024
    )

print(get_sync_run_info_flow())