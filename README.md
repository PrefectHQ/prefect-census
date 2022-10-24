# prefect-census

<p align="center">
    <a href="https://pypi.python.org/pypi/prefect-census/" alt="PyPI version">
        <img alt="PyPI" src="https://img.shields.io/pypi/v/prefect-census?color=0052FF&labelColor=090422"></a>
    <a href="https://github.com/PrefectHQ/prefect-census/" alt="Stars">
        <img src="https://img.shields.io/github/stars/PrefectHQ/prefect-census?color=0052FF&labelColor=090422" /></a>
    <a href="https://pepy.tech/badge/prefect-census/" alt="Downloads">
        <img src="https://img.shields.io/pypi/dm/prefect-census?color=0052FF&labelColor=090422" /></a>
    <a href="https://github.com/PrefectHQ/prefect-census/pulse" alt="Activity">
        <img src="https://img.shields.io/github/commit-activity/m/PrefectHQ/prefect-census?color=0052FF&labelColor=090422" /></a>
    <br>
    <a href="https://prefect-community.slack.com" alt="Slack">
        <img src="https://img.shields.io/badge/slack-join_community-red.svg?color=0052FF&labelColor=090422&logo=slack" /></a>
    <a href="https://discourse.prefect.io/" alt="Discourse">
        <img src="https://img.shields.io/badge/discourse-browse_forum-red.svg?color=0052FF&labelColor=090422&logo=discourse" /></a>
</p>

## Welcome!

Prefect integrations for working with Census syncs

## Getting Started

### Python setup

Requires an installation of Python 3.7+.

We recommend using a Python virtual environment manager such as pipenv, conda or virtualenv.

These tasks are designed to work with Prefect 2.0. For more information about how to use Prefect, please refer to the [Prefect documentation](https://orion-docs.prefect.io/).

### Installation

Install `prefect-census` with `pip`:

```bash
pip install prefect-census
```

Then, register to [view the block](https://orion-docs.prefect.io/ui/blocks/) on Prefect Cloud:

```bash
prefect block register -m prefect_census
```

Note, to use the `load` method on Blocks, you must already have a block document [saved through code](https://orion-docs.prefect.io/concepts/blocks/#saving-blocks) or [saved through the UI](https://orion-docs.prefect.io/ui/blocks/).

### Write and run a flow
Trigger Census sync run and wait for completion:

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

Get Census sync run info:
```python
from prefect import flow

from prefect_census import CensusCredentials
from prefect_census.runs import get_census_sync_run_info

@flow
def get_sync_run_info_flow():
    credentials = CensusCredentials(api_key="my_api_key")

    return get_census_sync_run_info(
        credentials=credentials,
        run_id=42
    )

get_sync_run_info_flow()
```

## Resources

If you encounter any bugs while using `prefect-census`, feel free to open an issue in the [prefect-census](https://github.com/PrefectHQ/prefect-census) repository.

If you have any questions or issues while using `prefect-census`, you can find help in either the [Prefect Discourse forum](https://discourse.prefect.io/) or the [Prefect Slack community](https://prefect.io/slack).

Feel free to ⭐️ or watch [`prefect-census`](https://github.com/PrefectHQ/prefect-census) for updates too!

## Development

If you'd like to install a version of `prefect-census` for development, clone the repository and perform an editable install with `pip`:

```bash
git clone https://github.com/PrefectHQ/prefect-census.git

cd prefect-census/

pip install -e ".[dev]"

# Install linting pre-commit hooks
pre-commit install
```
