import logging
from typing import Any, Callable

import pandas as pd
import requests
from firecloud import api as firecloud_api

from arret.utils import maybe_retry


class TerraWorkspace:
    def __init__(self, workspace_namespace: str, workspace_name: str) -> None:
        self.workspace_namespace = workspace_namespace
        self.workspace_name = workspace_name

    def get_bucket_name(self) -> str:
        """
        Retrieves the name of the bucket associated with the Terra workspace.

        :return: bucket name
        """

        j = call_firecloud_api(
            firecloud_api.get_workspace,
            namespace=self.workspace_namespace,
            workspace=self.workspace_name,
            fields="workspace.bucketName",
        )

        return j["workspace"]["bucketName"]

    def get_entity_types(self) -> list[str]:
        """
        Retrieves a list of entity types associated with the Terra workspace.

        :return: list of entity type names
        """

        j = call_firecloud_api(
            firecloud_api.list_entity_types,
            namespace=self.workspace_namespace,
            workspace=self.workspace_name,
        )

        return list(j.keys())

    def get_entities(self, entity_type: str) -> pd.DataFrame:
        """
        Get a workspace entity as a data frame.

        :param entity_type: an entityt type (e.g. "sample")
        :return: a data frame of entities
        """

        j = call_firecloud_api(
            firecloud_api.get_entities,
            namespace=self.workspace_namespace,
            workspace=self.workspace_name,
            etype=entity_type,
        )

        records = [{f"{entity_type}_id": x["name"], **x["attributes"]} for x in j]

        return pd.DataFrame(records)


def call_firecloud_api(
    func: Callable, max_retries: int = 3, *args: Any, **kwargs: Any
) -> Any:
    """
    Call a Firecloud API endpoint and check the response for a valid HTTP status code.

    :param func: a `firecloud.api` method
    :param max_retries: an optional maximum number of times to retry
    :param args: arguments to `func`
    :param kwargs: keyword arguments to `func`
    :return: the API response, if any
    """

    res = maybe_retry(
        func,
        retryable_exceptions=(requests.ConnectionError, requests.ConnectTimeout),
        max_retries=max_retries,
        *args,
        **kwargs,
    )

    if 200 <= res.status_code <= 299:
        try:
            return res.json()
        except requests.JSONDecodeError:
            return res.text

    try:
        raise requests.RequestException(f"HTTP {res.status_code} error: {res.json()}")
    except Exception as e:
        # it's returning HTML or we can't parse the JSON
        logging.error(f"Error getting response as JSON: {e}")
        logging.error(f"Response text: {res.text}")
        raise requests.RequestException(f"HTTP {res.status_code} error")
