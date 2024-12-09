import logging
from typing import Any, Callable
from urllib.parse import urlunsplit

import pandas as pd
import requests

from arret.utils import get_gcp_oidc_token, maybe_retry


class TerraWorkspace:
    def __init__(self, workspace_namespace: str, workspace_name: str) -> None:
        """
        Make an object representing a Terra workspace.

        :param workspace_namespace: the namespace of the Terra workspace
        :param workspace_name: the name of the Terra workspace
        """

        self.workspace_namespace = workspace_namespace
        self.workspace_name = workspace_name

    def get_bucket_name(self) -> str:
        """
        Retrieves the name of the bucket associated with the Terra workspace.

        :return: bucket name
        """

        j = call_firecloud_api(
            make_firecloud_req,
            path_parts=[
                "workspaces",
                self.workspace_namespace,
                self.workspace_name,
            ],
        )

        return j["workspace"]["bucketName"]

    def get_entity_types(self) -> list[str]:
        """
        Retrieves a list of entity types associated with the Terra workspace.

        :return: list of entity type names
        """

        j = call_firecloud_api(
            make_firecloud_req,
            path_parts=[
                "workspaces",
                self.workspace_namespace,
                self.workspace_name,
                "entities",
            ],
        )

        return list(j.keys())

    def get_entities(self, entity_type: str) -> pd.DataFrame:
        """
        Get a workspace entity as a data frame.

        :param entity_type: an entityt type (e.g. "sample")
        :return: a data frame of entities
        """

        j = call_firecloud_api(
            make_firecloud_req,
            path_parts=[
                "workspaces",
                self.workspace_namespace,
                self.workspace_name,
                "entities",
                entity_type,
            ],
        )

        records = [{f"{entity_type}_id": x["name"], **x["attributes"]} for x in j]

        return pd.DataFrame(records)


def make_firecloud_req(
    path_parts: list[str], params: dict[str, Any] | None = None
) -> Any:
    """
    Return a `requests.get` result for a Firecloud API endpoint.

    :param path_parts: a list of URL parts to '/'-join
    :param params: an optional dictionary of parameters
    :return:
    """

    if params is None:
        params = {}

    # get token for Firecloud API
    bearer = get_gcp_oidc_token()

    # construct URL and query params
    endpoint = urlunsplit(
        ("https", "api.firecloud.org", "/".join(["api", *path_parts]), "", "")
    )

    return requests.get(
        endpoint,
        params=params,
        headers={"Authorization": f"Bearer {bearer}"},
    )


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
