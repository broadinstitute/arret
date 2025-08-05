import logging
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from math import sqrt
from queue import Queue
from time import sleep
from typing import Callable, ParamSpec, Type, TypeVar

import google.auth
import google.oauth2
import pandas as pd
from google.oauth2 import id_token

P = ParamSpec("P")
R = TypeVar("R")


def generalized_fibonacci(n: int, *, f0: float = 1.0, f1: float = 1.0) -> float:
    """
    Calculate the nth number in a generalized Fibonacci sequence given two starting
    nonnegative real numbers. This generates a gradually increasing sequence that
    provides a good balance between linear and exponential functions for use as a
    backoff.

    :param n: the nth Fibonacci number to compute
    :param f0: the first starting value for the sequence
    :param f1: the second starting value for the sequence
    :return: the nth Fibonacci number
    """

    assert f0 >= 0, "f0 must be at least 0.0"
    assert f1 >= 0, "f1 must be at least 0.0"

    # compute constants for closed-form of Fibonacci sequence recurrence relation
    sqrt5 = sqrt(5)
    phi = (1 + sqrt5) / 2
    psi = 1 - phi
    a = (f1 - f0 * psi) / sqrt5
    b = (f0 * phi - f1) / sqrt5

    return max([0, a * phi**n + b * psi**n])


def maybe_retry(
    func: Callable[P, R],
    retryable_exceptions: tuple[Type[Exception], ...] = tuple([Exception]),
    max_retries: int = 0,
    waiter: Callable[..., float] = partial(generalized_fibonacci, f0=1.0, f1=1.0),
    *args: P.args,
    **kwargs: P.kwargs,
) -> R:
    """
    Call a function and optionally retry (at most `max_retries` times) if it raises
    certain exceptions.

    :param func: a function
    :param retryable_exceptions: a tuple of retryable exceptions
    :param max_retries: the maximum number of times to retry
    :param waiter: a function that returns the number of seconds to wait given how many
    tries have already happened
    :param kwargs: keyword arguments to `func`
    :return: the return value from `func`
    """

    if max_retries == 0:
        return func(*args, **kwargs)

    n_retries = 0

    while True:
        try:
            return func(*args, **kwargs)

        except retryable_exceptions as e:
            if n_retries == max_retries:
                raise e

            wait_seconds = round(waiter(n_retries + 1), 1)
            logging.warning(f"{e} (retrying in {wait_seconds}s)")
            sleep(wait_seconds)
            n_retries += 1


def collect_gs_urls(df: pd.DataFrame) -> pd.DataFrame:
    """
    Collect all strings beginning with 'gs://' from a data frame, including those nested
    in lists or dictionaries.

    :param df: data farme with potentially nested values.
    :return: long data frame with columns:
        - 'url': the extracted gs:// URL string
        - 'col': the original column the URL was found in
    """

    urls = []

    for col in df.columns:
        col_series = df[col].dropna()

        for val in col_series:
            stack = [val]

            while stack:
                current = stack.pop()

                if isinstance(current, str) and current.startswith("gs://"):
                    urls.append({"url": current, "col": col})
                elif isinstance(current, list):
                    stack.extend(current)
                elif isinstance(current, dict):
                    stack.extend(current.values())

    return pd.DataFrame(urls)


def human_readable_size(size: float) -> str:
    """
    Convert a file size in bytes to a more human-readable format.

    :param size: a file size in bytes
    :return: a human-readable representation of the file size
    """

    units = ["B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"]

    for unit in units:
        if size < 1024:
            return f"{size:.2f} {unit}"
        size /= 1024.0

    return f"{size:.2f} YB"


class BoundedThreadPoolExecutor(ThreadPoolExecutor):
    def __init__(self, *args, queue_size: int = 2, **kwargs):
        """
        Subclass the default `ThreadPoolExecutor` to use a `Queue` instead of a
        `SimpleQueue` so that the pool size cannot grow beyond the requested queue size.

        :param queue_size: number of jobs to keep in the thread pool
        """

        super().__init__(*args, **kwargs)
        self._work_queue = Queue(queue_size)  # type: ignore


def get_gcp_oidc_token() -> str:
    """
    Get a GCP OIDC token (ID token) for current credentials.

    :return: the auth/bearer token
    """

    auth_req = google.auth.transport.requests.Request()  # type: ignore

    token = id_token.fetch_id_token(  # type: ignore
        auth_req, "https://batch.googleapis.com"
    )

    if token is None:
        raise ValueError("GCP auth token cannot be None")

    return token


def split_workspace_names(other_workspaces: list[str]) -> list[dict[str, str]]:
    """
    Split "namespace/name" Terra workspace identifiers passed as the `other-workspaces`
    CLI option into dictionaries.

    :param other_workspaces: a list of "namespace/name" Terra workspace identifiers
    :return: a list of structured namespace/name dictionaries
    """

    return [
        {"workspace_namespace": x1, "workspace_name": x2}
        for x in other_workspaces
        for x1, x2 in [x.split("/")]
    ]
