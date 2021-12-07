import concurrent.futures
import requests
import logging
from typing import Optional

_logger = logging.getLogger("network")


def send(
    *, node: str, url: str, raw_message: dict, timeout: int = 5
) -> Optional[dict]:
    """Post JSON and return response or None on error.

    Timeout is in seconds.
    """
    try:
        # Make sure url starts with "/".
        full_url = f"http://{node}/{url.lstrip('/')}"
        response = requests.post(full_url,
                                 json=raw_message,
                                 timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as exc:
        _logger.warning(exc)
        # If post() or raise_for_status() threw, return None.


def send_to_all(
    *,
    nodes: list[str],
    url: str,
    raw_message: dict,
    timeout: int = 10
) -> list[Optional[dict]]:
    """Post JSON concurrently to all servers, and return gathered responses.

    Nodes is a list of ["host:port", ...]. Timeout is in seconds. Each response
    is a dict, or None on error.
    """

    def send_one(node):
        return send(node=node,
                    url=url,
                    raw_message=raw_message,
                    timeout=timeout)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        return list(executor.map(send_one, nodes))
