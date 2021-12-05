import concurrent.futures
import requests
import logging
from typing import Optional


def send(node: str, port: int, url: str, raw_message: dict) -> Optional[dict]:
    try:
        response = requests.post(f"http://{node}{url}:{port}",
                                 json=raw_message,
                                 timeout=10)  # 10 seconds.
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as exc:
        logging.getLogger("requests").warning(exc)
        # If post() or raise_for_status() threw, return None.


def send_to_all(
    nodes: list[str], port: int, url: str, raw_message: dict
) -> list[Optional[dict]]:
    def send_one(node):
        return send(node, port, url, raw_message)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        return list(executor.map(send_one, nodes))
