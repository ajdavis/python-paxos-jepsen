import concurrent.futures
import requests
import logging
from typing import Optional


def send(node: str, url: str, raw_message: dict) -> Optional[dict]:
    try:
        response = requests.post(f"http://{node}{url}",
                                 json=raw_message)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as exc:
        logging.getLogger("requests").warning(exc)


def send_all(
        nodes: list[str], url: str, raw_message: dict) -> list[Optional[dict]]:
    def send_one(node):
        return send(node, url, raw_message)
    with concurrent.futures.ThreadPoolExecutor() as executor:
        return list(executor.map(send_one, nodes))
