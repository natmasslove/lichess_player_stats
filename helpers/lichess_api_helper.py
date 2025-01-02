from typing import Iterator
import requests

BASE_URL = "https://lichess.org/"


class LichessAPIHelper(Exception):
    pass


class LichessAPIHelper:
    def __init__(self, lichess_token: str):
        self.lichess_token = lichess_token
        self.base_url = BASE_URL

    def _exec_request(
        self, api_url: str, params: dict = None, additional_headers: dict = None
    ) -> requests.Response:
        headers = {
            "Authorization": f"Bearer {self.lichess_token}",
        }
        if additional_headers:
            headers.update(additional_headers)

        url = self.base_url + api_url
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            return response
        else:
            raise LichessAPIHelper(
                f"Failed API request: {api_url}. Code/Error: {response.status_code}/{response.reason}"
            )

    def exec_request_json(self, api_url: str, params: dict = None) -> dict:
        response = self._exec_request(api_url, params)
        return response.json()

    def exec_request_ndjson(self, api_url: str, params: dict = None) -> Iterator[bytes]:
        additional_headers = {"Accept": "application/x-ndjson"}
        response = self._exec_request(
            api_url, params, additional_headers=additional_headers
        )
        return response.iter_lines()
