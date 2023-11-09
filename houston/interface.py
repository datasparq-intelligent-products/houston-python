"""Houston Client Http Request Wrapper"""

import time
from random import random
import logging
import requests
import os
from typing import Optional

from .exceptions import HoustonServerBusy, HoustonClientError, HoustonServerError

log = logging.getLogger(os.getenv('HOUSTON_LOG_NAME', "houston"))


class InterfaceRequest:
    """Houston client interface request class"""

    def __init__(self, key):
        self.headers = {"x-access-key": key, "Content-Type": "application/json"}

    def request(self, method: str, uri: str, data: Optional[str] = None, retry: int = 3,
                safe: bool = False, fire_and_forget: bool = False, headers: Optional[dict] = None):
        """
        Request a Houston resource

        :param method: Http method required for request (e.g. GET, POST, DELETE etc.)
        :param uri: Complete URL of request, including schema (e.g. https://)
        :param data: Text to send in the body of the request (will be JSON encoded)
        :param retry: Number of retries to attempt with request (only used by 429 server responses)
        :param safe: Do not raise errors in case of client error
        :param fire_and_forget: If true, do not wait for a response
        :param headers: (optional) Additional headers to be sent with request
        :return: HTTP response code and response payload parsed as dict
        """

        timeout = None
        if fire_and_forget:
            timeout = 1
        if headers is None:
            headers = {}

        try:
            response = requests.request(
                method, uri, headers={**self.headers, **headers}, data=data, timeout=timeout,
            )

        except requests.exceptions.ReadTimeout:
            if fire_and_forget:
                return 200, dict()
            else:
                raise
        except requests.exceptions.ConnectionError:
            if retry > 0:
                time.sleep(random())
                return self.request(method, uri, data, retry - 1,
                                    fire_and_forget=fire_and_forget, headers=headers)
            else:
                raise HoustonServerError(
                    f"Unable to connect to Houston API server at url: {uri}. Is your Houston server running?"
                )

        # retry if server busy - this can be common in a large workflow due to operations being immutable. 572 is the
        # Houston API's code for DagLockedError. 429 is 'Too Many Requests'.
        if response.status_code in (429, 572):
            if retry > 0:
                time.sleep(random())
                return self.request(method, uri, data, retry - 1,
                                    fire_and_forget=fire_and_forget, headers=headers)
            else:
                raise HoustonServerBusy(
                    "received too many 429 responses from server, please reduce the number of requests"
                )

        if 400 <= response.status_code < 500 and not safe:
            err_msg, err_type = self._parse_error(response)
            raise HoustonClientError(
                "Unknown client error occurred. Please check request"
                if err_msg is None
                else err_msg
            )

        if 400 <= response.status_code < 500 and safe:
            return response.status_code, None

        if response.status_code >= 500:
            err_msg, err_type = self._parse_error(response)
            raise HoustonServerError(
                "Unknown server error occurred. If this persists please contact support"
                if err_msg is None
                else err_msg
            )

        try:
            json_data = response.json()
        except ValueError:
            json_data = None

        return response.status_code, json_data

    @staticmethod
    def _parse_error(response) -> (str, str):
        """
        Parses any version of the API payload when the status code is != 200

        :param response: a response object
        :return: Error message, error type
        """
        err_msg = None
        err_type = None
        try:
            payload = response.json()
            if "msg" in payload:  # deprecated
                err_msg = payload["msg"]

            elif "message" in payload and "type" in payload:
                err_msg = payload.get("message")
                err_type = payload.get("type")
            elif "message" in payload:  # deprecated
                err_msg = payload["message"]
            elif "error" in payload and "message" in payload:  # deprecated
                err_msg = payload["error"] + ". " + payload["message"]
            elif "error" in payload:  # deprecated
                err_msg = payload["error"]
        except ValueError:
            # Generic Error
            pass

        if response.status_code == 404 and err_msg is None:
            err_msg = f"Resource not found at {response.request.url}."
            if "api/v1" not in response.request.path_url:
                err_msg += " The base URL may be incorrect; it should end with '/api/v1'"

        return err_msg, err_type
