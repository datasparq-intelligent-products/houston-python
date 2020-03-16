"""Houston Client Http Request Wrapper"""

import time
from random import random

import requests

from houston.exceptions import HoustonServerBusy, HoustonClientError, HoustonServerError


class InterfaceRequest:
    """Houston client interface request class"""

    def __init__(self, key):
        self.headers = {"x-access-key": key, "Content-Type": "application/json"}

    def request(self, method, uri, params=None, data=None, retry=3, safe=False):
        """
        Request a Houston resource

        :param string method: Http method required for request (e.g. GET, POST, DELETE etc)
        :param string uri: Complete URL of request, including schema (e.g. https://)
        :param dict params: Parameters to be sent with request (will be json encoded)
        :param dict data: Parameters to be sent with request (will be form encoded)
        :param int retry: Number of retry's to attempt with request (only used by 429 server responses)
        :param bool safe: Do not raise errors in-case of client error
        :return: HTTP response code and response payload parsed as dict
        """

        response = requests.request(
            method, uri, headers=self.headers, params=params, data=data
        )

        # retry if server busy - this can be common in a large workflow due to operations being immutable. 572 is the
        # Houston API's code for DagLockedError. 429 is 'Too Many Requests'.
        if response.status_code in (429, 572):
            if retry > 0:
                time.sleep(random())
                self.request(method, uri, params, data, retry - 1)
            else:
                raise HoustonServerBusy(
                    "received too many 429 responses from server, please reduce the number of requests"
                )

        if 400 <= response.status_code < 500 and not safe:
            err_msg = self._parse_error(response)
            raise HoustonClientError(
                "Unknown client error occurred. Please check request"
                if err_msg is None
                else err_msg
            )

        if 400 <= response.status_code < 500 and safe:
            return response.status_code, None

        if response.status_code >= 500:
            err_msg = self._parse_error(response)
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
    def _parse_error(response):
        """
        Parses any version of the API payload when the status code is != 200

        :param response: a response object
        :return: Error message
        """
        if response.headers.get("Content-Type") == "application/json":
            try:
                payload = response.json()
                if "msg" in payload:
                    return payload["msg"]
                elif "error" in payload and "message" in payload:
                    return payload["error"] + ". " + payload["message"]
                elif "error" in payload:
                    return payload["error"]
            except ValueError:
                # Generic Error
                return None
