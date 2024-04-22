"""Logic4 Authentication."""


import json
from datetime import datetime
from typing import Optional

import backoff
import requests
from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta
from singer_sdk.streams import Stream as RESTStreamBase


class Logic4Authenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for Logic4."""

    def __init__(
        self,
        stream: RESTStreamBase,
        config_file: Optional[str] = None,
        auth_endpoint: Optional[str] = None,
        oauth_scopes: Optional[str] = None,
    ) -> None:
        super().__init__(
            stream=stream, auth_endpoint=auth_endpoint, oauth_scopes=oauth_scopes
        )
        self._config_file = config_file
        self._tap = stream._tap

    @property
    def auth_headers(self) -> dict:
        if not self.is_token_valid():
            self.update_access_token()
        result = super().auth_headers
        result["Authorization"] = f"Bearer {self._tap._config.get('access_token')}"
        return result

    def is_token_valid(self) -> bool:
        access_token = self._tap._config.get("access_token")
        now = round(datetime.utcnow().timestamp())
        expires_in = self._tap.config.get("expires_in")
        if expires_in is not None:
            expires_in = int(expires_in)
        if not access_token:
            return False

        if not expires_in:
            return False

        return not ((expires_in - now) < 120)

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the Logic4 API."""
        # build scope
        scope = self.config.get('scope', "api administration.ADMINISTRATION")
        if not scope.startswith("api"):
            scope = f"api {scope}"

        return {
            "grant_type": "client_credentials",
            "client_id": f"{self.config['public_key']} {self.config['company_key']} {self.config['username']}",
            "client_secret": f"{self.config['secret_key']} {self.config['password']}",
            "scope": scope,
        }

    @classmethod
    def create_for_stream(cls, stream) -> "Logic4Authenticator":
        return cls(
            stream=stream,
            auth_endpoint="https://idp.logic4server.nl/token",
            oauth_scopes="",
        )

    @backoff.on_exception(backoff.expo, Exception, max_tries=3)
    def update_access_token(self) -> None:
        self.logger.info(
            f"Oauth request - endpoint: {self._auth_endpoint}, body: {self.oauth_request_body}"
        )
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        token_response = requests.post(
            self._auth_endpoint,
            data=self.oauth_request_body,
            headers=headers
        )
        try:
            token_response.raise_for_status()
            self.logger.info("OAuth authorization attempt was successful.")
        except Exception as ex:
            raise RuntimeError(
                f"Failed OAuth login, response was '{token_response.text}'. {ex}"
            )
        token_json = token_response.json()
        # Log the refresh_token
        self.access_token = token_json["access_token"]
        self._tap._config["access_token"] = token_json["access_token"]
        now = round(datetime.utcnow().timestamp())
        self._tap._config["expires_in"] = now + token_json["expires_in"]

        with open(self._tap.config_file, "w") as outfile:
            json.dump(self._tap._config, outfile, indent=4)
