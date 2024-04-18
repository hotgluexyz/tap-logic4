"""Logic4 Authentication."""


from singer_sdk.authenticators import OAuthAuthenticator, SingletonMeta


class Logic4Authenticator(OAuthAuthenticator, metaclass=SingletonMeta):
    """Authenticator class for Logic4."""

    @property
    def oauth_request_body(self) -> dict:
        """Define the OAuth request body for the Logic4 API."""
        return {
            "grant_type": "client_credentials",
            "client_id": self.config["client_id"],
            "client_secret": self.config["client_secret"],
            "scope": "api administration.1",
        }

    @classmethod
    def create_for_stream(cls, stream) -> "Logic4Authenticator":
        return cls(
            stream=stream,
            auth_endpoint="https://idp.logic4server.nl/token",
            oauth_scopes="",
        )
