"""REST client handling, including Logic4Stream base class."""

import datetime
from datetime import timedelta
from typing import Any, Dict, Optional

import pytz
import requests
from memoization import cached
from pendulum import parse
from singer_sdk.streams import RESTStream

from tap_logic4.auth import Logic4Authenticator


class Logic4Stream(RESTStream):
    """Logic4 stream class."""

    url_base = "https://api.logic4server.nl"

    records_jsonpath = "$.Records[*]"
    next_page_token_jsonpath = "$.next_page"
    rep_key_field = None
    rest_method = "POST"
    page_size = 10000
    from_to = True

    @property
    @cached
    def authenticator(self) -> Logic4Authenticator:
        """Return a new authenticator object."""
        return Logic4Authenticator.create_for_stream(self)

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config.get("user_agent")
        return headers

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Return a token for identifying next page or None if no more pages."""
        counter = response.json().get("RecordsCounter")
        if counter:
            previous_token = previous_token or 0
            next_page_token = previous_token + counter
            return next_page_token

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Return a dictionary of values to be used in URL parameterization."""
        params: dict = {}
        return params

    def get_starting_time(self, context):
        #NOTE: logic4 uses Amsterdam timezone
        start_date = self.config.get("start_date")
        if start_date:
            start_date = parse(self.config.get("start_date"))
        rep_key = self.get_starting_timestamp(context)
        date = rep_key or start_date
        date = date.astimezone(pytz.timezone('Europe/Amsterdam'))
        return date

    def prepare_request_payload(self, context, next_page_token):
        #NOTE: Logic4 uses Amsterdam timezone
        start_date = self.get_starting_time(context)
        if self.stream_state.get("replication_key_value"):
            start_date = start_date + timedelta(seconds=1)
        start_date = (
            start_date.strftime("%Y-%m-%dT%H:%M:%SZ") if start_date else start_date
        )
        now = datetime.datetime.now(pytz.timezone('Europe/Amsterdam')).strftime("%Y-%m-%dT%H:%M:%S")
        payload = {}
        payload["TakeRecords"] = self.page_size
        if start_date and self.replication_key and self.rep_key_field:
            if self.from_to:
                payload[f"{self.rep_key_field}From"] = start_date
                payload[f"{self.rep_key_field}To"] = now
            else:
                payload[self.rep_key_field] = start_date
        if next_page_token:
            payload["SkipRecords"] = next_page_token
        self.logger.info(f"Making request to '{self.path}' with payload: {payload}")
        return payload

    def get_records(self, context: Optional[dict]):
        sync_invoices = self.config.get("sync_invoices")

        if self.name == "invoices" and not sync_invoices:
            pass
        else:
            for record in self.request_records(context):
                transformed_record = self.post_process(record, context)
                if transformed_record is None:
                    continue
                yield transformed_record
