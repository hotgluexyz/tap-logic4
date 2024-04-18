"""Logic4 tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_logic4.streams import (
    BuyOrdersRowsStream,
    BuyOrdersStream,
    InvoicesStream,
    OrderRowsStream,
    OrdersStream,
    ProductsStream,
    StockStream,
)

STREAM_TYPES = [
    ProductsStream,
    StockStream,
    OrdersStream,
    OrderRowsStream,
    InvoicesStream,
    BuyOrdersStream,
    BuyOrdersRowsStream,
]


class TapLogic4(Tap):
    """Logic4 tap class."""

    name = "tap-logic4"

    def __init__(
        self,
        config=None,
        catalog=None,
        state=None,
        parse_env_config=False,
        validate_config=True,
    ) -> None:
        self.config_file = config[0]
        super().__init__(config, catalog, state, parse_env_config, validate_config)

    config_jsonschema = th.PropertiesList(
        th.Property(
            "client_id",
            th.StringType,
            required=True,
        ),
        th.Property(
            "client_secret",
            th.StringType,
            required=True,
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapLogic4.cli()
