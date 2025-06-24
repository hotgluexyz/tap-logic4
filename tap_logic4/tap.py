"""Logic4 tap class."""

from typing import List

from singer_sdk import Stream, Tap
from singer_sdk import typing as th

from tap_logic4.streams import (
    BuyOrdersRowsStream,
    BuyOrdersStream,
    BuyOrderDeliveriesStream,
    InvoicesStream,
    InvoiceRowsStream,
    OrderRowsStream,
    OrdersStream,
    ProductsStream,
    StockStream,
    SuppliersStream,
    SupplierProductStream,
    SupplierProductBulkStream
)

STREAM_TYPES = [
    ProductsStream,
    StockStream,
    OrdersStream,
    OrderRowsStream,
    InvoicesStream,
    InvoiceRowsStream,
    BuyOrdersStream,
    BuyOrdersRowsStream,
    BuyOrderDeliveriesStream,
    SuppliersStream,
    SupplierProductStream,
    SupplierProductBulkStream
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
            "public_key",
            th.StringType,
            required=True,
        ),
        th.Property(
            "company_key",
            th.StringType,
            required=True,
        ),
        th.Property(
            "username",
            th.StringType,
            required=True,
        ),
        th.Property(
            "secret_key",
            th.StringType,
            required=True,
        ),
        th.Property(
            "password",
            th.StringType,
            required=True,
        ),
        th.Property(
            "scope",
            th.StringType,
        ),
        th.Property(
            "start_date",
            th.DateTimeType,
        ),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]


if __name__ == "__main__":
    TapLogic4.cli()
