"""Stream type classes for tap-logic4."""

from singer_sdk import typing as th

from tap_logic4.client import Logic4Stream
import datetime 
import pytz
from datetime import timedelta

address_type = th.ObjectType(
    th.Property(
        "Type",
        th.ObjectType(
            th.Property("Id", th.IntegerType),
            th.Property("Name", th.StringType),
        ),
    ),
    th.Property(
        "Province",
        th.ObjectType(
            th.Property("Id", th.IntegerType),
            th.Property("Name", th.StringType),
        ),
    ),
    th.Property("Email", th.StringType),
    th.Property("ContactName", th.StringType),
    th.Property("CompanyName", th.StringType),
    th.Property("Address1", th.StringType),
    th.Property("Address2", th.StringType),
    th.Property("Id", th.IntegerType),
    th.Property("DebtorId", th.IntegerType),
    th.Property("CreditorId", th.IntegerType),
    th.Property("IsMainContact", th.BooleanType),
    th.Property("IsHidden", th.BooleanType),
    th.Property("OwnContactNumber", th.StringType),
    th.Property("CountryCode", th.StringType),
    th.Property("IsoCode", th.StringType),
    th.Property("City", th.StringType),
    th.Property("Zipcode", th.StringType),
    th.Property("Street", th.StringType),
    th.Property("HouseNumber", th.StringType),
    th.Property("HouseNumberAddition", th.StringType),
    th.Property("TelephoneNumber", th.StringType),
    th.Property("CountryId", th.IntegerType),
    th.Property("ZoneId", th.IntegerType),
)

product_type = th.ObjectType(
    th.Property("Id", th.IntegerType),
    th.Property("Value", th.StringType),
)

class ProductsStream(Logic4Stream):
    """Define custom stream."""

    name = "products"
    path = "/v1.1/Products/GetProducts"
    primary_keys = ["ProductId"]
    replication_key = "DateTimeLastChanged"
    rep_key_field = "DateTimeChanged"

    schema = th.PropertiesList(
        th.Property("ProductId", th.IntegerType),
        th.Property("SubUnit_ParentId", th.IntegerType),
        th.Property("ProductCode", th.StringType),
        th.Property("ProductName1", th.StringType),
        th.Property("ProductName2", th.StringType),
        th.Property("ProductInfo", th.StringType),
        th.Property("StatusId", th.IntegerType),
        th.Property("Statusname", th.StringType),
        th.Property("BrandId", th.IntegerType),
        th.Property("Brandname", th.StringType),
        th.Property("Imagename1", th.StringType),
        th.Property("ImageUrl1", th.StringType),
        th.Property("Imagename2", th.StringType),
        th.Property("ImageUrl2", th.StringType),
        th.Property("Imagename3", th.StringType),
        th.Property("ImageUrl3", th.StringType),
        th.Property("Unit", th.StringType),
        th.Property("UnitId", th.IntegerType),
        th.Property("MinSaleAmount", th.NumberType),
        th.Property("MinSaleAmountWebshop", th.NumberType),
        th.Property("MinSaleBuyAmountDropShipment", th.NumberType),
        th.Property("SaleCountIncrement", th.NumberType),
        th.Property("SaleCountIncrementWebshop", th.NumberType),
        th.Property("SaleCountIncrementDropShipment", th.NumberType),
        th.Property("MinBuyAmount", th.NumberType),
        th.Property("VatPercent", th.NumberType),
        th.Property("VatCodeId", th.IntegerType),
        th.Property("SellPriceGross", th.NumberType),
        th.Property("Weight", th.NumberType),
        th.Property("Volume", th.NumberType),
        th.Property(
            "Offer",
            th.ObjectType(
                th.Property("StartDate", th.DateTimeType),
                th.Property("EndDate", th.DateTimeType),
                th.Property("FromPrice", th.NumberType),
                th.Property("ToPrice", th.NumberType),
                th.Property("OfferGroupId", th.IntegerType),
                th.Property("ProductId", th.IntegerType),
            ),
        ),
        th.Property("SellPriceAdvice", th.NumberType),
        th.Property("BuyPrice", th.NumberType),
        th.Property("ProductGroupId1", th.IntegerType),
        th.Property("BuyCountIncrement", th.NumberType),
        th.Property("SellPriceLowestForWebshop", th.NumberType),
        th.Property("ExcludePriceFromPricelistCalculations", th.BooleanType),
        th.Property("AdditionalBuyPriceAmount", th.NumberType),
        th.Property("AdditionalBuyPricePercentage", th.NumberType),
        th.Property("IsComposedProduct", th.BooleanType),
        th.Property("IsAssemblyProduct", th.BooleanType),
        th.Property("ComposedProductSetChildSellPricesToZero", th.BooleanType),
        th.Property("ComposedProductSetSellPriceToZero", th.BooleanType),
        th.Property("FreeStock", th.NumberType),
        th.Property("ExternalStockActiveSupplier", th.NumberType),
        th.Property("CreditorDiscountGroupId", th.IntegerType),
        th.Property("DateTimeLastChanged", th.DateTimeType),
        th.Property("DateTimeAdded", th.DateTimeType),
        th.Property("BarCode1", th.StringType),
        th.Property(
            "FreeValues",
            th.ArrayType(
                th.ObjectType(
                    th.Property("Key", th.StringType),
                    th.Property("Value", th.StringType),
                )
            ),
        ),
        th.Property("Sorting", th.NumberType),
        th.Property("NextDelivery", th.DateTimeType),
        th.Property(
            "ShiftPrices",
            th.ArrayType(
                th.ObjectType(
                    th.Property("Qty", th.NumberType),
                    th.Property("BuyPrice", th.NumberType),
                    th.Property("Margin", th.NumberType),
                    th.Property("SellPriceExcl", th.NumberType),
                    th.Property("SellPriceGrossExcl", th.NumberType),
                    th.Property("Description", th.StringType),
                    th.Property("DiscountType", th.StringType),
                )
            ),
        ),
        th.Property("ProductGroups", th.ArrayType(
                th.ObjectType(
                    th.Property("Id", th.IntegerType),
                    th.Property("Name", th.StringType),
                    th.Property("ParentProductGroupId", th.IntegerType),
                    th.Property("Shortname", th.StringType),
                    th.Property("PictureUrl", th.StringType),
                    th.Property("SortValue", th.NumberType),
                    th.Property("PictureName", th.StringType),
                    th.Property("ProductGroupTypeId", th.IntegerType),
                    th.Property("IsVisibleOnWebshop", th.BooleanType),
                    th.Property("DepthLevel", th.IntegerType),
                    th.Property("ShowUnitOnWebsite", th.BooleanType),
                )
            ),
        ),
        th.Property("Barcode2", th.StringType),
        th.Property("BarcodeExtraList", th.ArrayType(
            th.ObjectType(
                    th.Property("Barcode", th.StringType),
                    th.Property("Qty", th.NumberType),
                )
            )
        ),
        th.Property("SystemBarcode", th.StringType),
        th.Property("ProductGroup1ProductGroupTypeId", th.IntegerType),
        th.Property("WareHouses", th.ArrayType(
            th.ObjectType(
                    th.Property("WarehouseId", th.IntegerType),
                    th.Property("WarehouseName", th.StringType),
                    th.Property("MinimalStock", th.NumberType),
                    th.Property("MaxStock", th.NumberType),
                    th.Property("DefaultStockLocationId", th.IntegerType),
                )
            )),
        th.Property("MinimalStock", th.NumberType),
        th.Property("PCSinInsidePackage", th.NumberType),
        th.Property("PCSinOutsidePackage", th.NumberType),
        th.Property("ProductType1", product_type),
        th.Property("ProductType2", product_type),
        th.Property("ProductType3", product_type),
        th.Property("ProductType4", product_type),
        th.Property("ProductType5", product_type),
        th.Property("StandardAmount", th.NumberType),
        th.Property("VendorCode", th.StringType),
        th.Property("ProductTemplateId", th.IntegerType),
        th.Property("ProductTemplateName", th.StringType),
        th.Property("IsVisibleOnWebShop", th.BooleanType),
        th.Property("IsVisibleInLogic4", th.BooleanType),
    ).to_dict()
    
    # we need to go over each pair of IsVisibleOnWebShop and IsVisibleInLogic4
    is_visible_pairs = [
        {"IsVisibleOnWebShop": True, "IsVisibleInLogic4": True},
        {"IsVisibleOnWebShop": False, "IsVisibleInLogic4": True},
        {"IsVisibleOnWebShop": True, "IsVisibleInLogic4": False},
        {"IsVisibleOnWebShop": False, "IsVisibleInLogic4": False},
    ]
    is_visible_current_pair = None
    
    def prepare_request_payload(self, context, next_page_token):
        payload = super().prepare_request_payload(context, None)
        if next_page_token and next_page_token.get("counter"):
            payload["SkipRecords"] = next_page_token.get("counter")
        
        # we need to change the pairs if is the first attempt or the response doesn't have any record (counter=0)
        if (len(self.is_visible_pairs) == 4) or (next_page_token and next_page_token.get("counter") == 0):
            if self.is_visible_pairs:
                self.is_visible_current_pair = self.is_visible_pairs.pop()
            else:
                self.is_visible_current_pair = None
        if self.is_visible_current_pair:
            payload.update(self.is_visible_current_pair)
            context.update(self.is_visible_current_pair)
        return payload

    def get_child_context(self, record: dict, context) -> dict:
        return {"ProductId": record["ProductId"]}
    
    def get_next_page_token(self, response, previous_token):
        counter = response.json().get("RecordsCounter")
        if counter == 0 and len(self.is_visible_pairs) == 0:
            return None  # this means we go through all the pages through all the pairs
        
        next_page_token = {"counter": 0}
        if counter:
            prev_token = previous_token.get("counter", 0) if previous_token else 0
            next_page_token["counter"] = prev_token + counter
        if self.is_visible_current_pair:
            next_page_token.update(self.is_visible_current_pair)
        
        return next_page_token
    
    def post_process(self, row, context):
        row = super().post_process(row, context)
        row.update(context)
        return row
    
    def _get_state_partition_context(self, context):
        if context == self.is_visible_current_pair:
            return None
        return super()._get_state_partition_context(context)


class SupplierProductBulkStream(Logic4Stream):
    """Define custom stream."""

    name = "supplier_products_bulk"
    path = "/v1.1/Products/GetSuppliersForProducts"
    primary_keys = ["CreditorProductCode"]

    schema = th.PropertiesList(
        th.Property("ProductId", th.IntegerType),
        th.Property("CreditorName", th.StringType),
        th.Property("CreditorProductCode", th.StringType),
        th.Property("IsActive", th.BooleanType),
        th.Property("CreditorId", th.IntegerType),
        th.Property("CreditorBuyPrices", th.ArrayType(
            th.ObjectType(
                th.Property("Key", th.IntegerType),
                th.Property("Value", th.NumberType),
            )
        )),
    ).to_dict()

class SupplierProductStream(Logic4Stream):
    """Define custom stream."""

    name = "supplier_products"
    path = "/v1.1/Products/GetSuppliersForProduct"
    primary_keys = ["CreditorProductCode"]
    parent_stream_type = ProductsStream

    schema = th.PropertiesList(
        th.Property("ProductId", th.IntegerType),
        th.Property("CreditorName", th.StringType),
        th.Property("CreditorProductCode", th.StringType),
        th.Property("IsActive", th.BooleanType),
        th.Property("CreditorId", th.IntegerType),
        th.Property("CreditorBuyPrices", th.ArrayType(
            th.ObjectType(
                th.Property("Key", th.IntegerType),
                th.Property("Value", th.NumberType),
            )
        )),      
    ).to_dict()

    def prepare_request_payload(self, context, next_page_token):
        return context["ProductId"]
    
    def get_next_page_token(self, response, previous_token):
        return None


class StockStream(Logic4Stream):
    """Define custom stream."""

    name = "stocks"
    path = "/v1.1/Stock/GetStockForWarehouses"
    primary_keys = ["ProductId"]
    schema = th.PropertiesList(
        th.Property("ProductCode", th.StringType),
        th.Property("WarehouseId", th.IntegerType),
        th.Property("QtyReserved", th.NumberType),
        th.Property("FreeStock", th.NumberType),
        th.Property("ProductId", th.IntegerType),
        th.Property("Qty", th.NumberType),
    ).to_dict()


class TransactionBaseStream(Logic4Stream):
    """Define custom stream."""

    name = "transaction_base_stream"
    replication_key = "ChangedAt"
    rep_key_field = "ChangedAfter" # filter param provided by logic4
    from_to = False

    schema = th.PropertiesList(
        th.Property("DebtorId", th.IntegerType),
        th.Property("Id", th.IntegerType),
        th.Property("ChangedAt", th.DateTimeType), #field created with current datetime to filter by rep_key
        th.Property(
            "Totals",
            th.ObjectType(
                th.Property("AmountEx", th.NumberType),
                th.Property("VATPercentage", th.NumberType),
                th.Property("ShippingCost", th.NumberType),
                th.Property("ShippingCostIncl", th.NumberType),
                th.Property("Calc_TotalPayed", th.NumberType),
                th.Property("AmountIncl", th.NumberType),
                th.Property("IsPaid", th.BooleanType),
            ),
        ),
        th.Property(
            "PaymentMethod",
            th.ObjectType(
                th.Property("Id", th.IntegerType),
                th.Property("Description", th.StringType),
                th.Property("MaxAmount", th.NumberType),
                th.Property("SelectKey", th.StringType),
            ),
        ),
        th.Property(
            "OrderStatus",
            th.ObjectType(
                th.Property("Id", th.IntegerType),
                th.Property("Value", th.StringType),
            ),
        ),
        th.Property(
            "ShippingMethod",
            th.ObjectType(
                th.Property("Id", th.IntegerType),
                th.Property("Name", th.StringType),
                th.Property("ExportCode", th.StringType),
            ),
        ),
        th.Property(
            "OrderShipments",
            th.ArrayType(
                th.ObjectType(
                    th.Property("Id", th.IntegerType),
                    th.Property("DateTimeAdded", th.DateTimeType),
                    th.Property("OrderId", th.IntegerType),
                    th.Property("ShipperId", th.IntegerType),
                    th.Property("Barcode", th.StringType),
                    th.Property("TrackTraceUrl", th.StringType),
                )
            ),
        ),
        th.Property("InvoiceBelongsToOrderNumber", th.NumberType),
        th.Property(
            "Payments",
            th.ArrayType(
                th.ObjectType(
                    th.Property("OrderId", th.IntegerType),
                    th.Property("InvoiceId", th.IntegerType),
                    th.Property("AmountIncl", th.NumberType),
                    th.Property("Description", th.StringType),
                    th.Property("BookingId", th.IntegerType),
                    th.Property("MatchingLedgerId", th.IntegerType),
                    th.Property("DateTime", th.DateTimeType),
                    th.Property("LedgerCode", th.NumberType),
                )
            ),
        ),
        th.Property(
            "CostCentre",
            th.ObjectType(
                th.Property("Id", th.IntegerType),
                th.Property("Code", th.StringType),
                th.Property("Description", th.StringType),
            ),
        ),
        th.Property("AccountAddress", address_type),
        th.Property("DeliveryAddress", address_type),
        th.Property("InvoiceAddress", address_type),
        th.Property("CreationDate", th.DateTimeType),
        th.Property("Description", th.StringType),
        th.Property("Reference", th.StringType),
        th.Property("BranchId", th.IntegerType),
        th.Property("UserId", th.IntegerType),
        th.Property("WebsiteDomainId", th.IntegerType),
        th.Property("DeliveryOptionId", th.IntegerType),
        th.Property("DeliveryDate", th.DateTimeType),
        th.Property(
            "OrderShipmentFreeValues",
            th.ObjectType(
                th.Property("ShipperTypeId", th.IntegerType),
                th.Property("Freevalue1", th.StringType),
                th.Property("Freevalue2", th.StringType),
                th.Property("Freevalue3", th.StringType),
                th.Property("Freevalue4", th.StringType),
                th.Property("Freevalue5", th.StringType),
            ),
        ),
        th.Property("Notes", th.StringType),
        th.Property("FreeValue1", th.StringType),
        th.Property("FreeValue2", th.StringType),
        th.Property("FreeValue3", th.StringType),
        th.Property("FreeValue4", th.StringType),
        th.Property("FreeValue5", th.StringType),
        th.Property("FreeValue6", th.StringType),
        th.Property("FreeValue7", th.StringType),
        th.Property("FreeValue8", th.StringType),
        th.Property("OrderType1Id", th.IntegerType),
        th.Property("OrderType2Id", th.IntegerType),
        th.Property("OrderType3Id", th.IntegerType),
        th.Property("OrderType4Id", th.IntegerType),
        th.Property("OrderType5Id", th.IntegerType),
        th.Property("OrderType6Id", th.IntegerType),
        th.Property("OrderType7Id", th.IntegerType),
        th.Property("OrderType8Id", th.IntegerType),
        th.Property(
            "OrderRows",
            th.ArrayType(
                th.ObjectType(
                    th.Property("SerialNumbers", th.ArrayType(th.StringType)),
                    th.Property("ExpectedNextQtyOnDelivery", th.NumberType),
                    th.Property("DiscountPercent", th.NumberType),
                    th.Property("QtyReserved", th.NumberType),
                    th.Property("InclPrice", th.NumberType),
                    th.Property("InternalNotes", th.StringType),
                    th.Property("IsAssemblyChild", th.BooleanType),
                    th.Property("Id", th.IntegerType),
                    th.Property("Description", th.StringType),
                    th.Property("Description2", th.StringType),
                    th.Property("ProductId", th.IntegerType),
                    th.Property("Qty", th.NumberType),
                    th.Property("BuyPrice", th.NumberType),
                    th.Property("GrossPrice", th.NumberType),
                    th.Property("NettPrice", th.NumberType),
                    th.Property("QtyDeliverd", th.NumberType),
                    th.Property("QtyDeliverd_NotInvoiced", th.NumberType),
                    th.Property("ProductCode", th.StringType),
                    th.Property("ProductBarcode1", th.StringType),
                    th.Property("VATPercentage", th.NumberType),
                    th.Property("Notes", th.StringType),
                    th.Property("DebtorId", th.IntegerType),
                    th.Property("OrderId", th.IntegerType),
                    th.Property("WarehouseId", th.IntegerType),
                    th.Property("Commission", th.StringType),
                    th.Property("DeliveryOptionId", th.IntegerType),
                    th.Property("VatCodeId", th.IntegerType),
                    th.Property("VatCodeIdOverrule", th.NumberType),
                    th.Property("FreeValue1", th.StringType),
                    th.Property("FreeValue2", th.StringType),
                    th.Property("FreeValue3", th.StringType),
                    th.Property("FreeValue4", th.StringType),
                    th.Property("FreeValue5", th.StringType),
                    th.Property("ExpectedNextDelivery", th.DateTimeType),
                    th.Property(
                        "ExternalValue",
                        th.ObjectType(
                            th.Property("TypeId", th.IntegerType),
                            th.Property("Value", th.StringType),
                        ),
                    ),
                    th.Property("AgreedDeliveryDate", th.DateTimeType),
                    th.Property("Type1Id", th.IntegerType),
                    th.Property("Type2Id", th.IntegerType),
                    th.Property("Type3Id", th.IntegerType),
                    th.Property("Type4Id", th.IntegerType),
                    th.Property("Type5Id", th.IntegerType),
                )
            ),
        ),
    ).to_dict()

    def post_process(self, row, context):
        row = super().post_process(row, context)
        # NOTE: while orders and invoices support a ChangedAfter filter, the tap needs a datetime value for the rep_key field 
        # in each record, as logic4 doesn't return any updated time value we're synthetically creating ChangedAt to use as rep_key
        now = datetime.datetime.now(pytz.timezone('Europe/Amsterdam')).strftime("%Y-%m-%dT%H:%M:%S.%f")
        row["ChangedAt"] = now
        return row


class OrdersStream(TransactionBaseStream):
    """Define custom stream."""

    name = "orders"
    path = "/v1.2/Orders/GetOrders"
    primary_keys = ["Id"]
    page_size = 1000

    def get_child_context(self, record, context) -> dict:
        return {"OrderId": record["Id"]}

class OrderRowsStream(Logic4Stream):
    """Define custom stream."""

    name = "order_rows"
    path = "/v1/Orders/GetOrderRows"
    primary_keys = ["Id"]
    parent_stream_type = OrdersStream
    schema = th.PropertiesList(
        th.Property("SerialNumbers", th.ArrayType(th.StringType)),
        th.Property("ExpectedNextQtyOnDelivery", th.NumberType),
        th.Property("DiscountPercent", th.NumberType),
        th.Property("QtyReserved", th.NumberType),
        th.Property("InclPrice", th.NumberType),
        th.Property("InternalNotes", th.StringType),
        th.Property("IsAssemblyChild", th.BooleanType),
        th.Property("Id", th.IntegerType),
        th.Property("Description", th.StringType),
        th.Property("Description2", th.StringType),
        th.Property("ProductId", th.IntegerType),
        th.Property("Qty", th.NumberType),
        th.Property("BuyPrice", th.NumberType),
        th.Property("GrossPrice", th.NumberType),
        th.Property("NettPrice", th.NumberType),
        th.Property("QtyDeliverd", th.NumberType),
        th.Property("QtyDeliverd_NotInvoiced", th.NumberType),
        th.Property("ProductCode", th.StringType),
        th.Property("ProductBarcode1", th.StringType),
        th.Property("VATPercentage", th.NumberType),
        th.Property("Notes", th.StringType),
        th.Property("DebtorId", th.IntegerType),
        th.Property("OrderId", th.IntegerType),
        th.Property("WarehouseId", th.NumberType),
        th.Property("Commission", th.StringType),
        th.Property("DeliveryOptionId", th.IntegerType),
        th.Property("VatCodeId", th.IntegerType),
        th.Property("VatCodeIdOverrule", th.NumberType),
        th.Property("FreeValue1", th.StringType),
        th.Property("FreeValue2", th.StringType),
        th.Property("FreeValue3", th.StringType),
        th.Property("FreeValue4", th.StringType),
        th.Property("FreeValue5", th.StringType),
        th.Property("ExpectedNextDelivery", th.DateTimeType),
        th.Property(
            "ExternalValue",
            th.ObjectType(
                th.Property("TypeId", th.IntegerType),
                th.Property("Value", th.StringType),
            ),
        ),
        th.Property("AgreedDeliveryDate", th.DateTimeType),
        th.Property("Type1Id", th.IntegerType),
        th.Property("Type2Id", th.IntegerType),
        th.Property("Type3Id", th.IntegerType),
        th.Property("Type4Id", th.IntegerType),
        th.Property("Type5Id", th.IntegerType),
    ).to_dict()

    def prepare_request_payload(self, context, next_page_token):
        payload = super().prepare_request_payload(context, next_page_token)
        payload["OrderId"] = context["OrderId"]
        return payload
    
    def get_next_page_token(self, response, previous_token):
        return None

class InvoicesStream(TransactionBaseStream):
    """Define custom stream."""

    name = "invoices"
    path = "/v1.2/Orders/GetInvoices"
    primary_keys = ["Id"]
    page_size = 1000

    def get_child_context(self, record, context) -> dict:
        return {"OrderId": record["Id"]}

class InvoiceRowsStream(Logic4Stream):
    """Define custom stream."""

    name = "invoice_rows"
    path = "/v1/Orders/GetInvoiceRows"
    primary_keys = ["Id"]
    parent_stream_type = InvoicesStream
    schema = th.PropertiesList(
        th.Property("SerialNumbers", th.ArrayType(th.StringType)),
        th.Property("ExpectedNextQtyOnDelivery", th.NumberType),
        th.Property("DiscountPercent", th.NumberType),
        th.Property("QtyReserved", th.NumberType),
        th.Property("InclPrice", th.NumberType),
        th.Property("InternalNotes", th.StringType),
        th.Property("IsAssemblyChild", th.BooleanType),
        th.Property("Id", th.IntegerType),
        th.Property("Description", th.StringType),
        th.Property("Description2", th.StringType),
        th.Property("ProductId", th.IntegerType),
        th.Property("Qty", th.NumberType),
        th.Property("BuyPrice", th.NumberType),
        th.Property("GrossPrice", th.NumberType),
        th.Property("NettPrice", th.NumberType),
        th.Property("QtyDeliverd", th.NumberType),
        th.Property("QtyDeliverd_NotInvoiced", th.NumberType),
        th.Property("ProductCode", th.StringType),
        th.Property("ProductBarcode1", th.StringType),
        th.Property("VATPercentage", th.NumberType),
        th.Property("Notes", th.StringType),
        th.Property("DebtorId", th.IntegerType),
        th.Property("OrderId", th.IntegerType),
        th.Property("WarehouseId", th.NumberType),
        th.Property("Commission", th.StringType),
        th.Property("DeliveryOptionId", th.IntegerType),
        th.Property("VatCodeId", th.IntegerType),
        th.Property("VatCodeIdOverrule", th.NumberType),
        th.Property("FreeValue1", th.StringType),
        th.Property("FreeValue2", th.StringType),
        th.Property("FreeValue3", th.StringType),
        th.Property("FreeValue4", th.StringType),
        th.Property("FreeValue5", th.StringType),
        th.Property("ExpectedNextDelivery", th.DateTimeType),
        th.Property(
            "ExternalValue",
            th.ObjectType(
                th.Property("TypeId", th.IntegerType),
                th.Property("Value", th.StringType),
            ),
        ),
        th.Property("AgreedDeliveryDate", th.DateTimeType),
        th.Property("Type1Id", th.IntegerType),
        th.Property("Type2Id", th.IntegerType),
        th.Property("Type3Id", th.IntegerType),
        th.Property("Type4Id", th.IntegerType),
        th.Property("Type5Id", th.IntegerType),
    ).to_dict()

    def prepare_request_payload(self, context, next_page_token):
        payload = super().prepare_request_payload(context, next_page_token)
        payload["OrderId"] = context["OrderId"]
        return payload
    
    def get_next_page_token(self, response, previous_token):
        return None


class BuyOrdersStream(Logic4Stream):
    """Define custom stream."""

    name = "buy_orders"
    path = "/v1.1/BuyOrders/GetBuyOrders"
    primary_keys = ["Id"]
    rep_key_field = "BuyOrderDate"

    schema = th.PropertiesList(
        th.Property("AmountOfRows", th.NumberType),
        th.Property("BranchId", th.IntegerType),
        th.Property("BuyOrderClosed", th.BooleanType),
        th.Property("CreatedAt", th.DateTimeType),
        th.Property("CreditorCompanyName", th.StringType),
        th.Property("CreditorId", th.IntegerType),
        th.Property("DatabaseAdministrationId", th.IntegerType),
        th.Property("Id", th.IntegerType),
        th.Property("OrderId", th.IntegerType),
        th.Property("Remarks", th.StringType),
        th.Property("FreeValue1", th.StringType),
        th.Property("FreeValue2", th.StringType),
        th.Property("FreeValue3", th.StringType),
    ).to_dict()

    def get_child_context(self, record, context) -> dict:
        BuyOrderIsClosed_GetRows = self.config.get("BuyOrderIsClosed_GetRows")
        if BuyOrderIsClosed_GetRows == False:
            # get only open buy orders rows
            if record["BuyOrderClosed"] == False:
                return {"OrderId": record["Id"]}
            return None
        else:
            return {"OrderId": record["Id"]}
    
    def _sync_children(self, child_context: dict) -> None:
        if child_context:
            return super()._sync_children(child_context)


class BuyOrdersRowsStream(Logic4Stream):
    """Define custom stream."""

    name = "buy_orders_rows"
    path = "/v1/BuyOrders/GetBuyOrderRows"
    primary_keys = ["BuyOrderRowId"]
    parent_stream_type = BuyOrdersStream

    schema = th.PropertiesList(
        th.Property("BuyOrderRowId", th.IntegerType),
        th.Property("DebtorName", th.StringType),
        th.Property("OrderId", th.IntegerType),
        th.Property("ProductCode", th.StringType),
        th.Property("ProductId", th.IntegerType),
        th.Property("QtyToDeliver", th.NumberType),
        th.Property("Price", th.NumberType),
        th.Property("Description", th.StringType),
        th.Property("CreditorProductCode", th.StringType),
        th.Property("ProductDesc1", th.StringType),
        th.Property("ProductDesc2", th.StringType),
        th.Property("StandardAmountQTY", th.NumberType),
        th.Property("StandardAmountQTYUnitId", th.IntegerType),
        th.Property("BuyOrderId", th.IntegerType),
        th.Property("ExpectedDeliveryDate", th.DateTimeType),
        th.Property("QtyToOrder", th.NumberType),
        th.Property("OrderedOnDateByDistributor", th.DateTimeType),
        th.Property("OrderRowId", th.IntegerType),
    ).to_dict()

    def prepare_request_payload(self, context, next_page_token):
        return context["OrderId"]
    
    def get_next_page_token(self, response, previous_token):
        return None


class BuyOrderDeliveriesStream(Logic4Stream):
    """Define custom stream for buy order deliveries."""

    name = "buy_order_deliveries"
    path = "/v1/BuyOrderDeliveries/GetBuyOrderDeliveries"
    primary_keys = ["BuyOrderDeliveryId"]
    replication_key = "DateTimeCreated"
    rep_key_field = "CreationDateFrom"
    page_size = 1000

    schema = th.PropertiesList(
        th.Property("BuyOrderDeliveryId", th.IntegerType),
        th.Property("DateTimeCreated", th.DateTimeType),
        th.Property("DateTimeProcessed", th.DateTimeType),
        th.Property("StatusId", th.IntegerType),
        th.Property("TypeId", th.IntegerType),
        th.Property("SupplierId", th.IntegerType),
        th.Property("BuyOrderId", th.IntegerType),
        th.Property("Remarks", th.StringType),
        th.Property("Description", th.StringType),
        th.Property("BranchId", th.IntegerType),
        th.Property("PickingListNumber", th.StringType),
        th.Property("Rows", th.ArrayType(
            th.ObjectType(
                th.Property("BuyPrice", th.NumberType),
                th.Property("Id", th.IntegerType),
                th.Property("ProductCode", th.StringType),
                th.Property("VendorCode", th.StringType),
                th.Property("ProductCodeSupplier", th.StringType),
                th.Property("BuyOrderRowId", th.IntegerType),
                th.Property("DebtorName", th.StringType),
                th.Property("OrderId", th.IntegerType),
                th.Property("ProductId", th.IntegerType),
                th.Property("Qty_Delivered", th.NumberType),
                th.Property("Remarks", th.StringType),
                th.Property("StockLocationId", th.IntegerType),
                th.Property("AmountOfLabelsToPrint", th.IntegerType),
            )
        )),
    ).to_dict()

    def prepare_request_payload(self, context, next_page_token):
        #NOTE: Logic4 uses Amsterdam timezone
        start_date = self.get_starting_time(context)
        if self.stream_state.get("replication_key_value"):
            start_date = start_date + timedelta(seconds=1)
        start_date = (
            start_date.strftime("%Y-%m-%dT%H:%M:%SZ") if start_date else start_date
        )
        payload = {}
        payload["Take"] = self.page_size
        if start_date:
            payload[self.rep_key_field] = start_date
        if next_page_token:
            payload["Skip"] = next_page_token
        self.logger.info(f"Making request to '{self.path}' with payload: {payload}")
        return payload

    def get_next_page_token(self, response, previous_token):
        """Return a token for identifying next page or None if no more pages."""
        counter = response.json().get("RecordsCounter")
        if counter:
            previous_token = previous_token or 0
            next_page_token = previous_token + counter
            return next_page_token
        return None


class SuppliersStream(Logic4Stream):
    """Define custom stream."""

    name = "suppliers"
    path = "/v1.1/Relations/GetCreditors"
    primary_keys = ["Id"]

    schema = th.PropertiesList(
        th.Property("CompanyName", th.StringType),
        th.Property("EmailAddress", th.StringType),
        th.Property("Faxnumber", th.StringType),
        th.Property("FirstName", th.StringType),
        th.Property("Id", th.IntegerType),
        th.Property("LastName", th.StringType),
        th.Property("MobileNumber", th.StringType),
        th.Property("TelephoneNumber", th.StringType),
    ).to_dict()
