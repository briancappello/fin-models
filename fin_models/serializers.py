from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from marshmallow import Schema, fields, post_load

from fin_models.dataclasses import Address, CompanyDetails, HistoricalMetadata
from fin_models.enums import Freq


class BaseSerializer(Schema):
    __model__ = dict

    @post_load()
    def post_load(self, data, **kwargs):
        return self.__model__(**data)


class TimestampUTC(fields.AwareDateTime):
    def __init__(self):
        super().__init__(default_timezone=timezone.utc)

    def _deserialize(self, value, attr, data, **kwargs) -> pd.Timestamp:
        if not isinstance(value, (datetime, pd.Timestamp)):
            value = super()._deserialize(value, attr, data, **kwargs)
        return pd.Timestamp(value).astimezone("UTC")


class AddressSerializer(BaseSerializer):
    __model__ = Address

    address1 = fields.String()
    address2 = fields.String(required=False, allow_none=True)
    city = fields.String()
    state = fields.String()
    postal_code = fields.String()


class CompanyDetailsSerializer(BaseSerializer):
    __model__ = CompanyDetails

    active = fields.Boolean()
    address = fields.Nested(AddressSerializer(), allow_none=True)
    currency_name = fields.String()
    locale = fields.String()
    market = fields.String()
    name = fields.String()
    ticker = fields.String()

    cik = fields.String(required=False, allow_none=True)
    composite_figi = fields.String(required=False, allow_none=True)
    delisted_utc = fields.Date(required=False, allow_none=True)
    description = fields.String(required=False, allow_none=True)
    homepage_url = fields.String(required=False, allow_none=True)
    list_date = fields.Date(required=False, allow_none=True)
    market_cap = fields.Integer(required=False, allow_none=True)
    phone_number = fields.String(required=False, allow_none=True)
    primary_exchange = fields.String(required=False, allow_none=True)
    round_lot = fields.Integer(required=False, allow_none=True)
    share_class_figi = fields.String(required=False, allow_none=True)
    share_class_shares_outstanding = fields.Integer(required=False, allow_none=True)
    sic_code = fields.String(required=False, allow_none=True)
    sic_description = fields.String(required=False, allow_none=True)
    ticker_root = fields.String(required=False, allow_none=True)
    ticker_suffix = fields.String(required=False, allow_none=True)
    total_employees = fields.Integer(required=False, allow_none=True)
    type = fields.String(required=False, allow_none=True)
    weighted_shares_outstanding = fields.Integer(required=False, allow_none=True)


class HistoricalMetadataSerializer(BaseSerializer):
    __model__ = HistoricalMetadata

    freq = fields.Enum(Freq, by_value=True)
    first_bar_utc = TimestampUTC()
    latest_bar_utc = TimestampUTC()
    timezone = fields.String(load_default="America/New_York")

    Open = fields.Float()
    High = fields.Float()
    Low = fields.Float()
    Close = fields.Float()
    Volume = fields.Integer()
