from datetime import date
from decimal import Decimal
from pydantic import BaseModel, Field, field_validator


class PortfolioCreate(BaseModel):
    portfolio_name: str = Field("My Portfolio", min_length=1, max_length=120)


class HoldingCreate(BaseModel):
    symbol: str = Field(..., min_length=1, max_length=32)
    quantity: Decimal = Field(..., gt=0)
    avg_buy_price: Decimal = Field(..., gt=0)
    buy_date: date

    @field_validator("symbol")
    @classmethod
    def normalize_symbol(cls, value: str) -> str:
        return value.strip().upper()


class HoldingUpdate(BaseModel):
    quantity: Decimal | None = Field(None, gt=0)
    avg_buy_price: Decimal | None = Field(None, gt=0)
    buy_date: date | None = None
    requires_confirmation: bool | None = None


class HoldingSold(BaseModel):
    sell_date: date
    sell_price: Decimal = Field(..., gt=0)
