from dataclasses import dataclass
from typing import Optional, Literal

@dataclass
class Limit:
    type: str
    min: float
    max: float

@dataclass
class Network:
    id: str
    name: str
    active: bool
    fee: float
    deposit: bool
    withdraw: bool

@dataclass
class ExchangeCurrency:
    exchange: str
    ccy: str
    active: bool
    deposit: bool
    withdraw: bool
    taker_fee: float
    maker_fee: float
    precision: dict[Literal['price','amount','cost'], int | float]
    limits: dict[Literal['amount','price','cost','leverage'], dict[str, float]]
    networks: list[Network]

    def to_row(self) -> tuple:
        import json
        from datetime import datetime
        return (
            self.exchange,
            self.ccy,
            self.active,
            self.deposit,
            self.withdraw,
            self.taker_fee,
            self.maker_fee,
            json.dumps(self.precision),
            json.dumps(self.limits),
            json.dumps([n.__dict__ for n in self.networks]),
            datetime.utcnow()
        )