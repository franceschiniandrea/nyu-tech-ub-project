import ccxt.async_support as ccxt
import logging
import json
from typing import Literal
from datetime import datetime
from crypto_hft.utils.config import Config

config = Config()
config = Config()

class Limit():
    def __init__(self, type: str, min: float, max: float):
        self.type = type
        self.min = min
        self.max = max

    @classmethod
    def from_dict(cls, type: str, data: dict):
        return cls(
            type=type,
            min=data['min'],
            max=data['max']
        )

class Network():
    def __init__(
        self,
        id: str,
        name: str,
        active: bool,
        fee: float,
        deposit: bool,
        withdraw: bool,
        withdrawal_limits: Limit,
        deposit_limits: Limit,
        is_complete: bool = True
    ):
        self.id = id
        self.name = name
        self.active = active
        self.fee = fee
        self.deposit = deposit
        self.withdraw = withdraw
        self.withdrawal_limits = withdrawal_limits
        self.deposit_limits = deposit_limits
        self.is_complete = is_complete

    @classmethod
    def from_dict(cls, data: dict):
        try:
            return cls(
                id=data['id'],
                name=data['name'],
                active=data['active'],
                fee=data['fee'],
                deposit=data['deposit'],
                withdraw=data['withdraw'],
                withdrawal_limits=Limit.from_dict('withdraw', data['limits']['withdraw']),
                deposit_limits=Limit.from_dict('deposit', data['limits']['deposit']),
                is_complete=True
            )
        except KeyError as e:
            raise KeyError(str(e))
