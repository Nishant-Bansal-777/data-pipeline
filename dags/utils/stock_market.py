import requests
from urllib.parse import urlencode
from dataclasses import dataclass, asdict
from enum import Enum
from datetime import datetime
import logging

logging.basicConfig(datefmt='%B-%d-%Y %I:%M:%S', format='%(asctime)s | %(levelname)s | %(message)s', level=logging.INFO,
                    handlers=[logging.FileHandler(filename='logfile.log', mode='w'), logging.StreamHandler()])
logger = logging.getLogger()

"""https://www.nirmalbang.com/equity/top-gainers-and-losers.aspx"""


class Exchange(Enum):
    NSE = 'NSE'
    BSE = 'BSE'


class Type(Enum):
    GAIN = 'GAIN'
    LOSE = 'LOSE'


class Period(Enum):
    DAILY = '1'
    WEEKLY = '2'
    MONTHLY = '3'


def get_api_url(type_: Type, exchange: Exchange = Exchange.NSE, period: Period = Period.DAILY):
    params = {
        'Exchange': exchange.value,
        'Type': type_.value,
        'Period': period.value,
        'Group': '',
        'Indices': '',
        'SortExp': 'PERCHG',
        'SortDirect': 'DESC',
        'pageNo': 1,
        'PageSize': 10
    }
    encoded_params = urlencode(params)
    url = f'https://www.nirmalbang.com/ajaxpages/equity/EquitygainLose.aspx?{encoded_params}'
    return url


def get_data(api):
    headers = {
        'authority': 'www.nirmalbang.com',
        'method': 'GET',
        'path': api.split('.com')[-1],
        'scheme': 'https',
        'Accept': '*/*',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'en-US,en;q=0.9',
        'Referer': 'https://www.nirmalbang.com/',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36',
    }

    try:
        r = requests.get(api, headers=headers)
        data = r.json()

    except Exception as e:
        logger.error(f'Error in api: {e}')
        data = []

    return data


@dataclass
class Record:
    FINCODE: int
    S_NAME: str
    CLOSE_PRICE: float
    PREVCLOSE: float
    PERCHG: float
    NETCHG: float
    VOLUME: float
    TIMESTAMP: str = str(datetime.now())

    @classmethod
    def from_dict(cls, data):
        return cls(
            FINCODE=int(data.get('FINCODE', 0)),
            S_NAME=data.get('S_NAME', ''),
            CLOSE_PRICE=float(data.get('CLOSE_PRICE', 0.0)),
            PREVCLOSE=float(data.get('PREVCLOSE', 0.0)),
            PERCHG=float(data.get('PERCHG', 0.0)),
            NETCHG=float(data.get('NETCHG', 0.0)),
            VOLUME=float(data.get('VOLUME', 0.0))
        )

    def to_dict(self):
        return asdict(self)


if __name__ == '__main__':
    """local testing"""
    from kafka import KafkaProducer
    import json
    import time
    gainer_api = get_api_url(Type.GAIN)
    loser_api = get_api_url(Type.LOSE)
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=5000)
    records = get_data(loser_api)
    for item in records:
        record = Record.from_dict(item).to_dict()
        producer.send(topic='lose', value=json.dumps(record).encode('utf-8'))
        print(record)
        time.sleep(2)


