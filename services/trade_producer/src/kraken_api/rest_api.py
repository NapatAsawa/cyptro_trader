import requests
from typing import Optional, Tuple
import json
from datetime import datetime, timezone
from loguru import logger
from time import sleep
class KrakenRestApi:
    def __init__(
        self,
        product_id: str,
        last_n_days: int,
        cache_dir: Optional[str] = None,
    ) -> None:
        """
        Args:
            product_id (str): One product ID for which we want to get the trades.
            last_n_days (int): The number of days from which we want to get historical data.
            cache_dir (Optional[str]): The directory where we will store the historical data to
        """
        self.url = f"https://api.kraken.com/0/public/Trades"
        self.product_id = product_id = product_id
        self.from_ms, self.to_ms = self._init_from_to_ms(last_n_days)
        self.last_trade_ms = self.from_ms

    @staticmethod
    def _init_from_to_ms(last_n_days: int) -> Tuple[int, int]:
        """
        Returns the from_ms and to_ms timestamps for the historical data.
        These values are computed using today's date at midnight and the last_n_days.

        Args:
            last_n_days (int): The number of days from which we want to get historical data.

        Returns:
            Tuple[int, int]: A tuple containing the from_ms and to_ms timestamps.
        """

        today_date = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        # today_date to milliseconds
        to_ms = int(today_date.timestamp() * 1000)

        # from_ms is last_n_days ago from today, so
        from_ms = to_ms - last_n_days * 24 * 60 * 60 * 1000

        return from_ms, to_ms

    def get_trades(self):
        since_ns = self.last_trade_ms * 1_000_000
        request_url = f"{self.url}?pair={self.product_id}&since={since_ns}"
        try:
            response = requests.request("GET", request_url)
            data = json.loads(response.text)
        except:
            raise Exception(request_url)
        if ('error' in data) and ('EGeneral:Too many requests' in data['error']):
                logger.info('Too many requests. Sleeping for 30 seconds')
                sleep(30)
        trades = [
                {
                    'product_id':self.product_id,
                    'price': float(trade[0]),
                    'volume': float(trade[1]),
                    'timestamp_ms': int(trade[2] * 1000)
                }
                for trade in data['result'][self.product_id]
            ]

        # update last_trade_ms for stop condition
        if trades[-1]['timestamp_ms'] == self.last_trade_ms:
            # if the last trade timestamp in the batch is the same as self.last_trade_ms,
            # then we need to increment it by 1 to avoid repeating the exact same API request,
            # which would result in an infinite loop
            self.last_trade_ms = self.last_trade_ms + 1
        else:
            # otherwise, update self.last_trade_ms to the timestamp of the last trade
            # in the batch
            self.last_trade_ms = trades[-1]['timestamp_ms'] 
        return trades
    def is_done(self) -> bool:
        return self.last_trade_ms >= self.to_ms