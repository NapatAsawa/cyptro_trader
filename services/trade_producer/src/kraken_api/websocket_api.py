from typing import List, Dict
from websocket import create_connection
import json
from loguru import logger


class KrakenWebSocketApi:
    def __init__(
        self,
        product_id: str
    ):
        self.url = 'wss://ws.kraken.com/v2'
        self.product_id = product_id 
        self._ws = create_connection(self.url)
        self._subscribe(product_id)

    def _subscribe(self, product_id: str):
        """
        Establish connection to the Kraken websocket API and subscribe to the trades for the given `product_id`.
        """
        logger.info("Connection establised")
        msg = {
            "method": "subscribe",
            "params": {
                "channel": "trade",
                "symbol": [
                    product_id
                ],
                "snapshot": False
            }
        }
        self._ws.send(json.dumps(msg))

        # dumping the first 2 message, because they contain no trade data
        # only for confirmation on their end that the connection is success
        _ = self._ws.recv()
        _ = self._ws.recv()
        logger.info("Subscription worked")

    def get_trades(self) -> List[Dict]:
        """
        Fetches trade data from the Kraken Websocket API and returns a list of Trades.
        """
        msg = self._ws.recv()
        if 'heartbeat' in msg:
            return []
        msg = json.loads(msg)
        trades = []
        for trade in msg['data']:
            trades.append(
                {
                    'product_id': self.product_id,
                    'price': trade['price'],
                    'volume': trade['qty'],
                    'timestamp_ms': trade['timestamp']
                }
            )
        return trades
    
    def is_done(self) -> bool:
        return False