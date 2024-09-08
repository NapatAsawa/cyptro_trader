from quixstreams import Application
from loguru import logger
from src.kraken_api.websocket_api import KrakenWebSocketApi
from src.kraken_api.rest_api import KrakenRestApi
from src import config

def produce_trades(
        kafka_broker_address: str,
        kafka_topic:str,
        product_id: str,
        live_or_historical: str,
        last_n_days: int,
    ) -> None:
    """
    Reads trades from the Kraken websocket API and saves them into a Kafka topic.

    Args:
        kafka_broker_addres (str): The address of the Kafka broker.
        kafka_topic (str): The name of the Kafka topic.
        product_id (str): The product ID for which we want to get the trades.
         live_or_historical (str): Whether we want to get live or historical data.
        last_n_days (int): The number of days from which we want to get historical data.
    Returns:
        None
    """
    app = Application(broker_address=kafka_broker_address)
    topic = app.topic(name = kafka_topic, value_deserializer='json')

    if live_or_historical == 'live':
        kraken_api = KrakenWebSocketApi(product_id= product_id)
    elif live_or_historical == 'historical':
        kraken_api = KrakenRestApi(
            product_id=product_id,
            last_n_days=last_n_days
        )

    with app.get_producer() as producer:
        while True:
            if kraken_api.is_done():
                logger.info('Done fetching historical data')
                break
            trades = kraken_api.get_trades()
            for trade in trades:
                message = topic.serialize(key=trade["product_id"], value=trade)
                producer.produce(
                    topic=topic.name,
                    key=message.key,
                    value=message.value,
                )
                #logger.info(message.value)
            

if __name__ == '__main__':
    produce_trades(
        kafka_broker_address = config.kafka_broker_address,
        kafka_topic = config.kafka_topic,
        product_id=config.product_id,
        live_or_historical = config.live_or_historical,
        last_n_days = config.last_n_days
    )

