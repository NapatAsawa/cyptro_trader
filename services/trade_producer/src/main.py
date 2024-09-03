from quixstreams import Application
from loguru import logger
from src.kraken_api import KrakenApi
from src import config

def produce_trades(
        kafka_broker_address: str,
        kafka_topic:str
    ) -> None:
    """
    Reads trades from the Kraken websocket API and saves them into a Kafka topic.

    Args:
        kafka_broker_addres (str): The address of the Kafka broker.
        kafka_topic (str): The name of the Kafka topic.

    Returns:
        None
    """
    app = Application(broker_address=kafka_broker_address)
    topic = app.topic(name = kafka_topic, value_deserializer='json')
    karken_api = KrakenApi(product_id= config.product_id)
    with app.get_producer() as producer:
        while True:
            trades = karken_api.get_trades()
            for trade in trades:
                message = topic.serialize(key=trade["product_id"], value=trade)
                producer.produce(
                    topic=topic.name,
                    key=message.key,
                    value=message.value,
                )
                logger.info(message.value)
            from time import sleep
            sleep(1)
            

if __name__ == '__main__':
    produce_trades(
        kafka_broker_address = config.kafka_broker_address,
        kafka_topic = config.kafka_topic
    )

