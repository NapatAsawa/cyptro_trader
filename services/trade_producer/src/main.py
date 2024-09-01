from quixstreams import Application
from loguru import logger
from src.kraken_api import KrakenApi

def produce_trades(
        kafka_broker_address: str,
        kafka_topic:str
    ) -> None:

    app = Application(broker_address=kafka_broker_address)
    topic = app.topic(name = kafka_topic, value_deserializer='json')
    karken_api = KrakenApi(product_id= "BTC/USD")
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
                logger.info("message saved")
            from time import sleep
            sleep(1)
            

if __name__ == '__main__':
    produce_trades(
        #kafka_broker_address = "localhost:19092",
        kafka_broker_address = "redpanda-0:9092",
        kafka_topic = "trades"
    )

