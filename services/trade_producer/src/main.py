from quixstreams import Application

def produce_trades(
        kafka_broker_address: str,
        kafka_topic:str
    ) -> None:
    app = Application(broker_address=kafka_broker_address)
    topic = app.topic(name = kafka_topic, value_deserializer='json')

    messages = [
        {"chat_id": "id1", "text": "Lorem ipsum dolor sit amet"},
        {"chat_id": "id2", "text": "Consectetur adipiscing elit sed"},
        {"chat_id": "id1", "text": "Do eiusmod tempor incididunt ut labore et"},
        {"chat_id": "id3", "text": "Mollis nunc sed id semper"},
    ]

    with app.get_producer() as producer:
        from src.kraken_api import KrakenApi
        karken_api = KrakenApi(product_id= "BTC/USD")
        while True:
            trades = karken_api.get_trades()
            for trade in trades:
                message = topic.serialize(key=trade["product_id"], value=trade)
                producer.produce(
                    topic=topic.name,
                    key=message.key,
                    value=message.value,
                )
                print("message saved")
            from time import sleep
            sleep(1)
            

if __name__ == '__main__':
    produce_trades(
        kafka_broker_address = "localhost:19092",
        kafka_topic = "trades"
    )

