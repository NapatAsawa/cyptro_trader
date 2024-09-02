from quixstreams import Application

def trade_to_ohlc(
        kafka_input_topic: str,
        kafka_output_topic: str,
        kafka_broker_address: str,
        kafka_consumer_group: str,
        ohlc_window_seconds: int,
    ) -> None:
    """
    Reads trades from the kafka input topic
    Aggregates them into OHLC candles using the specified window in `ohlc_window_seconds`
    Saves the ohlc data into another kafka topic

    Args:
        kafka_input_topic : str : Kafka topic to read trade data from
        kafka_output_topic : str : Kafka topic to write ohlc data to
        kafka_broker_address : str : Kafka broker address
        kafka_consumer_group : str : Kafka consumer group
        ohlc_window_seconds : int : Window size in seconds for OHLC aggregation

    Returns:
        None
    """
    app = Application(
        broker_address=kafka_broker_address,
        consumer_group="trade_to_ohlc"
    )

    input_topic = app.topic(name = kafka_input_topic, value_deserializer='json')
    output_topic = app.topic(name = kafka_output_topic, value_deserializer='json')