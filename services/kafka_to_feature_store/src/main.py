from typing import Optional
from quixstreams import Application
from src import config
from loguru import logger
import json
from src.hopsworks_api import push_data_to_feature_store

def kafka_to_feature_store(
    kafka_topic: str,
    kafka_broker_address: str,
    kafka_consumer_group: str,
    feature_group_name: str,
    feature_group_version: int,
    buffer_size: Optional[int] = 1,
) -> None:
    """
    Reads `ohlc` data from the Kafka topic and writes it to the feature store.
    More specifically, it writes the data to the feature group specified by
    - `feature_group_name` and `feature_group_version`.

    Args:
        kafka_topic (str): The Kafka topic to read from.
        kafka_broker_address (str): The address of the Kafka broker.
        kafka_consumer_group (str): The Kafka consumer group we use for reading messages.
        feature_group_name (str): The name of the feature group to write to.
        feature_group_version (int): The version of the feature group to write to.
        buffer_size (int): The number of messages to read from Kafka before writing to the feature store.

    Returns:
        None
    """
    app = Application(
        broker_address=kafka_broker_address,
        consumer_group=kafka_consumer_group,
        #auto_offset_reset="earliest" #process all data
        auto_offset_reset="latest" #process only latest data
    )

    with app.get_consumer() as consumer:
        consumer.subscribe(topics=[kafka_topic])
        logger.info("start consume")
        ohlc_buffer = []
        while True:
            
            msg = consumer.poll(1)

            if msg is None:
                continue
            elif msg.error():
                logger.error('kafka error', msg.error())
            else:
                ohlc = json.loads(msg.value().decode('utf-8'))
                logger.info(ohlc)
                ohlc_buffer.append(ohlc)
                logger.info(len(ohlc_buffer))
                if len(ohlc_buffer) >= buffer_size:
                    push_data_to_feature_store(
                        feature_group_name = feature_group_name,
                        feature_group_version = feature_group_version,
                        data = ohlc_buffer
                    )
                    ohlc_buffer = []

            # Storing offset only after the message is processed enables at-least-once delivery
            consumer.store_offsets(message=msg)



if __name__ == '__main__':

    try:
        kafka_to_feature_store(
            kafka_topic=config.kafka_topic,
            kafka_broker_address=config.kafka_broker_address,
            kafka_consumer_group=config.kafka_consumer_group,
            feature_group_name=config.feature_group_name,
            feature_group_version=config.feature_group_version,
            buffer_size=config.buffer_size
        )
    except KeyboardInterrupt:
        logger.info('Exiting neatly!')