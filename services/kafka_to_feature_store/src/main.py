from typing import Optional
from quixstreams import Application
from src import config
from loguru import logger
import json
from src.hopsworks_api import push_data_to_feature_store
from datetime import datetime, timezone

def get_current_utc_sec() -> int:
    return int(datetime.now(timezone.utc).timestamp())

def kafka_to_feature_store(
    kafka_topic: str,
    kafka_broker_address: str,
    kafka_consumer_group: str,
    feature_group_name: str,
    feature_group_version: int,
    buffer_size: Optional[int] = 1,
    live_or_historical: Optional[str] = 'live',
    save_every_n_sec: Optional[int] = 600
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
         live_or_historical (str): Whether we are saving live data to the Feature or historical data.
    Returns:
        None
    """
    app = Application(
        broker_address=kafka_broker_address,
        consumer_group=kafka_consumer_group,
        auto_offset_reset="earliest" if live_or_historical == 'historical' else "latest"
    )
    last_saved_to_feature_store_ts = get_current_utc_sec()
    with app.get_consumer() as consumer:
        consumer.subscribe(topics=[kafka_topic])
        logger.info("start consume")
        ohlc_buffer = []
        while True:
            msg = consumer.poll(1)
            sec_since_last_saved = get_current_utc_sec() - last_saved_to_feature_store_ts
            if (msg is not None) and msg.error():
                logger.error('Kafka error:', msg.error())
                continue
            elif (msg is None) and (sec_since_last_saved < save_every_n_sec):
                # There are no new messages in the input topic and we haven't hit the timer
                # limit yet. We skip and continue polling messages from Kafka.
                # logger.debug('No new messages in the input topic')
                # logger.debug(
                #     f'Last saved to feature store {sec_since_last_saved} seconds ago (limit={save_every_n_sec})'
                # )
                continue
            else:
                if msg is not None:
                    ohlc = json.loads(msg.value().decode('utf-8'))
                    #logger.info(ohlc)
                    ohlc_buffer.append(ohlc)
                if (len(ohlc_buffer) >= buffer_size) or (
                    sec_since_last_saved >= save_every_n_sec
                ):
                    # if the buffer is not empty we write the data to the feature store
                    if len(ohlc_buffer) > 0:
                        try:
                            push_data_to_feature_store(
                                feature_group_name=feature_group_name,
                                feature_group_version=feature_group_version,
                                data=ohlc_buffer,
                                online_or_offline='online'
                                if live_or_historical == 'live'
                                else 'offline',
                            )
                        except Exception as e:
                            logger.error(
                                f'Failed to push data to the feature store: {e}'
                            )
                            continue
                    ohlc_buffer = []
                    last_saved_to_feature_store_ts = get_current_utc_sec()
                    logger.info("load buffer to feature store")

            # Storing offset only after the message is processed enables at-least-once delivery
            #consumer.store_offsets(message=msg)



if __name__ == '__main__':

    try:
        kafka_to_feature_store(
            kafka_topic=config.kafka_topic,
            kafka_broker_address=config.kafka_broker_address,
            kafka_consumer_group=config.kafka_consumer_group,
            feature_group_name=config.feature_group_name,
            feature_group_version=config.feature_group_version,
            buffer_size=config.buffer_size,
            live_or_historical = config.live_or_historical,
            save_every_n_sec = config.save_every_n_sec
        )
    except KeyboardInterrupt:
        logger.info('Exiting neatly!')