from datetime import timedelta
from quixstreams import Application
from src import config
from loguru import logger

def init_ohlc_candle(value: dict) -> dict:
        """
        Initialize the OHLC candle with the first trade
        """
        return {
            'open': value['price'],
            'high': value['price'],
            'low': value['price'],
            'close': value['price'],
            'product_id': value['product_id']
        }

def update_ohlc_candle(ohlc_candle: dict, trade: dict) -> dict:
    """
    Update the OHLC candle with the new trade and return the updated candle

    Args:
        ohlc_candle : dict : The current OHLC candle
        trade : dict : The incoming trade

    Returns:
        dict : The updated OHLC candle
    """
    return {
        'open': ohlc_candle['open'],
        'high': max(ohlc_candle['high'], trade['price']),
        'low': min(ohlc_candle['low'], trade['price']),
        'close': trade['price'],
        'product_id': trade['product_id']
    }
def custom_ts_extractor(
    value,
    headers,
    timestamp: float,
    timestamp_type,  #: TimestampType,
) -> int:
    """
    Specifying a custom timestamp extractor to use the timestamp from the message payload
    instead of Kafka timestamp.

    We want to use the `timestamp_ms` field from the message value.
    
    See the Quix Streams documentation here
    https://quix.io/docs/quix-streams/windowing.html#extracting-timestamps-from-messages
    """
    return value['timestamp_ms']

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
        consumer_group=kafka_consumer_group,
        #auto_offset_reset="earliest" #process all data
        auto_offset_reset="latest" #process only latest data
    )

    input_topic = app.topic(
         name = kafka_input_topic,  
         timestamp_extractor=custom_ts_extractor, 
         value_deserializer='json')
    output_topic = app.topic(name = kafka_output_topic, value_deserializer='json')

    sdf = app.dataframe(input_topic)
    sdf = sdf.tumbling_window(duration_ms=timedelta(seconds=ohlc_window_seconds))
    sdf = sdf.reduce(reducer=update_ohlc_candle, initializer=init_ohlc_candle).final()

    sdf['open'] = sdf['value']['open']
    sdf['high'] = sdf['value']['high']
    sdf['low'] = sdf['value']['low']
    sdf['close'] = sdf['value']['close']
    sdf['product_id'] = sdf['value']['product_id']
    sdf['timestamp'] = sdf['end']
    sdf = sdf[['timestamp', 'open', 'high', 'low', 'close', 'product_id']]

    #sdf = sdf.update(logger.info)

    sdf = sdf.to_topic(output_topic)
    app.run(sdf)

if __name__ == '__main__':
    trade_to_ohlc(
        kafka_input_topic=config.kafka_input_topic,
        kafka_output_topic=config.kafka_output_topic,
        kafka_broker_address=config.kafka_broker_address,
        kafka_consumer_group=config.kafka_consumer_group,
        ohlc_window_seconds=config.ohlc_window_seconds,
    )