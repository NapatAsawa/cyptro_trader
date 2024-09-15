import os
kafka_broker_address = os.environ["KAFKA_BROKER_ADDRESS"]

kafka_input_topic="trades"
kafka_output_topic="ohlcs"
kafka_consumer_group="trade_to_ohlc_consumer_group_2"
ohlc_window_seconds=10
