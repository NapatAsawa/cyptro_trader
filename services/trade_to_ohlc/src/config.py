kafka_broker_address = "redpanda-0:9092"
#kafka_broker_address = "localhost:19092"
kafka_input_topic="trades"
kafka_output_topic="ohlc"
kafka_consumer_group="trade_to_ohlc_consumer_group"
ohlc_window_seconds=10