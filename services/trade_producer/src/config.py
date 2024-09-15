import os
live_or_historical = os.environ["LIVE_OR_HISTORICAL"]
kafka_broker_address = os.environ["KAFKA_BROKER_ADDRESS"]
kafka_topic = "trades"
product_id= "BTC/USD"
last_n_days = 1