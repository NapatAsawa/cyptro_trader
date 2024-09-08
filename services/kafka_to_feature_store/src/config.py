import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
kafka_broker_address = "redpanda-0:9092"
#kafka_broker_address = "localhost:19092"
kafka_topic="ohlc"
kafka_consumer_group="trade_to_ohlc_consumer_group"
feature_group_name="ohlc_feature_group"
feature_group_version=2
buffer_size = 1000

hopsworks_project_name = os.environ["HOPSWORKS_PROJECT_NAME"]
hopsworks_api_key = os.environ["HOPSWORKS_API_KEY"]