import os
from dotenv import load_dotenv, find_dotenv
load_dotenv(find_dotenv())
kafka_broker_address = os.environ["KAFKA_BROKER_ADDRESS"]
live_or_historical = os.environ["LIVE_OR_HISTORICAL"]
kafka_topic="ohlcs"
kafka_consumer_group="ohlc_consumer_group_99"
feature_group_name="ohlc_feature_group"
feature_group_version=4

if live_or_historical == 'live':
    buffer_size = 1
    save_every_n_sec = 600
elif live_or_historical == 'historical':
    buffer_size = 10
    save_every_n_sec = 60
hopsworks_project_name = os.environ["HOPSWORKS_PROJECT_NAME"]
hopsworks_api_key = os.environ["HOPSWORKS_API_KEY"]