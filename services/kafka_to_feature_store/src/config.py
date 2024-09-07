import os
kafka_broker_address = "redpanda-0:9092"
#kafka_broker_address = "localhost:19092"
kafka_topic="ohlc"
kafka_consumer_group="trade_to_ohlc_consumer_group"
feature_group_name="ohlc_feature_group"
feature_group_version=1

# hopsworks_project_name = os.environ["HOPSWORKS_PROJECT_NAME"]
# hopsworks_api_key = os.environ["HOPSWORKS_API_KEY"]
hopsworks_api_key = "bIe6uArciWpewFsy.WxvXDxOwnCsmXeroVYuus5lN3IbLRnrZHsiQlZEn2KXoMn0hr8zfuVuBIMRlJV1n"
hopsworks_project_name = "nontty12"