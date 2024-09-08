import hopsworks
from src.config import *
from typing import List
import pandas as pd
def push_data_to_feature_store(
    feature_group_name: str,
    feature_group_version: int,
    data: List[dict]
) -> None:
    """
    Pushes the given `data` to the feature store, writing it to the feature group
    with name `feature_group_name` and version `feature_group_version`.

    Args:
        feature_group_name (str): The name of the feature group to write to.
        feature_group_version (int): The version of the feature group to write to.
        data (List[dict]): The data to write to the feature store.
    Returns:
        None
    """
    project = hopsworks.login(
        project=hopsworks_project_name,
        api_key_value=hopsworks_api_key,
    )
    feature_store = project.get_feature_store()
    ohlc_feature_group = feature_store.get_or_create_feature_group(
        name=feature_group_name,
        version=feature_group_version,
        description='OHLC data coming from Kraken',
        primary_key=['product_id', 'timestamp'],
        event_time='timestamp',
        online_enabled=True,
    )
    data = pd.DataFrame(data)

    # Write the data to the feature group
    ohlc_feature_group.insert(
        data,
        write_options={
            'start_offline_materialization': False
        }
    )
