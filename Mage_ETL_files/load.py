from mage_ai.settings.repo import get_repo_path
from mage_ai.io.bigquery import BigQuery
from mage_ai.io.config import ConfigFileLoader
from pandas import DataFrame
from os import path

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter




@data_exporter
def export_data_to_big_query(data, **kwargs) -> None:
    """
    Template for exporting data to a BigQuery warehouse.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#bigquery

    
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    for key, value in data.items():
        table_id = 'nyc-taxi-project-mrunal.nyc_taxi_data_engineering_project.{}'.format(key)
        BigQuery.with_config(ConfigFileLoader(config_path, config_profile)).export(
            DataFrame(value),
            table_id,
            if_exists='replace',  # Specify resolution policy if table name already exists
        )


# Output
# ========================

# BigQuery initialized

# └─ Connecting to BigQuery warehouse...DONE

# ├─ 

# └─ Exporting data to table 'nyc-taxi-project-mrunal.nyc_taxi_data_engineering_project.datetime_dim'...

# DONEBigQuery initialized

# ├─ Connecting to BigQuery warehouse...DONE

# └─ Exporting data to table 'nyc-taxi-project-mrunal.nyc_taxi_data_engineering_project.passenger_count_dim'...

# DONEBigQuery initialized

# ├─ Connecting to BigQuery warehouse...DONE

# └─ Exporting data to table 'nyc-taxi-project-mrunal.nyc_taxi_data_engineering_project.trip_distance_dim'...

# DONEBigQuery initialized

# ├─ Connecting to BigQuery warehouse...DONE

# └─ Exporting data to table 'nyc-taxi-project-mrunal.nyc_taxi_data_engineering_project.rate_code_dim'...

# DONEBigQuery initialized

# ├─ Connecting to BigQuery warehouse...DONE

# └─ Exporting data to table 'nyc-taxi-project-mrunal.nyc_taxi_data_engineering_project.pickup_location_dim'...

# DONEBigQuery initialized

# ├─ Connecting to BigQuery warehouse...DONE

# └─ Exporting data to table 'nyc-taxi-project-mrunal.nyc_taxi_data_engineering_project.dropoff_location_dim'...

# DONEBigQuery initialized

# ├─ Connecting to BigQuery warehouse...DONE

# └─ Exporting data to table 'nyc-taxi-project-mrunal.nyc_taxi_data_engineering_project.payment_type_dim'...

# DONEBigQuery initialized

# └─ Connecting to BigQuery warehouse...DONE

# ├─ 

# └─ Exporting data to table 'nyc-taxi-project-mrunal.nyc_taxi_data_engineering_project.fact_table'...

# DONE