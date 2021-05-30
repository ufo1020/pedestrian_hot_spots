from io import StringIO

import boto3
import pandas as pd
import requests

PEDESTRIAN_COUNT_URL = "https://data.melbourne.vic.gov.au/resource/b2ak-trbp.json"
SENSOR_LOCATION_URL = "https://data.melbourne.vic.gov.au/resource/h57g-5234.json"

S3_BUCKET = "data_bucket"
S3_OUTPUT_PREFIX = "pedestrian-count/"


class BadRequest(Exception):
    pass


def write_to_s3(bucket, path, data):
    s3 = boto3.resource('s3')
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, sep='|', encoding='utf-8', header=True, index=False)
    s3.Object(bucket, path).put(Body=csv_buffer.getvalue())


def get_sensor_locations():
    try:
        response = requests.get(SENSOR_LOCATION_URL)
        response.raise_for_status()  # any errors?
        return pd.DataFrame(response.json())

    except Exception as e:
        raise


def get_pedestrain_count(query):
    try:
        response = requests.get(PEDESTRIAN_COUNT_URL, params=query)
        response.raise_for_status()  # any errors?
        return pd.DataFrame(response.json())
    except Exception as e:
        raise


def get_top_locations_by_month(top_n):
    try:
        sensor_locations = get_sensor_locations()
    except Exception as e:
        raise BadRequest(f"Failed to get sensor location data. Error details:\n {e}")

    try:
        query = {"$select": f"distinct sensor_id,year,month,sum(hourly_counts)", "$group": "sensor_id,year,month",
                 "$order": "sum_hourly_counts desc"}
        pedstrian_count = get_pedestrain_count(query)
    except Exception as e:
        raise BadRequest(f"Failed to get pedestrian data. Error details:\n {e}")

    # query already order count in desc order
    if top_n < len(pedstrian_count.index):
        pedstrian_count = pedstrian_count.head(top_n)

    pedstrian_count = pedstrian_count.merge(sensor_locations, how="left", left_on=["sensor_id"], right_on=["sensor_id"])
    top_locations = pedstrian_count[["latitude", "longitude", "sensor_id", "sum_hourly_counts"]]

    output_path = f"{S3_OUTPUT_PREFIX}top-{top_n}-locations-by-month.csv"
    write_to_s3(bucket=S3_BUCKET, path=output_path, data=top_locations)


def get_top_locations_by_day(top_n):
    try:
        sensor_locations = get_sensor_locations()
    except Exception as e:
        raise BadRequest(f"Failed to get sensor location data. Error details:\n {e}")

    try:
        query = {"$select": f"distinct sensor_id,year,month,mdate, sum(hourly_counts)",
                 "$group": "sensor_id,year,month,mdate", "$order": "sum_hourly_counts desc"}
        pedstrian_count = get_pedestrain_count(query)
    except Exception as e:
        raise BadRequest(f"Failed to get pedestrian data. Error details:\n {e}")

    # query already order count in desc order
    if top_n < len(pedstrian_count.index):
        pedstrian_count = pedstrian_count.head(top_n)

    pedstrian_count = pedstrian_count.merge(sensor_locations, how="left", left_on=["sensor_id"], right_on=["sensor_id"])
    top_locations = pedstrian_count[["latitude", "longitude", "sensor_id", "sum_hourly_counts"]]

    output_path = f"{S3_OUTPUT_PREFIX}top-{top_n}-locations-by-day.csv"
    write_to_s3(bucket=S3_BUCKET, path=output_path, data=top_locations)


if __name__ == "__main__":
    top_n = 10  # get top 10 records
    get_top_locations_by_day(top_n)
    get_top_locations_by_month(top_n)
