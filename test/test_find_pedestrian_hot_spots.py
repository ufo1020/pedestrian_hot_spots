import csv
import importlib
import json
import os
import sys
from enum import Enum
from io import StringIO
from unittest.mock import MagicMock

import boto3
import pandas as pd
import pytest
from botocore.exceptions import ClientError

CURRENT_DIR = os.path.dirname(__file__)
PROJECT_ROOT_DIR = CURRENT_DIR.rsplit(os.sep, 1)[0]
sys.path.insert(0, PROJECT_ROOT_DIR)

SAMPLES_DIR = "./samples"


class OutputColumns(Enum):
    LATITUDE = 0
    LONGITUDE = 1
    SENSOR_ID = 2
    SUM_HOURLY_COUNTS = 3


def remove_test_folder(bucket_name, s3_test_folder):
    s3_client = boto3.client('s3')
    objects_to_delete = s3_client.list_objects(Bucket=bucket_name, Prefix=s3_test_folder)
    if "Contents" in objects_to_delete.keys():
        delete_keys = {'Objects': [{'Key': k} for k in [obj['Key'] for obj in objects_to_delete.get('Contents', [])]]}
        s3_client.delete_objects(Bucket=bucket_name, Delete=delete_keys)
        print(f"delete: {delete_keys}")


@pytest.fixture
def setup_test(request):
    find_pedestrian_hot_spots_lib = importlib.import_module("find_pedestrian_hot_spots")
    importlib.reload(find_pedestrian_hot_spots_lib)

    # overwrite for tests
    find_pedestrian_hot_spots_lib.S3_BUCKET = "data_bucket_test"
    find_pedestrian_hot_spots_lib.S3_OUTPUT_PREFIX = "pedestrian-count/"

    def fin():
        try:
            remove_test_folder(find_pedestrian_hot_spots_lib.S3_BUCKET, find_pedestrian_hot_spots_lib.S3_OUTPUT_PREFIX)
        except Exception as e:
            raise e

    request.addfinalizer(fin)

    remove_test_folder(find_pedestrian_hot_spots_lib.S3_BUCKET, find_pedestrian_hot_spots_lib.S3_OUTPUT_PREFIX)

    return find_pedestrian_hot_spots_lib


def test_get_pedestrain_count(setup_test):
    find_pedestrian_hot_spots_lib = setup_test
    query = {"$select": f"distinct sensor_id"}

    # test real API call
    response = find_pedestrian_hot_spots_lib.get_pedestrain_count(query)

    assert len(response.index) > 0


def test_get_locations(setup_test):
    find_pedestrian_hot_spots_lib = setup_test
    # test real API call
    locations = find_pedestrian_hot_spots_lib.get_sensor_locations()

    assert len(locations.index) > 0
    assert "sensor_id" in locations.columns
    assert "latitude" in locations.columns
    assert "longitude" in locations.columns


def test_get_top_locations_by_day(setup_test):
    find_pedestrian_hot_spots_lib = setup_test
    top_n = 10
    bucket = find_pedestrian_hot_spots_lib.S3_BUCKET
    s3_prefix = find_pedestrian_hot_spots_lib.S3_OUTPUT_PREFIX
    file_name = f"top-10-locations-by-day.csv"

    with open(SAMPLES_DIR + '/sensor_locations.json') as f:
        sensor_locations = pd.DataFrame(json.load(f))

    with open(SAMPLES_DIR + '/pedestrian_count_by_day.json') as f:
        pedestrian_count = pd.DataFrame(json.load(f))

    # Mock API calls
    find_pedestrian_hot_spots_lib.get_sensor_locations = MagicMock(return_value=sensor_locations)
    find_pedestrian_hot_spots_lib.get_pedestrain_count = MagicMock(return_value=pedestrian_count)

    find_pedestrian_hot_spots_lib.get_top_locations_by_day(top_n)

    try:
        s3 = boto3.resource('s3')
        # --------------------------------------------
        response = s3.Object(bucket_name=bucket, key=s3_prefix + file_name).get()['Body'].read().decode('utf-8')
        f = StringIO(response)
        reader = csv.reader(f, delimiter='|')
        output_rows = [row for row in reader]

        # expect 4 rows including header
        assert len(output_rows) == 4

        # expected header
        assert output_rows[0] == ['latitude', 'longitude', 'sensor_id', 'sum_hourly_counts']

        # skip header, checking contents of all 3 rows
        for row in output_rows[1:]:
            if row[OutputColumns.SENSOR_ID.value] == '7':
                assert row[OutputColumns.LATITUDE.value] == '-37.81862929' and row[
                    OutputColumns.LONGITUDE.value] == '144.97169395'
            elif row[OutputColumns.SENSOR_ID.value] == '35':
                assert row[OutputColumns.LATITUDE.value] == '-37.82017828' and row[
                    OutputColumns.LONGITUDE.value] == '144.96508877'
            elif row[OutputColumns.SENSOR_ID.value] == '38':
                assert row[OutputColumns.LATITUDE.value] == '-37.81723437' and row[
                    OutputColumns.LONGITUDE.value] == '144.96715033'
            else:
                # unexpected sensor_id
                assert False

    except ClientError as e:
        # cannot find the file on s3
        assert False


def test_get_top_locations_by_month(setup_test):
    find_pedestrian_hot_spots_lib = setup_test
    top_n = 10
    bucket = find_pedestrian_hot_spots_lib.S3_BUCKET
    s3_prefix = find_pedestrian_hot_spots_lib.S3_OUTPUT_PREFIX
    file_name = f"top-10-locations-by-month.csv"

    with open(SAMPLES_DIR + '/sensor_locations.json') as f:
        sensor_locations = pd.DataFrame(json.load(f))

    with open(SAMPLES_DIR + '/pedestrian_count_by_month.json') as f:
        pedestrian_count = pd.DataFrame(json.load(f))

    # Mock API calls
    find_pedestrian_hot_spots_lib.get_sensor_locations = MagicMock(return_value=sensor_locations)
    find_pedestrian_hot_spots_lib.get_pedestrain_count = MagicMock(return_value=pedestrian_count)

    find_pedestrian_hot_spots_lib.get_top_locations_by_month(top_n)

    try:
        s3 = boto3.resource('s3')
        # --------------------------------------------
        response = s3.Object(bucket_name=bucket, key=s3_prefix + file_name).get()['Body'].read().decode('utf-8')
        f = StringIO(response)
        reader = csv.reader(f, delimiter='|')
        output_rows = [row for row in reader]

        # expect 4 rows including header
        assert len(output_rows) == 4

        # expected header
        assert output_rows[0] == ['latitude', 'longitude', 'sensor_id', 'sum_hourly_counts']

        # skip header, checking contents of all 3 rows
        for row in output_rows[1:]:
            if row[OutputColumns.SENSOR_ID.value] == '38':
                assert row[OutputColumns.LATITUDE.value] == '-37.81723437' and row[
                    OutputColumns.LONGITUDE.value] == '144.96715033'
            else:
                # unexpected sensor_id
                assert False

    except ClientError as e:
        # cannot find the file on s3
        assert False


def test_get_top_locations_by_day_bad_request(setup_test):
    find_pedestrian_hot_spots_lib = setup_test
    top_n = 10
    bucket = find_pedestrian_hot_spots_lib.S3_BUCKET
    s3_prefix = find_pedestrian_hot_spots_lib.S3_OUTPUT_PREFIX
    file_name = "top-10-locations-by-day.csv"

    # invalid URL
    find_pedestrian_hot_spots_lib.PEDESTRIAN_COUNT_URL = "https://data.melbourne.vic.gov.au/resource/invalid.json"

    with pytest.raises(find_pedestrian_hot_spots_lib.BadRequest):
        find_pedestrian_hot_spots_lib.get_top_locations_by_day(top_n)

    try:
        s3 = boto3.resource('s3')

        # not expect files here
        assert not s3.Object(bucket_name=bucket, key=s3_prefix + file_name).get()['Body'].read().decode('utf-8')
    except ClientError as e:
        assert e.response["Error"]["Code"] == 'NoSuchKey'


def test_get_top_locations_by_month_bad_request(setup_test):
    find_pedestrian_hot_spots_lib = setup_test
    top_n = 10
    bucket = find_pedestrian_hot_spots_lib.S3_BUCKET
    s3_prefix = find_pedestrian_hot_spots_lib.S3_OUTPUT_PREFIX
    file_name = "top-10-locations-by-month.csv"

    # invalid URL
    find_pedestrian_hot_spots_lib.PEDESTRIAN_COUNT_URL = "https://data.melbourne.vic.gov.au/resource/invalid.json"

    with pytest.raises(find_pedestrian_hot_spots_lib.BadRequest):
        find_pedestrian_hot_spots_lib.get_top_locations_by_month(top_n)

    try:
        s3 = boto3.resource('s3')

        # not expect files here
        assert not s3.Object(bucket_name=bucket, key=s3_prefix + file_name).get()['Body'].read().decode('utf-8')
    except ClientError as e:
        assert e.response["Error"]["Code"] == 'NoSuchKey'


def test_get_top_locations_by_day_no_maching_locations(setup_test):
    find_pedestrian_hot_spots_lib = setup_test
    top_n = 10
    bucket = find_pedestrian_hot_spots_lib.S3_BUCKET
    s3_prefix = find_pedestrian_hot_spots_lib.S3_OUTPUT_PREFIX
    file_name = f"top-10-locations-by-day.csv"

    with open(SAMPLES_DIR + '/sensor_locations.json') as f:
        sensor_locations = pd.DataFrame(json.load(f))

    with open(SAMPLES_DIR + '/pedestrian_count_by_day_no_match_locations.json') as f:
        pedestrian_count = pd.DataFrame(json.load(f))

    # Mock API calls
    find_pedestrian_hot_spots_lib.get_sensor_locations = MagicMock(return_value=sensor_locations)
    find_pedestrian_hot_spots_lib.get_pedestrain_count = MagicMock(return_value=pedestrian_count)

    find_pedestrian_hot_spots_lib.get_top_locations_by_day(top_n)

    try:
        s3 = boto3.resource('s3')
        # --------------------------------------------
        response = s3.Object(bucket_name=bucket, key=s3_prefix + file_name).get()['Body'].read().decode('utf-8')
        f = StringIO(response)
        reader = csv.reader(f, delimiter='|')
        output_rows = [row for row in reader]

        # expect 4 rows including header
        assert len(output_rows) == 4

        # expected header
        assert output_rows[0] == ['latitude', 'longitude', 'sensor_id', 'sum_hourly_counts']

        # skip header, expect no locations for each sensor
        for row in output_rows[1:]:
            assert row[OutputColumns.LATITUDE.value] == '' and row[
                OutputColumns.LONGITUDE.value] == ''

    except ClientError as e:
        # cannot find the file on s3
        assert False


def test_get_top_locations_by_month_no_maching_locations(setup_test):
    find_pedestrian_hot_spots_lib = setup_test
    top_n = 10
    bucket = find_pedestrian_hot_spots_lib.S3_BUCKET
    s3_prefix = find_pedestrian_hot_spots_lib.S3_OUTPUT_PREFIX
    file_name = f"top-10-locations-by-month.csv"

    with open(SAMPLES_DIR + '/sensor_locations.json') as f:
        sensor_locations = pd.DataFrame(json.load(f))

    with open(SAMPLES_DIR + '/pedestrian_count_by_month_no_matching_locations.json') as f:
        pedestrian_count = pd.DataFrame(json.load(f))

    # Mock API calls
    find_pedestrian_hot_spots_lib.get_sensor_locations = MagicMock(return_value=sensor_locations)
    find_pedestrian_hot_spots_lib.get_pedestrain_count = MagicMock(return_value=pedestrian_count)

    find_pedestrian_hot_spots_lib.get_top_locations_by_month(top_n)

    try:
        s3 = boto3.resource('s3')
        # --------------------------------------------
        response = s3.Object(bucket_name=bucket, key=s3_prefix + file_name).get()['Body'].read().decode('utf-8')
        f = StringIO(response)
        reader = csv.reader(f, delimiter='|')
        output_rows = [row for row in reader]

        # expect 4 rows including header
        assert len(output_rows) == 4

        # expected header
        assert output_rows[0] == ['latitude', 'longitude', 'sensor_id', 'sum_hourly_counts']

        # skip header, expect no locations for each sensor
        for row in output_rows[1:]:
            assert row[OutputColumns.LATITUDE.value] == '' and row[
                OutputColumns.LONGITUDE.value] == ''

    except ClientError as e:
        # cannot find the file on s3
        assert False


def test_get_top_locations_by_day_no_pedstrain_data(setup_test):
    find_pedestrian_hot_spots_lib = setup_test
    top_n = 10
    bucket = find_pedestrian_hot_spots_lib.S3_BUCKET
    s3_prefix = find_pedestrian_hot_spots_lib.S3_OUTPUT_PREFIX
    file_name = f"top-10-locations-by-day.csv"

    with open(SAMPLES_DIR + '/sensor_locations.json') as f:
        sensor_locations = pd.DataFrame(json.load(f))

    pedestrian_count = pd.DataFrame()

    # Mock API calls
    find_pedestrian_hot_spots_lib.get_sensor_locations = MagicMock(return_value=sensor_locations)
    find_pedestrian_hot_spots_lib.get_pedestrain_count = MagicMock(return_value=pedestrian_count)

    with pytest.raises(Exception):
        find_pedestrian_hot_spots_lib.get_top_locations_by_day(top_n)

    try:
        s3 = boto3.resource('s3')

        # not expect files here
        assert not s3.Object(bucket_name=bucket, key=s3_prefix + file_name).get()['Body'].read().decode('utf-8')
    except ClientError as e:
        assert e.response["Error"]["Code"] == 'NoSuchKey'


def test_get_top_locations_by_month_no_pedstrain_data(setup_test):
    find_pedestrian_hot_spots_lib = setup_test
    top_n = 10
    bucket = find_pedestrian_hot_spots_lib.S3_BUCKET
    s3_prefix = find_pedestrian_hot_spots_lib.S3_OUTPUT_PREFIX
    file_name = f"top-10-locations-by-month.csv"

    with open(SAMPLES_DIR + '/sensor_locations.json') as f:
        sensor_locations = pd.DataFrame(json.load(f))

    pedestrian_count = pd.DataFrame()

    # Mock API calls
    find_pedestrian_hot_spots_lib.get_sensor_locations = MagicMock(return_value=sensor_locations)
    find_pedestrian_hot_spots_lib.get_pedestrain_count = MagicMock(return_value=pedestrian_count)

    with pytest.raises(Exception):
        find_pedestrian_hot_spots_lib.get_top_locations_by_month(top_n)

    try:
        s3 = boto3.resource('s3')

        # not expect files here
        assert not s3.Object(bucket_name=bucket, key=s3_prefix + file_name).get()['Body'].read().decode('utf-8')
    except ClientError as e:
        assert e.response["Error"]["Code"] == 'NoSuchKey'
