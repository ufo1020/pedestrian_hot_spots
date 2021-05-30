# Find Melbourne CDB Pedestrian Hot Spots

## Requirments
- Find Top 10 (most pedestrains) locations in Melbourne CDB by day
- Find Top 10 (most pedestrains) locations in Melbourne CDB by month
- Save data into S3

## Data source
From City of Melbourne Pedestrian Counting System, which includes 2 data sets:
- Pedestrian counts per hour
- Sensor Locations

City of Melbourne has Open Data API that is free and easy for aquiring data.
Below APIs are used to get data in json format:
- https://data.melbourne.vic.gov.au/resource/b2ak-trbp.json
- https://data.melbourne.vic.gov.au/resource/h57g-5234.json

The original data set for pedestrian data is relatively large data set with over 300MB raw data. To optimise for the task, SoQL("Socrata Query Language") is used to get results from APIs.
https://dev.socrata.com/docs/queries/

SoQL queries are used in API calls to aggreagte hourly pedestrian counts to daily and monthly counts.
- Daily pedestrian counts query:
https://data.melbourne.vic.gov.au/resource/b2ak-trbp.json?$group=sensor_id,year,month,mdate&$order=sum_hourly_counts desc&$select=distinct sensor_id,year,month,mdate, sum(hourly_counts)
- Monthly pedestrian counts query:
https://data.melbourne.vic.gov.au/resource/b2ak-trbp.json?$group=sensor_id,year,month&$order=sum_hourly_counts desc&$select=distinct sensor_id,year,month, sum(hourly_counts)

## Install required packages:
To run the code, certain pakcages need to be installed by running the following command:
python3 -m pip install requirements.txt

## Run the application
To get the results, simply run this command, it will generate top 10 locations for both daily and monthly pedestrian counts.
python3 find_pedestrian_hot_spots.py

## Outputs
Final outputs are saved to S3 in csv format. 
To change s3 bucket and path, simply update S3_BUCKET and S3_OUTPUT_PREFIX variables in find_pedestrian_hot_spots.py.
Here is an example of output files on S3 and the output csv format.

## Unit test
Unit tests are inside test folder. All unit tests are passed.
