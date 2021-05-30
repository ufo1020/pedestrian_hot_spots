# Find Melbourne CDB Pedestrian Hot Spots

## Requirments
- Find Top 10 (most pedestrains) locations in Melbourne CDB by day
- Find Top 10 (most pedestrains) locations in Melbourne CDB by month
- Save data into S3

## Data source
City of Melbourne Pedestrian Counting System provides those 2 data sets:
- [Pedestrian counts per hour](https://data.melbourne.vic.gov.au/Transport/Pedestrian-Counting-System-Monthly-counts-per-hour/b2ak-trbp)
- [Sensor Locations](https://data.melbourne.vic.gov.au/Transport/Pedestrian-Counting-System-Sensor-Locations/h57g-5234)

City of Melbourne has Open Data API that is free and easy for aquiring data.
Below APIs are used to get data in json format:
- https://data.melbourne.vic.gov.au/resource/b2ak-trbp.json
- https://data.melbourne.vic.gov.au/resource/h57g-5234.json

The original data set for pedestrian data is relatively large data set with over 300MB raw data. To optimise for the task, SoQL(["Socrata Query Language"](https://dev.socrata.com/docs/queries/)) is used to get results from APIs.


SoQL queries are used in API calls to aggreagte hourly pedestrian counts to daily and monthly counts.
- Daily pedestrian counts query:
```
https://data.melbourne.vic.gov.au/resource/b2ak-trbp.json?$group=sensor_id,year,month,mdate&$order=sum_hourly_counts desc&$select=distinct sensor_id,year,month,mdate, sum(hourly_counts)
```
- Monthly pedestrian counts query:
```
https://data.melbourne.vic.gov.au/resource/b2ak-trbp.json?$group=sensor_id,year,month&$order=sum_hourly_counts desc&$select=distinct sensor_id,year,month, sum(hourly_counts)
```
## Install required packages:
To run the code, certain pakcages need to be installed by running the following command:
```python
python3 -m pip install requirements.txt
```

## Run the application
To get the results, simply run this command, it will generate top 10 locations for both daily and monthly pedestrian counts.
```python
python3 find_pedestrian_hot_spots.py
```
## Outputs
Final outputs are saved to S3 in csv format. 
To change s3 bucket and path, simply update **S3_BUCKET** and **S3_OUTPUT_PREFIX** variables in find_pedestrian_hot_spots.py.
Here is an example of csv outputs.
### top-10-locations-by-day.csv
```
latitude|longitude|sensor_id|sum_hourly_counts
-37.82017828|144.96508877|35|95832
-37.81862929|144.97169395|7|88086
-37.81723437|144.96715033|38|85375
-37.82017828|144.96508877|35|82158
-37.81862929|144.97169395|7|81848
-37.81862929|144.97169395|7|79902
-37.81723437|144.96715033|38|79278
-37.81723437|144.96715033|38|79089
-37.81723437|144.96715033|38|78378
-37.81723437|144.96715033|38|78160
```
### top-10-locations-by-month.csv
```
latitude|longitude|sensor_id|sum_hourly_counts
-37.81723437|144.96715033|38|1966429
-37.81723437|144.96715033|38|1951326
-37.81723437|144.96715033|38|1931228
-37.81723437|144.96715033|38|1900791
-37.81723437|144.96715033|38|1857062
-37.81723437|144.96715033|38|1844471
-37.81723437|144.96715033|38|1820460
-37.81723437|144.96715033|38|1818857
-37.81723437|144.96715033|38|1811931
-37.81723437|144.96715033|38|1805067
```
## Unit test
Unit tests are inside test folder. All unit tests are passed.
