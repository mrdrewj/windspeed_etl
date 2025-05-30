import openmeteo_requests
from prefect import flow, task
import pandas as pd
import requests_cache
from retry_requests import retry
import sqlite3



@task
def get_windspeeds():
	# Set up the Open-Meteo API client with cache and retry on error
	cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
	retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
	openmeteo = openmeteo_requests.Client(session = retry_session)

	url = "https://api.open-meteo.com/v1/forecast"
	params = {
		"latitude": 39.510483,
		"longitude": -119.810905,
		"minutely_15": ["wind_speed_10m", "wind_direction_10m", "wind_gusts_10m"],
		"timezone": "America/Los_Angeles",
		"forecast_days": 1,
		"wind_speed_unit": "mph",
		"temperature_unit": "fahrenheit",
		"precipitation_unit": "inch",
		"forecast_hours": 1,
		"forecast_minutely_15": 4
	}
	responses = openmeteo.weather_api(url, params=params)
	return responses[0]

@task
def transform_data(raw_data):

	# get TZ offset
	timezone_offset = raw_data.UtcOffsetSeconds()

	# Process minutely_15 data.
	minutely_15 = raw_data.Minutely15()
	minutely_15_wind_speed_10m = minutely_15.Variables(0).ValuesAsNumpy()
	minutely_15_wind_direction_10m = minutely_15.Variables(1).ValuesAsNumpy()
	minutely_15_wind_gusts_10m = minutely_15.Variables(2).ValuesAsNumpy()


	minutely_15_data = {"date": pd.date_range(
		start = pd.to_datetime(minutely_15.Time(), unit = "s", utc = True),
		end = pd.to_datetime(minutely_15.TimeEnd(), unit = "s", utc = True),
		freq = pd.Timedelta(seconds = minutely_15.Interval()),
		inclusive = "left"
	)}


	# Convert to timezone-adjusted time by applying the offset
	offset = pd.Timedelta(seconds=timezone_offset)
	minutely_15_data["date"] = minutely_15_data["date"] + offset
	print(minutely_15_data)
	minutely_15_data["wind_speed_10m"] = minutely_15_wind_speed_10m
	minutely_15_data["wind_gusts_10m"] = minutely_15_wind_gusts_10m
	minutely_15_data["wind_direction_10m"] = minutely_15_wind_direction_10m

	minutely_15_dataframe = pd.DataFrame(data = minutely_15_data)
	return minutely_15_dataframe

@task
def save_to_sqlite(df):
	conn = sqlite3.connect("data/windspeed.db")
	try:
		# Read existing dates from the database
		existing = pd.read_sql_query("SELECT date FROM windspeed", conn, parse_dates=["date"])

		# Ensure both DataFrames have timezone-aware datetime objects in UTC
		existing["date"] = existing["date"].dt.tz_localize("UTC")
		df["date"] = df["date"].dt.tz_localize("UTC")

		# Filter out records that already exist
		df = df[~df["date"].isin(existing["date"])]
	except Exception as e:
		print(f"Error reading existing data: {e}")
		# Table might not exist yet
		pass

	print(f"Number of new records to insert: {len(df)}")
	if not df.empty:
		df.to_sql("windspeed", conn, if_exists="append", index=False)
	conn.close()


@task
def export_sqlite_to_csv():
	conn = sqlite3.connect("data/windspeed.db")
	df = pd.read_sql_query("SELECT * FROM windspeed", conn)
	conn.close()
	df.to_csv("data/windspeed_export.csv", index=False)

@flow
def windspeed_etl_pipeline():
	raw_data = get_windspeeds()
	df = transform_data(raw_data)
	save_to_sqlite(df)
	export_sqlite_to_csv()

if __name__ == '__main__':
	windspeed_etl_pipeline()

