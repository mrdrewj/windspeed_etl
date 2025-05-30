import os

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

	# MAKE IT SQLITE-READY HERE:
	# 1. Drop timezone (force naive)
	minutely_15_data["date"] = minutely_15_data["date"].tz_localize(None)
	# 2. Format as string
	minutely_15_data["date"] = minutely_15_data["date"].strftime('%Y-%m-%d %H:%M:%S')

	minutely_15_data["wind_speed_10m"] = minutely_15_wind_speed_10m
	minutely_15_data["wind_gusts_10m"] = minutely_15_wind_gusts_10m
	minutely_15_data["wind_direction_10m"] = minutely_15_wind_direction_10m

	minutely_15_dataframe = pd.DataFrame(data = minutely_15_data)
	return minutely_15_dataframe

def create_table_if_not_exists(conn):
    create_table_query = """
    CREATE TABLE IF NOT EXISTS windspeed (
        date TIMESTAMP PRIMARY KEY,
        wind_speed_10m REAL,
        wind_gusts_10m REAL,
        wind_direction_10m REAL
    );
    """
    conn.execute(create_table_query)
    conn.commit()

def upsert_windspeed_data(conn, df):

    insert_query = """
    INSERT INTO windspeed (date, wind_speed_10m, wind_gusts_10m, wind_direction_10m)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(date) DO UPDATE SET
        wind_speed_10m=excluded.wind_speed_10m,
        wind_gusts_10m=excluded.wind_gusts_10m,
        wind_direction_10m=excluded.wind_direction_10m;
    """
    data = df[["date", "wind_speed_10m", "wind_gusts_10m", "wind_direction_10m"]].values.tolist()
    conn.executemany(insert_query, data)
    conn.commit()


@task
def save_to_sqlite(df):
    os.makedirs("data", exist_ok=True)

    conn = sqlite3.connect("data/windspeed.db")
    create_table_if_not_exists(conn)
    upsert_windspeed_data(conn, df)
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

