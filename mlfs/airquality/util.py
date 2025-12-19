import os
import datetime
import time
import requests
import pandas as pd
import json
from geopy.geocoders import Nominatim
import matplotlib.pyplot as plt
from matplotlib.patches import Patch
from matplotlib.ticker import MultipleLocator
import openmeteo_requests
import requests_cache
from retry_requests import retry
import hopsworks
import hsfs
from pathlib import Path

def get_historical_weather(target_time_df, start_date, end_date, latitude, longitude):
    cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        # ✅ hourly variables (not daily ones)
        "hourly": ["temperature_2m", "precipitation", "wind_speed_10m", "wind_direction_10m"],
        "timezone": "Europe/Stockholm",
    }

    response = openmeteo.weather_api(url, params=params)[0]

    hourly = response.Hourly()
    if hourly is None:
        raise RuntimeError(
            "No hourly data returned. This usually happens if the requested hourly variables are invalid "
            "or the date range/location has no coverage."
        )

    h_temp = hourly.Variables(0).ValuesAsNumpy()
    h_prec = hourly.Variables(1).ValuesAsNumpy()
    h_wspd = hourly.Variables(2).ValuesAsNumpy()
    h_wdir = hourly.Variables(3).ValuesAsNumpy()

    hourly_df = pd.DataFrame({
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s"),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        ),
        "temperature_2m": h_temp,
        "precipitation": h_prec,
        "wind_speed_10m": h_wspd,
        "wind_direction_10m": h_wdir,
    }).dropna()

    # Round both sides to the nearest hour and merge
    hourly_df["rounded"] = pd.to_datetime(hourly_df["date"]).dt.round("H")

    target_time_df = target_time_df.copy()
    target_time_df["formatted_time"] = pd.to_datetime(target_time_df["formatted_time"])
    target_time_df["rounded"] = target_time_df["formatted_time"].dt.round("H")

    merged = target_time_df.merge(
        hourly_df.drop(columns=["date"]),
        on="rounded",
        how="left",
    )
    return merged

def get_hourly_weather_forecast(latitude, longitude):

    # latitude, longitude = get_city_coordinates(city)

    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
    retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
    openmeteo = openmeteo_requests.Client(session = retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://api.open-meteo.com/v1/ecmwf"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ["temperature_2m", "precipitation", "wind_speed_10m", "wind_direction_10m"]
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    response = responses[0]
    print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
    print(f"Elevation {response.Elevation()} m asl")
    print(f"Timezone {response.Timezone()} {response.TimezoneAbbreviation()}")
    print(f"Timezone difference to GMT+0 {response.UtcOffsetSeconds()} s")

    # Process hourly data. The order of variables needs to be the same as requested.

    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(1).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(2).ValuesAsNumpy()
    hourly_wind_direction_10m = hourly.Variables(3).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s"),
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s"),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}
    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_data["precipitation"] = hourly_precipitation
    hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
    hourly_data["wind_direction_10m"] = hourly_wind_direction_10m

    hourly_dataframe = pd.DataFrame(data = hourly_data)
    hourly_dataframe = hourly_dataframe.dropna()
    return hourly_dataframe



def get_city_coordinates(city_name: str):
    """
    Takes city name and returns its latitude and longitude (rounded to 2 digits after dot).
    """
    # Initialize Nominatim API (for getting lat and long of the city)
    geolocator = Nominatim(user_agent="MyApp")
    city = geolocator.geocode(city_name)

    latitude = round(city.latitude, 2)
    longitude = round(city.longitude, 2)

    return latitude, longitude

def trigger_request(url:str):
    response = requests.get(url)
    if response.status_code == 200:
        # Extract the JSON content from the response
        data = response.json()
    else:
        print("Failed to retrieve data. Status Code:", response.status_code)
        raise requests.exceptions.RequestException(response.status_code)

    return data


def get_wt(saveToCsv: bool=False):
    startDate = "2025-12-17"
    endDate = "2025-12-17"
    url = f"https://api.sodertalje.se/GETALLwatertemp?start={startDate}&end={endDate}"
    resp = requests.get(url)
    data = resp.json()
    
    
    df = pd.DataFrame(data)
    df = df[(df["type"] == "Watertemp")]
    df = df.dropna()

    df["formatted_time"] = pd.to_datetime(df["formatted_time"], format="%b %d %Y %H:%M:%S").dt.floor("min")
    df["date"] = df["formatted_time"].dt.date
    df = df.sort_values(["alias", "formatted_time"]).reset_index(drop=True)
    midday = df["formatted_time"].dt.hour.between(10, 15)  # 10:00–15:59
    df = df[midday]

    counts = (
    df.groupby(["alias", "date"])
      .size()
      .reset_index(name="n_rows")
    )

    # We only want the cases where a date repeats (more than 1 row that day):
    repeats = counts[counts["n_rows"] > 1].sort_values(["alias", "date"])
    #print(repeats)
    dup_rows = df.merge(repeats[["alias", "date"]], on=["alias", "date"], how="inner")

    noon = dup_rows["formatted_time"].dt.normalize() + pd.Timedelta(hours=12)
    #print(noon)
    dup_rows["delta_to_noon"] = (dup_rows["formatted_time"] - noon).abs()
    closest_dup = (
    dup_rows.sort_values(["alias", "date", "delta_to_noon", "formatted_time"])
            .drop_duplicates(subset=["alias", "date"], keep="first")
    )

    # all non-duplicate (alias,date) rows:
    non_dup = df.merge(repeats[["alias", "date"]], on=["alias", "date"], how="left", indicator=True)
    non_dup = non_dup[non_dup["_merge"] == "left_only"].drop(columns=["_merge"])

    # final dataset
    final_df = (
        pd.concat([non_dup, closest_dup], ignore_index=True)
        .sort_values(["alias", "formatted_time"])
        .reset_index(drop=True)
    )

    if saveToCsv:
        final_df[["temp_water","formatted_time","alias","latitude","longitude"]].to_csv(
        "watertemp_midday_deduped.csv", index=False, encoding="utf-8"
        )
    #out = df[["temp_water","formatted_time","alias","latitude","longitude"]]#beach[["temp_water", "formatted_time", "alias", "latitude", "longitude"]]
    #out.to_csv("watertemp_midday.csv", index=False, encoding="utf-8")
    #print("Saved:", len(out), "rows to watertemp_midday.csv")
    return final_df[["temp_water","formatted_time","alias","latitude","longitude"]]


def plot_water_temp_forecast(bath_location: str, df: pd.DataFrame, file_path: str, hindcast=False):
    fig, ax = plt.subplots(figsize=(10, 6))

    day = pd.to_datetime(df['formatted_time']).dt.date
    # Plot each column separately in matplotlib
    ax.plot(day, df['predicted_temp_water'], label='Predicted Water Temperature', color='red', linewidth=2, marker='o', markersize=5, markerfacecolor='blue')

    # Set the y-axis to a logarithmic scale
    ax.set_yscale('log')
    ax.set_yticks([0, 10, 25, 50, 100, 250, 500])
    ax.get_yaxis().set_major_formatter(plt.ScalarFormatter())
    ax.set_ylim(bottom=1)

    # Set the labels and title
    ax.set_xlabel('Date')
    ax.set_title(f"Predicted Water Temperature (Logarithmic Scale) for {bath_location}")
    ax.set_ylabel('Water Temperature')

    # Aim for ~10 annotated values on x-axis, will work for both forecasts ans hindcasts
    if len(df.index) > 11:
        every_x_tick = len(df.index) / 10
        ax.xaxis.set_major_locator(MultipleLocator(every_x_tick))

    plt.xticks(rotation=45)

    if hindcast == True:
        ax.plot(day, df['temp_water'], label='Actual Water temperature', color='black', linewidth=2, marker='^', markersize=5, markerfacecolor='grey')

    # Ensure everything is laid out neatly
    plt.tight_layout()

    # # Save the figure, overwriting any existing file with the same name
    plt.savefig(file_path)
    return plt


def delete_feature_groups(fs, name):
    try:
        for fg in fs.get_feature_groups(name):
            fg.delete()
            print(f"Deleted {fg.name}/{fg.version}")
    except hsfs.client.exceptions.RestAPIError:
        print(f"No {name} feature group found")

def delete_feature_views(fs, name):
    try:
        for fv in fs.get_feature_views(name):
            fv.delete()
            print(f"Deleted {fv.name}/{fv.version}")
    except hsfs.client.exceptions.RestAPIError:
        print(f"No {name} feature view found")

def delete_models(mr, name):
    models = mr.get_models(name)
    if not models:
        print(f"No {name} model found")
    for model in models:
        model.delete()
        print(f"Deleted model {model.name}/{model.version}")

def delete_secrets(proj, name):
    secrets = secrets_api(proj.name)
    try:
        secret = secrets.get_secret(name)
        secret.delete()
        print(f"Deleted secret {name}")
    except hopsworks.client.exceptions.RestAPIError:
        print(f"No {name} secret found")

# WARNING - this will wipe out all your feature data and models
def purge_project(proj):
    fs = proj.get_feature_store()
    mr = proj.get_model_registry()

    # Delete Feature Views before deleting the feature groups
    delete_feature_views(fs, "air_quality_fv")

    # Delete ALL Feature Groups
    delete_feature_groups(fs, "air_quality")
    delete_feature_groups(fs, "weather")
    delete_feature_groups(fs, "aq_predictions")

    # Delete all Models
    delete_models(mr, "air_quality_xgboost_model")
    delete_secrets(proj, "SENSOR_LOCATION_JSON")

def check_file_path(file_path):
    my_file = Path(file_path)
    if my_file.is_file() == False:
        print(f"Error. File not found at the path: {file_path} ")
    else:
        print(f"File successfully found at the path: {file_path}")

def backfill_predictions_for_monitoring(weather_fg, air_quality_df, monitor_fg, model):
    features_df = weather_fg.read()
    features_df = features_df.sort_values(by=['date'], ascending=True)
    features_df = features_df.tail(10)
    features_df['predicted_pm25'] = model.predict(features_df[['lagged_aq_1_day','lagged_aq_2_days', 'lagged_aq_3_days', 'temperature_2m_mean', 'precipitation_sum', 'wind_speed_10m_max', 'wind_direction_10m_dominant']])
    df = pd.merge(features_df, air_quality_df[['date','pm25','street','country']], on="date")
    df['days_before_forecast_day'] = 1
    hindcast_df = df
    df = df.drop('pm25', axis=1)
    monitor_fg.insert(df, write_options={"wait_for_job": True})
    return hindcast_df
