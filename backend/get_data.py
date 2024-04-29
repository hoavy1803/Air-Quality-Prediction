# pip install pyspark
# pip install pymongo
# pip install openmeteo-requests
# pip install requests-cache retry-requests

import datetime
import os
import time
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from pyspark.ml.regression import RandomForestRegressor
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import round
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# các hằng số
# thời gian: năm-tháng-ngày
START_DATE = str(datetime.date.today() + datetime.timedelta(-210))
END_DATE = str(datetime.date.today() + datetime.timedelta(-2))
FORECAST_LENGTH = 7


def getAirHistory(lat, lon):
    cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://air-quality-api.open-meteo.com/v1/air-quality"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "us_aqi",
        "timezone": "auto",
        "start_date": START_DATE,
        "end_date": END_DATE,
    }
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    # The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_us_aqi = hourly.Variables(0).ValuesAsNumpy()

    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s"),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        )
    }
    hourly_data["us_aqi"] = hourly_us_aqi
    hourly_dataframe = pd.DataFrame(data=hourly_data)
    return hourly_dataframe


def getWeatherHistory(lat, lon):
    cache_session = requests_cache.CachedSession(".cache", expire_after=-1)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": START_DATE,
        "end_date": END_DATE,
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "dew_point_2m",
            "apparent_temperature",
            "precipitation",
            "snow_depth",
            "cloud_cover",
            "surface_pressure",
            "cloud_cover_low",
            "et0_fao_evapotranspiration",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_gusts_10m",
        ],
    }
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    # The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
    hourly_dew_point_2m = hourly.Variables(2).ValuesAsNumpy()
    hourly_apparent_temperature = hourly.Variables(3).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(4).ValuesAsNumpy()
    hourly_snow_depth = hourly.Variables(5).ValuesAsNumpy()
    hourly_cloud_cover = hourly.Variables(6).ValuesAsNumpy()
    hourly_surface_pressure = hourly.Variables(7).ValuesAsNumpy()
    hourly_cloud_cover_low = hourly.Variables(8).ValuesAsNumpy()
    hourly_et0_fao_evapotranspiration = hourly.Variables(9).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(10).ValuesAsNumpy()
    hourly_wind_direction_10m = hourly.Variables(11).ValuesAsNumpy()
    hourly_wind_gusts_10m = hourly.Variables(12).ValuesAsNumpy()

    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s"),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        )
    }
    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
    hourly_data["dew_point_2m"] = hourly_dew_point_2m
    hourly_data["apparent_temperature"] = hourly_apparent_temperature
    hourly_data["precipitation"] = hourly_precipitation
    hourly_data["snow_depth"] = hourly_snow_depth
    hourly_data["cloud_cover"] = hourly_cloud_cover
    hourly_data["surface_pressure"] = hourly_surface_pressure
    hourly_data["cloud_cover_low"] = hourly_cloud_cover_low
    hourly_data["et0_fao_evapotranspiration"] = hourly_et0_fao_evapotranspiration
    hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
    hourly_data["wind_direction_10m"] = hourly_wind_direction_10m
    hourly_data["wind_gusts_10m"] = hourly_wind_gusts_10m

    hourly_dataframe = pd.DataFrame(data=hourly_data)
    return hourly_dataframe


def getWeatherForecast(lat, lon):
    cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # Make sure all required weather variables are listed here
    # The order of variables in hourly or daily is important to assign them correctly below
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "dew_point_2m",
            "apparent_temperature",
            "precipitation",
            "snow_depth",
            "cloud_cover",
            "surface_pressure",
            "cloud_cover_low",
            "et0_fao_evapotranspiration",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_gusts_10m",
        ],
        "timezone": "auto",
        "forecast_days": FORECAST_LENGTH,
    }
    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    # The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_relative_humidity_2m = hourly.Variables(1).ValuesAsNumpy()
    hourly_dew_point_2m = hourly.Variables(2).ValuesAsNumpy()
    hourly_apparent_temperature = hourly.Variables(3).ValuesAsNumpy()
    hourly_precipitation = hourly.Variables(4).ValuesAsNumpy()
    hourly_snow_depth = hourly.Variables(5).ValuesAsNumpy()
    hourly_cloud_cover = hourly.Variables(6).ValuesAsNumpy()
    hourly_surface_pressure = hourly.Variables(7).ValuesAsNumpy()
    hourly_cloud_cover_low = hourly.Variables(8).ValuesAsNumpy()
    hourly_et0_fao_evapotranspiration = hourly.Variables(9).ValuesAsNumpy()
    hourly_wind_speed_10m = hourly.Variables(10).ValuesAsNumpy()
    hourly_wind_direction_10m = hourly.Variables(11).ValuesAsNumpy()
    hourly_wind_gusts_10m = hourly.Variables(12).ValuesAsNumpy()

    hourly_data = {
        "date": pd.date_range(
            start=pd.to_datetime(hourly.Time(), unit="s"),
            end=pd.to_datetime(hourly.TimeEnd(), unit="s"),
            freq=pd.Timedelta(seconds=hourly.Interval()),
            inclusive="left",
        )
    }
    hourly_data["temperature_2m"] = hourly_temperature_2m
    hourly_data["relative_humidity_2m"] = hourly_relative_humidity_2m
    hourly_data["dew_point_2m"] = hourly_dew_point_2m
    hourly_data["apparent_temperature"] = hourly_apparent_temperature
    hourly_data["precipitation"] = hourly_precipitation
    hourly_data["snow_depth"] = hourly_snow_depth
    hourly_data["cloud_cover"] = hourly_cloud_cover
    hourly_data["surface_pressure"] = hourly_surface_pressure
    hourly_data["cloud_cover_low"] = hourly_cloud_cover_low
    hourly_data["et0_fao_evapotranspiration"] = hourly_et0_fao_evapotranspiration
    hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
    hourly_data["wind_direction_10m"] = hourly_wind_direction_10m
    hourly_data["wind_gusts_10m"] = hourly_wind_gusts_10m

    hourly_dataframe = pd.DataFrame(data=hourly_data)
    return hourly_dataframe


# chạy model trả về kết quả dự đoán
def runModel(lat, lon, spark):
    # xử lý dữ liệu
    air_history_pd = getAirHistory(lat, lon)
    weather_history_pd = getWeatherHistory(lat, lon)
    weather_forecast_pd = getWeatherForecast(lat, lon)
   
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled", "true")
    data_weather = pd.merge(weather_history_pd, air_history_pd, on="date", how="inner")
    data = spark.createDataFrame(data_weather)
    data = data.na.fill(value=0)
    weather_forecast = spark.createDataFrame(weather_forecast_pd)

    # Huấn luyện mô hình
    features_columns = [
        "temperature_2m",
        "relative_humidity_2m",
        "dew_point_2m",
        "apparent_temperature",
        "precipitation",
        "snow_depth",
        "cloud_cover",
        "surface_pressure",
        "cloud_cover_low",
        "et0_fao_evapotranspiration",
        "wind_speed_10m",
        "wind_direction_10m",
        "wind_gusts_10m",
    ]
    assembler = VectorAssembler(inputCols=features_columns, outputCol="features")
    rf = RandomForestRegressor(featuresCol="features", labelCol="us_aqi")
    pipeline = Pipeline(stages=[assembler, rf])
    model = pipeline.fit(data)

    # Dự đoán
    weather_forecast = weather_forecast.withColumn("us_aqi", F.lit(0.0))
    predictions = model.transform(weather_forecast)
    result = predictions["date", "features", "prediction"]
    result = result.withColumn("AQI_prediction", round(result["prediction"]))
    result = result.withColumn(
        "AQI_dis",
        F.when((F.col("AQI_prediction") >= 0) & (F.col("AQI_prediction") <= 50), "Good")
        .when((F.col("AQI_prediction") > 50) & (F.col("AQI_prediction") <= 100), "Moderate",)
        .when((F.col("AQI_prediction") > 100) & (F.col("AQI_prediction") <= 150), "Unhealthy for SG",)
        .when((F.col("AQI_prediction") > 150) & (F.col("AQI_prediction") <= 200), "Unhealthy",)
        .when((F.col("AQI_prediction") > 200) & (F.col("AQI_prediction") <= 300), "Very unhealthy",)
        .otherwise("Hazardous"),
    )
    return result.toPandas()


# # up kết quả dự đoán lên mongo
def uploadDataMongo(lat, lon, data, database):
    collection_name = "aqi_" + str(lat) + "_" + str(lon)
    if collection_name in database.list_collection_names():
        return

    aqi_col = database[collection_name]

    mydict = [
        {
            "date": data["date"][i],
            "AQI_prediction": data["AQI_prediction"][i],
            "AQI_dis": data["AQI_dis"][i],
        }
        for i in range(len(data))
    ]

    aqi_col.insert_many(mydict)
