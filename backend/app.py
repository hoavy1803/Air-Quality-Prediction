from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pymongo
from pyspark.sql import SparkSession
from pydantic import BaseModel
import get_data

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class Coordinates(BaseModel):
    lat: float
    lon: float

@app.post('/aqi')
async def predict_aqi(coordinates: Coordinates):
    lat = round(coordinates.lat, 4)
    lon = round(coordinates.lon, 4)

    # khởi tạo mongo và spark
    myclient = pymongo.MongoClient("mongodb://127.0.0.1:27017")
    mydatabase = myclient["air_forecast"]
    spark = SparkSession.builder.appName("AQIPrediction").getOrCreate()

    # kiểm tra xem tọa độ có trong database chưa
    mycollection_name = "aqi_" + str(lat) + '_' + str(lon)

    if mycollection_name not in mydatabase.list_collection_names():   
        resultCollection = get_data.runModel(lat, lon, spark)  # chưa thì chạy model
        get_data.uploadDataMongo(lat, lon, resultCollection, mydatabase)

    resultCollection = mydatabase.get_collection(mycollection_name)  # nếu có rồi thì lấy data đã có

    results = list(resultCollection.find({}, {"_id": 0}))
    
    return results