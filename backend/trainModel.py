from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.sql.functions import round
from pyspark.sql.functions import col
import time
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
start_time = time.time()
spark = SparkSession.builder.appName("AQIPrediction").getOrCreate()
data = spark.read.csv("data_weather.csv", header=True, inferSchema=True)
data = data.na.fill(value=0)
# Làm tròn giá trị nhãn và gán vào cột mới "us_aqi_rounded"
# data = data.withColumn("us_aqi_rounded", round(col("us_aqi")))

# Chuyển đổi cột mới thành kiểu dữ liệu nguyên
# data = data.withColumn("us_aqi", data["us_aqi_rounded"].cast("integer")).drop("us_aqi_rounded")
# data = data.withColumn("AQI_dis", F.when((F.col("us_aqi") >= 0) & (F.col("us_aqi") <= 50), 1)
#                            .when((F.col("us_aqi") > 50) & (F.col("us_aqi") <= 100), 2)
#                            .when((F.col("us_aqi") > 100) & (F.col("us_aqi") <= 150), 3)
#                            .when((F.col("us_aqi") > 150) & (F.col("us_aqi") <= 200), 4)
#                            .when((F.col("us_aqi") > 200) & (F.col("us_aqi") <= 300), 5)
#                            .otherwise(6))
data.show(10)
# Huấn luyện mô hình
features_columns = ['temperature_2m', 'relative_humidity_2m', 'dew_point_2m', 'apparent_temperature',
                    'precipitation', 'snow_depth', 'cloud_cover', 'surface_pressure', 'cloud_cover_low',
                    'et0_fao_evapotranspiration', 'wind_speed_10m', 'wind_direction_10m', 'wind_gusts_10m']
assembler = VectorAssembler(inputCols=features_columns, outputCol="features")
transformed_data = assembler.transform(data)
(train_data, test_data) = transformed_data.randomSplit([0.8, 0.2], seed=42)
# rf = RandomForestClassifier(labelCol='us_aqi', featuresCol='features', maxDepth=10, numTrees=30)
rf = RandomForestRegressor(labelCol='us_aqi', featuresCol='features', maxDepth=10, numTrees=30)
pipeline = Pipeline(stages=[rf])
model = pipeline.fit(train_data)

# Dự đoán
predictions = model.transform(test_data)
result = predictions["date", "features", "prediction"]
# result = result.withColumn("AQI_dis", F.when((F.col("prediction") >= 0) & (F.col("prediction") <= 50), "excellent")
#                            .when((F.col("prediction") > 50) & (F.col("prediction") <= 100), "good")
#                            .when((F.col("prediction") > 100) & (F.col("prediction") <= 150), "lightly polluted")
#                            .when((F.col("prediction") > 150) & (F.col("prediction") <= 200), "moderately polluted")
#                            .when((F.col("prediction") > 200) & (F.col("prediction") <= 300), "heavily polluted")
#                            .otherwise("severely polluted"))
result.show(10)

evaluator = RegressionEvaluator(labelCol="us_aqi", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) =", rmse)

evaluator = RegressionEvaluator(labelCol="us_aqi", predictionCol="prediction", metricName="r2")
r2 = evaluator.evaluate(predictions)
print("R-squared score =", r2)

print("exec time =", time.time() - start_time)