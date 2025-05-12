import requests, time
from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession


#Free Weather API key (don't even think about it)
API_KEY = '97680c436122aec2c5de28f1b2bd0952'
CITY = "Ho Chi Minh"
URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"

while True:
    response = requests.get(URL)
    data = response.json()
    weather_record = {
        "timestamp" : datetime.utcnow().isoformat(),
        "city" : CITY,
        "temp_C": data["main"]["temp"]
    }
    df = pd.DataFrame([weather_record])
    spark_df = spark.createDataFrame(df)
    spark_df.write.format("delta").mode("append").saveAsTable("default.weather_data")

    time.sleep(60)




