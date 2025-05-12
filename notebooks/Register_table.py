spark.sql("""
    CREATE TABLE IF NOT EXSISTS default.weather_data
    USING DELTA
    LOCATION '/tmp/weather_delta'
""")