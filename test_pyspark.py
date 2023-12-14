from pyspark.sql import SparkSession

def test_pyspark():
    spark = SparkSession.builder.appName("PySparkTest").getOrCreate()

    data = [("John", 10), ("Lily", 15), ("Sanjay", 20)]
    columns = ["Name", "Age"]
    df = spark.createDataFrame(data, columns)

    df.show()

    spark.stop()

if __name__ == "__main__":
    test_pyspark()

