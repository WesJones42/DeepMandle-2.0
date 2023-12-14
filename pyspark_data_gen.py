from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, DoubleType, StringType, BooleanType
import random
import numpy as np
from tqdm import tqdm

spark = SparkSession.builder.appName("Mandelbrot Set").config("spark.driver.memory", "2g").config("spark.executor.memory", "2g").getOrCreate()

def is_mandelbrot_udf(c_real, c_imag):
    c = complex(c_real, c_imag)
    z = 0 + 0j
    for i in range(1000):
        z = z * z + c
        if abs(z) > 2:
            return False
    return True

def is_converge_edge_udf(c_real, c_imag):
    c = complex(c_real, c_imag)
    
    if not is_mandelbrot_udf(c_real, c_imag):
        return False

    delta = 0.12
    neighbors = [
        complex(c_real + delta, c_imag),
        complex(c_real - delta, c_imag),
        complex(c_real, c_imag + delta),
        complex(c_real, c_imag - delta)
    ]

    for neighbor in neighbors:
        if not is_mandelbrot_udf(neighbor.real, neighbor.imag):
            return True

    return False

def is_diverge_edge_udf(c_real, c_imag):
    c = complex(c_real, c_imag)
    
    if is_mandelbrot_udf(c_real, c_imag):
        return False

    delta = 0.12  # Edge thickness
    neighbors = [
        complex(c_real + delta, c_imag),
        complex(c_real - delta, c_imag),
        complex(c_real, c_imag + delta),
        complex(c_real, c_imag - delta)
    ]

    for neighbor in neighbors:
        if is_mandelbrot_udf(neighbor.real, neighbor.imag):
            return True

    return False

def generate_points(n, point_type):
    points = []
    progress_bar = tqdm(total=n, desc="Generating points")
    while len(points) < n:
        real = random.uniform(-1.5, 0.5)
        imaginary = random.uniform(-1, 1)
        if point_type == "mandelbrot" and is_mandelbrot_udf(real, imaginary):
            points.append((real, imaginary))
            progress_bar.update(1)
        elif point_type == "conv_edge" and is_converge_edge_udf(real, imaginary):
            points.append((real, imaginary))
            progress_bar.update(1)
        elif point_type == "div_edge" and is_diverge_edge_udf(real, imaginary):
            points.append((real, imaginary))
            progress_bar.update(1)

        progress_bar.set_description(f"Current number of points {len(points)}/{n}")
    progress_bar.close()
    return points

is_mandelbrot_spark_udf = udf(is_mandelbrot_udf, BooleanType())
is_converge_edge_spark_udf = udf(is_converge_edge_udf, BooleanType())
is_diverge_edge_spark_udf = udf(is_diverge_edge_udf, BooleanType())

schema = StructType([
    StructField("Real", DoubleType(), True),
    StructField("Imaginary", DoubleType(), True),
])


rd_points = 5000
cd_points = [10_000]
for i, point_num in enumerate(cd_points):
    points_rdd = spark.sparkContext.parallelize([
        generate_points(rd_points, "mandelbrot"),
        generate_points(point_num, "conv_edge"),
        generate_points(point_num, "div_edge")
    ]).flatMap(lambda x: x)

    points_df = spark.createDataFrame(points_rdd, schema)

    csv_filename = f"/code_test"
    points_df.write.option("delimiter", " ").csv(csv_filename)

    print(f"Created: '{csv_filename}'")

