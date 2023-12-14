from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

spark = SparkSession.builder.appName("Mandelbrot Iterations").config("spark.driver.memory", "2g").config("spark.executor.memory", "2g").getOrCreate()

def mandelbrot_iterations(c_real, c_imag):
    c = complex(c_real, c_imag)
    z = 0 + 0j
    iterations = []
    divergence_threshold = 2
    cap_threshold = 100  # A threshold to cap the values of z to prevent overflow
    record_limit = 26
    has_diverged_25 = False
    has_diverged_100 = False

    for i in range(1, 101): 
        if abs(z) <= cap_threshold:
            z = z * z + c
        else:
            z = complex(cap_threshold, 0)

        if i <= record_limit:
            if abs(z) > divergence_threshold and not has_diverged_25:
                has_diverged_25 = True
            iterations.append(f"{z.real if not has_diverged_25 else 0} {z.imag if not has_diverged_25 else 0}")

        if not has_diverged_100 and abs(z) > divergence_threshold:
            has_diverged_100 = True


    stability_status_25 = "stable" if not has_diverged_25 else "unstable"
    stability_status_100 = "stable" if not has_diverged_100 else "unstable"
    return ' '.join(iterations) + f" {stability_status_25} {stability_status_100}"

mandelbrot_udf = udf(mandelbrot_iterations, StringType())

input_file = '/small_data_points/combined_points.csv'
df = spark.read.csv(input_file, sep=' ', inferSchema=True).toDF("c_real", "c_imag")

df_with_iterations = df.withColumn("iterations", mandelbrot_udf("c_real", "c_imag"))

output_file = '/small_iterations'
df_with_iterations.write.csv(output_file, sep=' ', header=False)

spark.stop()

