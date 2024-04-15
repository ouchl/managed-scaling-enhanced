from pyspark.sql import SparkSession
import time
import math
import sys
import signal


def is_prime(num):
    if num <= 1:
        return False
    for i in range(2, int(math.sqrt(num)) + 1):
        if num % i == 0:
            return False
    return True


def sigterm_handler(signum, frame):
    print("SIGTERM received, stopping Spark context...")
    try:
        spark.stop()
    except Exception as e:
        print(f"Error during Spark context shutdown: {e}")
    finally:
        print("Exiting program.")
        sys.exit(0)


# Register the SIGTERM signal handler
signal.signal(signal.SIGTERM, sigterm_handler)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: spark-submit this_script.py <parallelism> <duration_in_minutes>")
        sys.exit(1)
    spark = SparkSession.builder.appName("Distributed High CPU Usage Job").getOrCreate()
    sc = spark.sparkContext

    parallelism = int(sys.argv[1])
    duration = int(sys.argv[2])

    end_time = time.time() + duration * 60

    while time.time() < end_time:
        nums = sc.parallelize(range(1, 10000), numSlices=parallelism)
        primes_count = nums.filter(is_prime).count()
        print(f"Calculated {primes_count} primes")

    spark.stop()
