from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count
from pyspark.sql import functions as F

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Customer Analysis") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode-bda:9000") \
    .getOrCreate()

# Define schema for the data
columns = ["customer_id", "name", "email", "phone", "age", "gender", "location"]

try:
    # Load Customer Data from HDFS
    print("Loading data from HDFS...")
    customer_df = spark.read.csv(
        "hdfs://hadoop-namenode-bda:9000/airflow/customer_data_cleaned/part-r-00000",
        header=False,  # No headers in the CSV file
        inferSchema=True
    ).toDF(*columns)  # Assign column names

    # Log the schema and initial data
    print("Initial Data Schema:")
    customer_df.printSchema()
    print("Sample of Loaded Data:")
    customer_df.show(5)

    # Log after filtering invalid rows
    print(f"Number of rows after filtering invalid customer_id: {customer_df.count()}")
    customer_df.show(5)

    # Fill null or empty values in specific columns with default placeholders
    customer_df = customer_df.fillna({
        "age": 25,
        "gender": "Unknown",
        "location": "Unknown",
        "name": "Unnamed",
        "email": "Unknown",
        "phone": "Unknown"
    })

    # Log after filling null values
    print(f"Number of rows after filling null values: {customer_df.count()}")
    customer_df.show(5)

    # Identify min and max age
    min_age = customer_df.select(F.min("age")).first()[0]
    max_age = customer_df.select(F.max("age")).first()[0]

    print(f"Min Age: {min_age}, Max Age: {max_age}")

    # Define age groups with a step of 5 years
    customer_df = customer_df.withColumn(
        "age_group",
        F.concat(
            (F.col("age") / 5).cast("int") * 5,  # Lower bound of the group
            F.lit("-"),
            ((F.col("age") / 5).cast("int") * 5 + 4)  # Upper bound of the group
        )
    )

    # Group by age_group and count
    age_group_distribution = customer_df.groupBy("age_group").count().orderBy("age_group")

    # Save results if there is data
    if age_group_distribution.count() > 0:
        print("Age Group Distribution:")
        age_group_distribution.show()
        age_group_distribution.write.csv(
            "hdfs://hadoop-namenode-bda:9000/airflow/results/age_distribution",
            header=True,
            mode="overwrite"
        )
    else:
        print("No data available for age group distribution.")

    # Gender-Based Patterns
    gender_sales = customer_df.groupBy("gender") \
        .agg(count("customer_id").alias("Customer Count")) \
        .filter(col("Customer Count") > 0)
    if gender_sales.count() > 0:
        print("Gender-Based Patterns:")
        gender_sales.show()
        gender_sales.write.csv(
            "hdfs://hadoop-namenode-bda:9000/airflow/results/gender_sales",
            header=True,
            mode="overwrite"
        )
    else:
        print("No data available for gender-based analysis.")

    # Top Locations by Customer Count
    location_sales = customer_df.groupBy("location") \
        .agg(count("customer_id").alias("Customer Count")) \
        .filter(col("Customer Count") > 0) \
        .orderBy(col("Customer Count").desc())
    if location_sales.count() > 0:
        print("Top Locations by Customer Count:")
        location_sales.show()
        location_sales.write.csv(
            "hdfs://hadoop-namenode-bda:9000/airflow/results/location_sales",
            header=True,
            mode="overwrite"
        )
    else:
        print("No data available for location analysis.")

except Exception as e:
    print(f"Error occurred: {e}")
finally:
    # Stop SparkSession
    spark.stop()
