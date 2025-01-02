from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, month, desc

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Sales Analysis") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://hadoop-namenode-bda:9000") \
    .getOrCreate()

# Define column names for the Order Table
order_columns = [
    "order_id", "customer_id", "product_id", "quantity", 
    "total_amount", "transaction_date", "payment_method"
]

# Load Order Data from HDFS (CSV format) and assign column names
try:
    order_df = spark.read.csv(
        "hdfs://hadoop-namenode-bda:9000/airflow/order_data_cleaned/part-r-00000",
        header=False,  # Indicate that the CSV file has no header
        inferSchema=True
    ).toDF(*order_columns)  # Assign column names

    # Log the number of rows in the DataFrame
    print(f"Number of rows in order_df: {order_df.count()}")

    # Check if the DataFrame is empty
    if order_df.count() == 0:
        print("No data available in order_data_cleaned.")
        spark.stop()
        exit(1)

    # Filter out rows with null or invalid values
    order_df = order_df.filter(
        (order_df["product_id"].isNotNull()) &
        (order_df["quantity"].isNotNull()) &
        (order_df["total_amount"].isNotNull()) &
        (order_df["transaction_date"].isNotNull())
    )

    # Total Sales Revenue
    total_revenue = order_df.select(col("total_amount")).agg(sum("total_amount").alias("Total Revenue"))
    total_revenue.show()
    total_revenue.write.csv(
        "hdfs://hadoop-namenode-bda:9000/airflow/results/total_revenue",
        header=True,
        mode="overwrite"
    )

    # Top-Selling Products by Quantity
    top_products = order_df.groupBy("product_id") \
        .agg(sum("quantity").alias("Total Quantity"), sum("total_amount").alias("Total Revenue")) \
        .orderBy(col("Total Quantity").desc())
    top_products.show()
    top_products.write.csv(
        "hdfs://hadoop-namenode-bda:9000/airflow/results/top_products",
        header=True,
        mode="overwrite"
    )

    # Monthly Sales Trends
    monthly_sales = order_df.withColumn("Month", month("transaction_date")) \
        .groupBy("Month") \
        .agg(sum("total_amount").alias("Monthly Revenue"))
    monthly_sales.show()
    monthly_sales.write.csv(
        "hdfs://hadoop-namenode-bda:9000/airflow/results/monthly_sales",
        header=True,
        mode="overwrite"
    )

    # Payment Method Distribution
    payment_method_distribution = order_df.groupBy("payment_method") \
        .agg(count("order_id").alias("Number of Orders")) \
        .orderBy(desc("Number of Orders"))
    payment_method_distribution.show()
    payment_method_distribution.write.csv(
        "hdfs://hadoop-namenode-bda:9000/airflow/results/payment_method_distribution",
        header=True,
        mode="overwrite"
    )

    # High-Value Customers
    high_value_customers = order_df.groupBy("customer_id") \
        .agg(sum("total_amount").alias("Total Revenue")) \
        .orderBy(desc("Total Revenue"))
    high_value_customers.show()
    high_value_customers.write.csv(
        "hdfs://hadoop-namenode-bda:9000/airflow/results/high_value_customers",
        header=True,
        mode="overwrite"
    )

    # Order Quantity Trends
    monthly_order_trends = order_df.withColumn("Month", month("transaction_date")) \
        .groupBy("Month") \
        .agg(sum("quantity").alias("Total Quantity"))
    monthly_order_trends.show()
    monthly_order_trends.write.csv(
        "hdfs://hadoop-namenode-bda:9000/airflow/results/monthly_order_trends",
        header=True,
        mode="overwrite"
    )

except Exception as e:
    print(f"Error occurred: {e}")
finally:
    spark.stop()