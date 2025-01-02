from airflow import DAG
from airflow.operators import BashOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=0),
}

# Define the DAG
dag = DAG(
    'bda_pipeline',
    default_args=default_args,
    description='Big Data Analytics Pipeline with Temporary File Deletion after Bulk Loading',
    schedule_interval="*/3 * * * *",
    start_date=datetime.now(),
    catchup=False,
)

# Task 1: Create Directories in HDFS
create_hdfs_dirs = BashOperator(
    task_id='create_hdfs_dirs',
    bash_command="""
    hdfs dfs -mkdir -p /airflow/customer_data_raw &&
    hdfs dfs -mkdir -p /airflow/product_data_raw &&
    hdfs dfs -mkdir -p /airflow/order_data_raw &&
    hdfs dfs -mkdir -p /airflow/customer_data_cleaned &&
    hdfs dfs -mkdir -p /airflow/product_data_cleaned &&
    hdfs dfs -mkdir -p /airflow/order_data_cleaned
    """,
    dag=dag,
)

# Task 2: Consume Kafka Data (ProductTopic) and Save to HDFS
consume_product_data = BashOperator(
    task_id='consume_product_data_to_hdfs',
    bash_command='python /scripts/consumer_script.py --topic ProductTopic --hdfs_path hdfs://hadoop-namenode-bda:9000/airflow/product_data_raw',
    dag=dag,
)

# Task 3: Run MapReduce for Product Data with Appending
run_product_etl = BashOperator(
    task_id='run_product_etl',
    bash_command="""
    timestampProduct=$(date +%s) &&
    hadoop jar /usr/local/airflow/ProductCleaner/product-data-cleaner.jar ProductDataDriver /airflow/product_data_raw /airflow/product_data_cleaned/run-${timestampProduct} &&
    hdfs dfs -cat /airflow/product_data_cleaned/part-r-00000 /airflow/product_data_cleaned/run-${timestampProduct}/part-r-00000 | hdfs dfs -put -f - /airflow/product_data_cleaned/part-r-00000
    """,
    dag=dag,
)

# Task 4: Bulk Load Product Data into HBase
bulk_load_product = BashOperator(
    task_id='bulk_load_product',
    bash_command="""
    timestampProduct=$(date +%s) &&
    hadoop jar /usr/local/airflow/ProductHFile/product-hfile-generator.jar ProductHFileDriver /airflow/product_data_cleaned/run-${timestampProduct}/part-r-00000 /hfiles/product ProductTable &&
    docker exec hadoop-namenode-bda hdfs dfs -chown -R hbase:hbase /hfiles &&
    docker exec hadoop-namenode-bda hdfs dfs -chmod -R 775 /hfiles &&
    docker exec hbasebda hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles hdfs://hadoop-namenode-bda:9000/hfiles/product ProductTable &&
    docker exec hadoop-namenode-bda hdfs dfs -chown -R airflow:supergroup /hfiles &&
    hdfs dfs -rm -r /hfiles/product
    """,
    dag=dag,
)

# Task 5: Remove Temporary Product Data Directory
remove_temp_product_data = BashOperator(
    task_id='remove_temp_product_data',
    bash_command="""
    timestampProduct=$(date +%s) &&
    hdfs dfs -rm -r /airflow/product_data_cleaned/run-${timestampProduct}
    """,
    dag=dag,
)

# Task 6: Consume Kafka Data (CustomerTopic) and Save to HDFS
consume_customer_data = BashOperator(
    task_id='consume_customer_data_to_hdfs',
    bash_command='python /scripts/consumer_script.py --topic CustomerTopic --hdfs_path hdfs://hadoop-namenode-bda:9000/airflow/customer_data_raw',
    dag=dag,
)

# Task 7: Run MapReduce for Customer Data with Appending
run_customer_etl = BashOperator(
    task_id='run_customer_etl',
    bash_command="""
    timestampCustomer=$(date +%s) &&
    hadoop jar /usr/local/airflow/CustomerCleaner/customer-data-cleaner.jar CustomerDataDriver /airflow/customer_data_raw /airflow/customer_data_cleaned/run-${timestampCustomer} &&
    hdfs dfs -cat /airflow/customer_data_cleaned/part-r-00000 /airflow/customer_data_cleaned/run-${timestampCustomer}/part-r-00000 | hdfs dfs -put -f - /airflow/customer_data_cleaned/part-r-00000
    """,
    dag=dag,
)



# Task 8: Bulk Load Customer Data into HBase
bulk_load_customer = BashOperator(
    task_id='bulk_load_customer',
    bash_command="""
    timestampCustomer=$(date +%s) &&
    hadoop jar /usr/local/airflow/CustomerHFile/customer-hfile-generator.jar CustomerHFileDriver /airflow/customer_data_cleaned/run-${timestampCustomer}/part-r-00000 /hfiles/customer CustomerTable &&
    docker exec hadoop-namenode-bda hdfs dfs -chown -R hbase:hbase /hfiles &&
    docker exec hadoop-namenode-bda hdfs dfs -chmod -R 775 /hfiles &&
    docker exec hbasebda hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles hdfs://hadoop-namenode-bda:9000/hfiles/customer CustomerTable &&
    docker exec hadoop-namenode-bda hdfs dfs -chown -R airflow:supergroup /hfiles &&
    hdfs dfs -rm -r /hfiles/customer
    """,
    dag=dag,
)


# Task 9: Remove Temporary Customer Data Directory
remove_temp_customer_data = BashOperator(
    task_id='remove_temp_customer_data',
    bash_command="""
    timestampCustomer=$(date +%s) &&
    hdfs dfs -rm -r /airflow/customer_data_cleaned/run-${timestampCustomer}
    """,
    dag=dag,
)

# Task 10: Consume Kafka Data (OrderTopic) and Save to HDFS
consume_order_data = BashOperator(
    task_id='consume_order_data_to_hdfs',
    bash_command='python /scripts/consumer_script.py --topic OrderTopic --hdfs_path hdfs://hadoop-namenode-bda:9000/airflow/order_data_raw',
    dag=dag,
)

# Task 11: Run MapReduce for Order Data with Appending and Product Data
run_order_etl = BashOperator(
    task_id='run_order_etl',
    bash_command="""
    timestampOrder=$(date +%s) &&
    hadoop jar /usr/local/airflow/OrderCleaner/order-data-cleaner.jar OrderDataDriver /airflow/order_data_raw /airflow/order_data_cleaned/run-${timestampOrder} /airflow/product_data_cleaned/part-r-00000 &&
    hdfs dfs -cat /airflow/order_data_cleaned/part-r-00000 /airflow/order_data_cleaned/run-${timestampOrder}/part-r-00000 | hdfs dfs -put -f - /airflow/order_data_cleaned/part-r-00000
    """,
    dag=dag,
)

# Task 12: Bulk Load Order Data into HBase
bulk_load_order = BashOperator(
    task_id='bulk_load_order',
    bash_command="""
    timestampOrder=$(date +%s) &&
    hadoop jar /usr/local/airflow/OrderHFile/order-hfile-generator.jar OrderHFileDriver /airflow/order_data_cleaned/run-${timestampOrder}/part-r-00000 /hfiles/order OrderTable &&
    docker exec hadoop-namenode-bda hdfs dfs -chown -R hbase:hbase /hfiles &&
    docker exec hadoop-namenode-bda hdfs dfs -chmod -R 775 /hfiles &&
    docker exec hbasebda hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles hdfs://hadoop-namenode-bda:9000/hfiles/order OrderTable &&
    docker exec hadoop-namenode-bda hdfs dfs -chown -R airflow:supergroup /hfiles &&
    hdfs dfs -rm -r /hfiles/order
    """,
    dag=dag,
)


# Task 13: Remove Temporary Order Data Directory
remove_temp_order_data = BashOperator(
    task_id='remove_temp_order_data',
    bash_command="""
    timestampOrder=$(date +%s) &&
    hdfs dfs -rm -r /airflow/order_data_cleaned/run-${timestampOrder}
    """,
    dag=dag,
)

# Task 14: Remove Raw Data Directories
remove_raw_data = BashOperator(
    task_id='remove_raw_data',
    bash_command="""
    hdfs dfs -rm -r /airflow/product_data_raw &&
    hdfs dfs -rm -r /airflow/customer_data_raw &&
    hdfs dfs -rm -r /airflow/order_data_raw
    """,
    dag=dag,
)

# Task 15: Run EDA with Spark
run_eda = BashOperator(
    task_id='run_eda',
    bash_command='spark-submit --master yarn /path/to/spark_job.py',
    dag=dag,
)

# Task 16: Save EDA Results to HDFS
save_results = BashOperator(
    task_id='save_results_to_hdfs',
    bash_command='hdfs dfs -put /local/results /airflow/results',
    dag=dag,
)

# Task 17: Update Dashboard
update_dashboard = BashOperator(
    task_id='update_dashboard',
    bash_command='python /path/to/dashboard_script.py',
    dag=dag,
)

# Task Dependencies
create_hdfs_dirs >> consume_product_data >> run_product_etl >> bulk_load_product >> remove_temp_product_data
run_product_etl >> [consume_customer_data, consume_order_data]
consume_customer_data >> run_customer_etl >> bulk_load_customer >> remove_temp_customer_data
consume_order_data >> run_order_etl >> bulk_load_order >> remove_temp_order_data
[remove_temp_product_data, remove_temp_customer_data, remove_temp_order_data] >> remove_raw_data >> run_eda >> save_results >> update_dashboard