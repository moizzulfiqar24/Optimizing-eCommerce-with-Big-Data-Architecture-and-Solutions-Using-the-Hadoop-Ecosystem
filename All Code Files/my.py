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
    schedule_interval=None,  # No automatic scheduling
    start_date=datetime(2024,12,31,14,15),
    catchup=False,  # Do not backfill
)

# Task 1: Create Directories in HDFS
create_hdfs_dirs = BashOperator(
    task_id='create_hdfs_dirs',
    bash_command="""
    export HADOOP_HOME=/usr/local/airflow/hadoop/hadoop-3.2.1 &&
    export PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin &&
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 &&
    export PATH=$JAVA_HOME/bin:$PATH &&
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop &&
    export HBASE_HOME=/usr/local/airflow/hbase/hbase-2.4.13 &&
    export PATH=$PATH:$HBASE_HOME/bin &&
    export HADOOP_CLASSPATH=/usr/local/airflow/hbase/hbase-2.4.13/lib/*:$(find $HADOOP_HOME/share/hadoop -name "*.jar" | tr '\\n' ':') &&
    export SPARK_HOME=/usr/local/airflow/spark &&
    hdfs dfs -mkdir -p /airflow/customer_data_raw &&
    hdfs dfs -mkdir -p /airflow/product_data_raw &&
    hdfs dfs -mkdir -p /airflow/order_data_raw &&
    # hdfs dfs -mkdir -p /airflow/customer_data_cleaned &&
    # hdfs dfs -mkdir -p /airflow/product_data_cleaned &&
    # hdfs dfs -mkdir -p /airflow/order_data_cleaned &&
    hdfs dfs -mkdir -p /airflow/results/
    """,
    dag=dag,
)

# Task 2: Consume Kafka Data (ProductTopic) and Save to HDFS
consume_product_data = BashOperator(
    task_id='consume_product_data_to_hdfs',
    bash_command='python /usr/local/airflow/LoadScripts/product_script.py --topic ProductTopic --hdfs_path hdfs://hadoop-namenode-bda:9000/airflow/product_data_raw/ --batch_size 10',
    dag=dag,
)

# Task 3: Run MapReduce for Product Data with Appending
run_product_etl = BashOperator(
    task_id='run_product_etl',
    bash_command="""
    export HADOOP_HOME=/usr/local/airflow/hadoop/hadoop-3.2.1 &&
    export PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin &&
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 &&
    export PATH=$JAVA_HOME/bin:$PATH &&
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop &&
    export HBASE_HOME=/usr/local/airflow/hbase/hbase-2.4.13 &&
    export PATH=$PATH:$HBASE_HOME/bin &&
    export HADOOP_CLASSPATH=/usr/local/airflow/hbase/hbase-2.4.13/lib/*:$(find $HADOOP_HOME/share/hadoop -name "*.jar" | tr '\\n' ':') &&
    export SPARK_HOME=/usr/local/airflow/spark &&
    timestampProduct=$(date +%s) &&
    echo $timestampProduct > /tmp/timestamp_product.txt &&
    hadoop jar /usr/local/airflow/ProductCleaner/product-data-cleaner.jar ProductDataDriver /airflow/product_data_raw /airflow/product_data_cleaned/run-${timestampProduct} &&
    hdfs dfs -cat /airflow/product_data_cleaned/part-r-00000 /airflow/product_data_cleaned/run-${timestampProduct}/part-r-00000 | hdfs dfs -put -f - /airflow/product_data_cleaned/part-r-00000
    """,
    dag=dag,
)

# Task 4: Bulk Load Product Data into HBase
bulk_load_product = BashOperator(
    task_id='bulk_load_product',
    bash_command="""
    export HADOOP_HOME=/usr/local/airflow/hadoop/hadoop-3.2.1 &&
    export PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin &&
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 &&
    export PATH=$JAVA_HOME/bin:$PATH &&
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop &&
    export HBASE_HOME=/usr/local/airflow/hbase/hbase-2.4.13 &&
    export PATH=$PATH:$HBASE_HOME/bin &&
    export HADOOP_CLASSPATH=/usr/local/airflow/hbase/hbase-2.4.13/lib/*:$(find $HADOOP_HOME/share/hadoop -name "*.jar" | tr '\\n' ':') &&
    export SPARK_HOME=/usr/local/airflow/spark &&
    timestampProduct=$(cat /tmp/timestamp_product.txt) &&
    hadoop jar /usr/local/airflow/ProductHFile/product-hfile-generator.jar ProductHFileDriver /airflow/product_data_cleaned/part-r-00000 /hfiles/product ProductTable &&
    docker exec --user root hadoop-namenode-bda hdfs dfs -chown -R hbase:hbase /hfiles &&
    docker exec --user root hadoop-namenode-bda hdfs dfs -chmod -R 775 /hfiles &&
    docker exec --user root hbasebda hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles hdfs://hadoop-namenode-bda:9000/hfiles/product ProductTable &&
    docker exec --user root hadoop-namenode-bda hdfs dfs -chown -R airflow:supergroup /hfiles &&
    hdfs dfs -rm -r /hfiles/product
    """,
    dag=dag,
)

# Task 5: Remove Temporary Product Data Directory
remove_temp_product_data = BashOperator(
    task_id='remove_temp_product_data',
    bash_command="""
    export HADOOP_HOME=/usr/local/airflow/hadoop/hadoop-3.2.1 &&
    export PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin &&
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 &&
    export PATH=$JAVA_HOME/bin:$PATH &&
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop &&
    export HBASE_HOME=/usr/local/airflow/hbase/hbase-2.4.13 &&
    export PATH=$PATH:$HBASE_HOME/bin &&
    export HADOOP_CLASSPATH=/usr/local/airflow/hbase/hbase-2.4.13/lib/*:$(find $HADOOP_HOME/share/hadoop -name "*.jar" | tr '\\n' ':') &&
    export SPARK_HOME=/usr/local/airflow/spark
    """,
    dag=dag,
)

# Task 6: Consume Kafka Data (CustomerTopic) and Save to HDFS
consume_customer_data = BashOperator(
    task_id='consume_customer_data_to_hdfs',
    bash_command='python /usr/local/airflow/LoadScripts/customer_script.py --topic CustomerTopic --hdfs_path hdfs://hadoop-namenode-bda:9000/airflow/customer_data_raw/ --batch_size 20',
    dag=dag,
)

# Task 7: Run MapReduce for Customer Data with Appending
run_customer_etl = BashOperator(
    task_id='run_customer_etl',
    bash_command="""
    export HADOOP_HOME=/usr/local/airflow/hadoop/hadoop-3.2.1 &&
    export PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin &&
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 &&
    export PATH=$JAVA_HOME/bin:$PATH &&
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop &&
    export HBASE_HOME=/usr/local/airflow/hbase/hbase-2.4.13 &&
    export PATH=$PATH:$HBASE_HOME/bin &&
    export HADOOP_CLASSPATH=/usr/local/airflow/hbase/hbase-2.4.13/lib/*:$(find $HADOOP_HOME/share/hadoop -name "*.jar" | tr '\\n' ':') &&
    export SPARK_HOME=/usr/local/airflow/spark &&
    timestampCustomer=$(date +%s) &&
    echo $timestampCustomer > /tmp/timestamp_customer.txt &&
    hadoop jar /usr/local/airflow/CustomerCleaner/customer-data-cleaner.jar CustomerDataDriver /airflow/customer_data_raw /airflow/customer_data_cleaned/run-${timestampCustomer} &&
    hdfs dfs -cat /airflow/customer_data_cleaned/part-r-00000 /airflow/customer_data_cleaned/run-${timestampCustomer}/part-r-00000 | hdfs dfs -put -f - /airflow/customer_data_cleaned/part-r-00000
    """,
    dag=dag,
    do_xcom_push=True,
    xcom_push=True,
    env={"AIRFLOW__CORE__ENABLE_XCOM_PICKLING": "true"}  # Optional: Ensures XCom supports serialization if needed
)

# Task 8: Bulk Load Customer Data into HBase
bulk_load_customer = BashOperator(
    task_id='bulk_load_customer',
    bash_command="""
    export HADOOP_HOME=/usr/local/airflow/hadoop/hadoop-3.2.1 &&
    export PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin &&
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 &&
    export PATH=$JAVA_HOME/bin:$PATH &&
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop &&
    export HBASE_HOME=/usr/local/airflow/hbase/hbase-2.4.13 &&
    export PATH=$PATH:$HBASE_HOME/bin &&
    export HADOOP_CLASSPATH=/usr/local/airflow/hbase/hbase-2.4.13/lib/*:$(find $HADOOP_HOME/share/hadoop -name "*.jar" | tr '\\n' ':') &&
    export SPARK_HOME=/usr/local/airflow/spark &&
    timestampCustomer=$(cat /tmp/timestamp_customer.txt) &&
    hadoop jar /usr/local/airflow/CustomerHFile/customer-hfile-generator.jar CustomerHFileDriver /airflow/customer_data_cleaned/ /hfiles/customer CustomerTable &&
    docker exec --user root hadoop-namenode-bda hdfs dfs -chown -R hbase:hbase /hfiles &&
    docker exec --user root hadoop-namenode-bda hdfs dfs -chmod -R 775 /hfiles &&
    docker exec --user root hbasebda hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles hdfs://hadoop-namenode-bda:9000/hfiles/customer CustomerTable &&
    docker exec --user root hadoop-namenode-bda hdfs dfs -chown -R airflow:supergroup /hfiles &&
    hdfs dfs -rm -r /hfiles/customer
    """,
    dag=dag,
)

# Task 9: Remove Temporary Customer Data Directory
remove_temp_customer_data = BashOperator(
    task_id='remove_temp_customer_data',
    bash_command="""
    export HADOOP_HOME=/usr/local/airflow/hadoop/hadoop-3.2.1 &&
    export PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin &&
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 &&
    export PATH=$JAVA_HOME/bin:$PATH &&
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop &&
    export HBASE_HOME=/usr/local/airflow/hbase/hbase-2.4.13 &&
    export PATH=$PATH:$HBASE_HOME/bin &&
    export HADOOP_CLASSPATH=/usr/local/airflow/hbase/hbase-2.4.13/lib/*:$(find $HADOOP_HOME/share/hadoop -name "*.jar" | tr '\\n' ':') &&
    export SPARK_HOME=/usr/local/airflow/spark
    """,
    dag=dag,
)

# Task 10: Consume Kafka Data (OrderTopic) and Save to HDFS
consume_order_data = BashOperator(
    task_id='consume_order_data_to_hdfs',
    bash_command='python /usr/local/airflow/LoadScripts/order_script.py --topic OrderTopic --hdfs_path hdfs://hadoop-namenode-bda:9000/airflow/order_data_raw/ --batch_size 20',
    dag=dag,
)

# Task 11: Run MapReduce for Order Data with Appending and Product Data
run_order_etl = BashOperator(
    task_id='run_order_etl',
    bash_command="""
    export HADOOP_HOME=/usr/local/airflow/hadoop/hadoop-3.2.1 &&
    export PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin &&
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 &&
    export PATH=$JAVA_HOME/bin:$PATH &&
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop &&
    export HBASE_HOME=/usr/local/airflow/hbase/hbase-2.4.13 &&
    export PATH=$PATH:$HBASE_HOME/bin &&
    export HADOOP_CLASSPATH=/usr/local/airflow/hbase/hbase-2.4.13/lib/*:$(find $HADOOP_HOME/share/hadoop -name "*.jar" | tr '\\n' ':') &&
    export SPARK_HOME=/usr/local/airflow/spark &&
    timestampOrder=$(date +%s) &&
    echo $timestampOrder > /tmp/timestamp_order.txt &&
    hadoop jar /usr/local/airflow/OrderCleaner/order-data-cleaner.jar OrderDataDriver /airflow/order_data_raw /airflow/order_data_cleaned/run-${timestampOrder} /airflow/product_data_cleaned/part-r-00000 &&
    hdfs dfs -cat /airflow/order_data_cleaned/part-r-00000 /airflow/order_data_cleaned/run-${timestampOrder}/part-r-00000 | hdfs dfs -put -f - /airflow/order_data_cleaned/part-r-00000
    """,
    dag=dag,
    do_xcom_push=True,
    xcom_push=True,
    env={"AIRFLOW__CORE__ENABLE_XCOM_PICKLING": "true"}  # Optional: Ensures XCom supports serialization if needed
)

# Task 12: Bulk Load Order Data into HBase
bulk_load_order = BashOperator(
    task_id='bulk_load_order',
    bash_command="""
    export HADOOP_HOME=/usr/local/airflow/hadoop/hadoop-3.2.1 &&
    export PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin &&
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 &&
    export PATH=$JAVA_HOME/bin:$PATH &&
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop &&
    export HBASE_HOME=/usr/local/airflow/hbase/hbase-2.4.13 &&
    export PATH=$PATH:$HBASE_HOME/bin &&
    export HADOOP_CLASSPATH=/usr/local/airflow/hbase/hbase-2.4.13/lib/*:$(find $HADOOP_HOME/share/hadoop -name "*.jar" | tr '\\n' ':') &&
    export SPARK_HOME=/usr/local/airflow/spark &&
    timestampOrder=$(cat /tmp/timestamp_order.txt) &&
    hadoop jar /usr/local/airflow/OrderHFile/order-hfile-generator.jar OrderHFileDriver /airflow/order_data_cleaned/run-${timestampOrder}/part-r-00000 /hfiles/order OrderTable &&
    docker exec --user root hadoop-namenode-bda hdfs dfs -chown -R hbase:hbase /hfiles &&
    docker exec --user root hadoop-namenode-bda hdfs dfs -chmod -R 775 /hfiles &&
    docker exec --user root hbasebda hbase org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles hdfs://hadoop-namenode-bda:9000/hfiles/order OrderTable &&
    docker exec --user root hadoop-namenode-bda hdfs dfs -chown -R airflow:supergroup /hfiles &&
    hdfs dfs -rm -r /hfiles/order
    """,
    dag=dag,
)

# Task 13: Remove Temporary Order Data Directory
remove_temp_order_data = BashOperator(
    task_id='remove_temp_order_data',
    bash_command="""
    export HADOOP_HOME=/usr/local/airflow/hadoop/hadoop-3.2.1 &&
    export PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin &&
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 &&
    export PATH=$JAVA_HOME/bin:$PATH &&
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop &&
    export HBASE_HOME=/usr/local/airflow/hbase/hbase-2.4.13 &&
    export PATH=$PATH:$HBASE_HOME/bin &&
    export HADOOP_CLASSPATH=/usr/local/airflow/hbase/hbase-2.4.13/lib/*:$(find $HADOOP_HOME/share/hadoop -name "*.jar" | tr '\\n' ':') &&
    export SPARK_HOME=/usr/local/airflow/spark
    """,
    dag=dag,
)

# Task 14: Remove Raw Data Directories
remove_raw_data = BashOperator(
    task_id='remove_raw_data',
    bash_command="""
    export HADOOP_HOME=/usr/local/airflow/hadoop/hadoop-3.2.1 &&
    export PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin &&
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 &&
    export PATH=$JAVA_HOME/bin:$PATH &&
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop &&
    export HBASE_HOME=/usr/local/airflow/hbase/hbase-2.4.13 &&
    export PATH=$PATH:$HBASE_HOME/bin &&
    export HADOOP_CLASSPATH=/usr/local/airflow/hbase/hbase-2.4.13/lib/*:$(find $HADOOP_HOME/share/hadoop -name "*.jar" | tr '\\n' ':') &&
    export SPARK_HOME=/usr/local/airflow/spark &&
    hdfs dfs -rm -r /airflow/product_data_raw &&
    hdfs dfs -rm -r /airflow/customer_data_raw &&
    hdfs dfs -rm -r /airflow/order_data_raw &&
    timestampProduct=$(cat /tmp/timestamp_product.txt) &&
    timestampCustomer=$(cat /tmp/timestamp_customer.txt) &&
    timestampOrder=$(cat /tmp/timestamp_order.txt) &&
    hdfs dfs -rm -r /airflow/order_data_cleaned/run-${timestampOrder} &&
    hdfs dfs -rm -r /airflow/customer_data_cleaned/run-${timestampCustomer} &&
    hdfs dfs -rm -r /airflow/product_data_cleaned/run-${timestampProduct}
    """,
    dag=dag,
)

# Task 15: Customer Analysis with Spark
customer_analysis = BashOperator(
    task_id='customer_analysis',
    bash_command="""
    export HADOOP_HOME=/usr/local/airflow/hadoop/hadoop-3.2.1 &&
    export PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin &&
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 &&
    export PATH=$JAVA_HOME/bin:$PATH &&
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop &&
    export HBASE_HOME=/usr/local/airflow/hbase/hbase-2.4.13 &&
    export PATH=$PATH:$HBASE_HOME/bin &&
    export HADOOP_CLASSPATH=/usr/local/airflow/hbase/hbase-2.4.13/lib/*:$(find $HADOOP_HOME/share/hadoop -name "*.jar" | tr '\\n' ':') &&
    export SPARK_HOME=/usr/local/airflow/spark &&
    export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH &&
    spark-submit --master spark://spark-master-bda:7077 --deploy-mode client /usr/local/airflow/sparkanalysis/customerAnalysis.py
    """,
    dag=dag,
)

# Task 16: Sales Analysis with Spark
sales_analysis = BashOperator(
    task_id='sales_analysis',
    bash_command="""
    export HADOOP_HOME=/usr/local/airflow/hadoop/hadoop-3.2.1 &&
    export PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin &&
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 &&
    export PATH=$JAVA_HOME/bin:$PATH &&
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop &&
    export HBASE_HOME=/usr/local/airflow/hbase/hbase-2.4.13 &&
    export PATH=$PATH:$HBASE_HOME/bin &&
    export HADOOP_CLASSPATH=/usr/local/airflow/hbase/hbase-2.4.13/lib/*:$(find $HADOOP_HOME/share/hadoop -name "*.jar" | tr '\\n' ':') &&
    export SPARK_HOME=/usr/local/airflow/spark &&
    export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH &&
    spark-submit --master spark://spark-master-bda:7077 --deploy-mode client /usr/local/airflow/sparkanalysis/salesAnalysis.py
    """,
    dag=dag,
)

# Task 17: Save EDA Results to HDFS and copy to dashboard container
save_results = BashOperator(
    task_id='save_results_from_hdfs',
    bash_command="""
    export HADOOP_HOME=/usr/local/airflow/hadoop/hadoop-3.2.1 &&
    export PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin &&
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 &&
    export PATH=$JAVA_HOME/bin:$PATH &&
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop &&
    export HBASE_HOME=/usr/local/airflow/hbase/hbase-2.4.13 &&
    export PATH=$PATH:$HBASE_HOME/bin &&
    export HADOOP_CLASSPATH=/usr/local/airflow/hbase/hbase-2.4.13/lib/*:$(find $HADOOP_HOME/share/hadoop -name "*.jar" | tr '\\n' ':') &&
    export SPARK_HOME=/usr/local/airflow/spark &&
    base_hdfs_dir="/airflow/results"
    local_dir="/usr/local/airflow/sparkanalysis/analysisdata"
    dashboard_dir="/app/csvfiles"
    
    # List of directories to process
    subdirectories=(
        "age_distribution"
        "gender_sales"
        "high_value_customers"
        "location_sales"
        "monthly_order_trends"
        "monthly_sales"
        "payment_method_distribution"
        "top_products"
        "total_revenue"
    )
    
    # Iterate over each subdirectory
    for subdir in "${subdirectories[@]}"; do
        # Fetch files from HDFS to the local directory
        hdfs dfs -get ${base_hdfs_dir}/${subdir}/part-*.csv ${local_dir}/${subdir}/

        # Copy files to the dashboard container
        docker cp ${local_dir}/${subdir}/part-*.csv dashboard:${dashboard_dir}/${subdir}/

        # Cleanup local directory
        rm -rf ${local_dir}/${subdir}/*
    done
    docker exec hbasebda hbase thrift start &
    """,
    dag=dag,
)

# Task 18: Update Dashboard - Run Two Streamlit Apps on Different Ports
update_dashboard = BashOperator(
    task_id='update_dashboard',
    bash_command="""
    # Define app paths and ports
    export HADOOP_HOME=/usr/local/airflow/hadoop/hadoop-3.2.1 &&
    export PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin &&
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 &&
    export PATH=$JAVA_HOME/bin:$PATH &&
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop &&
    export HBASE_HOME=/usr/local/airflow/hbase/hbase-2.4.13 &&
    export PATH=$PATH:$HBASE_HOME/bin &&
    export HADOOP_CLASSPATH=/usr/local/airflow/hbase/hbase-2.4.13/lib/*:$(find $HADOOP_HOME/share/hadoop -name "*.jar" | tr '\\n' ':') &&
    export SPARK_HOME=/usr/local/airflow/spark &&
    app1_path="/app/EDA.py"
    app2_path="/app/Analysis.py"
    port1=8501
    port2=8502

    # Launch Streamlit apps in the background
    docker exec dashboard bash -c "streamlit run ${app1_path} --server.port=${port1} &"
    docker exec dashboard bash -c "streamlit run ${app2_path} --server.port=${port2} &"

    # Wait for the user to close the apps
    echo "Streamlit apps are running on ports ${port1} and ${port2}. Close the browser tabs or press Enter to continue."
    while docker exec dashboard bash -c "pgrep -f 'streamlit run'" > /dev/null; do
        sleep 5  # Wait for 5 seconds before checking again
    done

    # Terminate Streamlit apps
    docker exec dashboard bash -c "pkill -f 'streamlit run ${app1_path}'"
    docker exec dashboard bash -c "pkill -f 'streamlit run ${app2_path}'"
    """,
    dag=dag,
)

# Task 19: Run EDA with Spark (moved to the end and updated)
Remove_data = BashOperator(
    task_id='Remove_data',
    bash_command="""
    export HADOOP_HOME=/usr/local/airflow/hadoop/hadoop-3.2.1 &&
    export PATH=/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin &&
    export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 &&
    export PATH=$JAVA_HOME/bin:$PATH &&
    export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop &&
    export HBASE_HOME=/usr/local/airflow/hbase/hbase-2.4.13 &&
    export PATH=$PATH:$HBASE_HOME/bin &&
    export HADOOP_CLASSPATH=/usr/local/airflow/hbase/hbase-2.4.13/lib/*:$(find $HADOOP_HOME/share/hadoop -name "*.jar" | tr '\\n' ':') &&
    export SPARK_HOME=/usr/local/airflow/spark &&
    # Remove HDFS directory
    hdfs dfs -rm -r /airflow/results || echo "HDFS directory /airflow/results does not exist or already removed."

    # Define the subdirectories to clean up in the dashboard container
    subdirectories=("age_distribution" "gender_sales" "high_value_customers" "location_sales" 
                    "monthly_order_trends" "monthly_sales" "payment_method_distribution" 
                    "top_products" "total_revenue")

    # Iterate over subdirectories and remove CSV files
    for subdir in "${subdirectories[@]}"; do
        docker exec dashboard bash -c "rm -rf /app/csvfiles/${subdir}/* || echo 'Directory /app/csvfiles/${subdir} is already empty or does not exist.'"
    done
    """,
    dag=dag,
)

# Task Dependencies
create_hdfs_dirs >> consume_product_data >> run_product_etl >> bulk_load_product >> remove_temp_product_data
run_product_etl >> [consume_customer_data, consume_order_data]
consume_customer_data >> run_customer_etl >> bulk_load_customer >> remove_temp_customer_data >> bulk_load_order >> remove_temp_order_data
consume_order_data >> run_order_etl
[remove_temp_product_data, remove_temp_order_data] >> remove_raw_data >> [customer_analysis, sales_analysis] >> save_results >> update_dashboard >> Remove_data