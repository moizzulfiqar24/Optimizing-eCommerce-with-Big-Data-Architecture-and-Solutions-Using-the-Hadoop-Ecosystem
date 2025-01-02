# Optimizing eCommerce Business with Big Data Architecture and Solutions Using the Hadoop Ecosystem

## Table of Contents
1. [Introduction](#introduction)
2. [Architecture Overview](#architecture-overview)
3. [Setup Guide](#setup-guide)
   - [Prerequisites](#prerequisites)
   - [Cloning the Repository](#cloning-the-repository)
   - [Docker Containers Setup](#docker-compose-setup)
      - [Setting Up Hadoop NameNode](#)
4. [Running the Containers](#running-the-containers)
   - [Kafka](#kafka)
   - [Hadoop Cluster](#hadoop-cluster)
   - [HBase](#hbase)
   - [Airflow](#airflow)
   - [Dashboard](#dashboard)
5. [Airflow Pipeline Configuration](#airflow-pipeline-configuration)
6. [End-to-End Workflow](#end-to-end-workflow)
7. [Contributing](#contributing)
8. [License](#license)

## Introduction
<!-- Add a brief overview of the project here -->

## Architecture Overview

## Setup Guide

### Prerequisites

### Docker Containers Setup

#### Setting Up Hadoop NameNode

##### Step 1: Prepare the Project Directory
1. **Create a folder for the project**:
   Create a directory for the BDA Project.
   ```bash
   mkdir BDA_Project
   cd BDA_Project
   ```

2. **Move the Hadoop Compose File**:
   Copy the `docker-compose-hadoop.yml` file from the `All Docker Compose Files` folder to the `BDA_Project` folder.
   ```bash
   mv ../All_Docker_Compose_Files/docker-compose-hadoop.yml .
   ```

##### Step 2: Start Hadoop Containers
1. **Compose Up the Hadoop Containers**:
   Run the `docker-compose` command to start the Hadoop cluster.
   ```bash
   docker-compose -f docker-compose-hadoop.yml up -d
   ```

2. **Check the Running Containers**:
   Verify that the containers are running.
   ```bash
   docker ps
   ```


##### Step 3: Access the NameNode
1. **Enter the NameNode Container**:
   Use the following command to enter the NameNode container.
   ```bash
   docker exec -it hadoop-namenode-bda /bin/bash
   ```


##### Step 4: Create and Copy Hadoop Configuration Archive
1. **Create a TAR Archive of the Hadoop Directory**:
   Tar the Hadoop folder to allow its configurations to be moved across containers.
   ```bash
   tar -czvf hadoop-3.2.1.tar.gz hadoop-3.2.1/
   ```

2. **Copy the TAR Archive to Local Machine**:
   Use `docker cp` to move the archive from the container to your local machine.
   ```bash
   docker cp hadoop-namenode-bda:/hadoop-3.2.1.tar.gz .
   ```


##### Step 5: Configure NameNode Environment
1. **Set Paths and Create JAR Files**:
   Inside the NameNode container, run the following commands step by step:

   - **Check JAVA_HOME**:
     ```bash
     echo $JAVA_HOME
     ```

   - **Set Path Variables**:
     ```bash
     export PATH=$JAVA_HOME/bin:$PATH
     echo $HADOOP_HOME
     export HADOOP_HOME=/opt/hadoop-3.2.1
     export PATH=$HADOOP_HOME/bin:$PATH
     ```

   - **Create Necessary Directories and JAR Files**:
     ```bash
     mkdir -p classes
     hadoop com.sun.tools.javac.Main -d classes /tmp/*.java
     ```

2. **Update Bash Configuration**:
   Append Hadoop and HBase paths to the bash configuration file for persistent use:
   ```bash
   echo "export HBASE_HOME=/hbase/hbase-1.2.6" >> ~/.bashrc
   echo "export HADOOP_CLASSPATH=\$(find \$HADOOP_HOME/share/hadoop -name \"*.jar\" | tr '\n' ':'):\$HBASE_HOME/lib/*" >> ~/.bashrc
   echo "export PATH=\$PATH:\$HADOOP_HOME/bin:\$HADOOP_HOME/sbin:\$HBASE_HOME/bin" >> ~/.bashrc
   source ~/.bashrc
   ```


##### Step 6: Set Up HDFS for Airflow
1. **Create Airflow Directory in HDFS**:
   ```bash
   hdfs dfs -mkdir -p /user/airflow
   ```

2. **Change Ownership and Permissions**:
   Provide necessary access to the `airflow` user.
   ```bash
   hdfs dfs -chown airflow:supergroup /user/airflow
   hdfs dfs -chmod 775 /user/airflow
   ```

You have now successfully set up the Hadoop NameNode and configured it for both distributed file system access and integration with Airflow. Proceed to set up the remaining components or workflows.

#### Setting Up Kafka, HBase, Spark, and Dashboard Containers

##### Step 1: Prepare the Project Directory
1. **Move Kafka, HBase, and Dashboard Compose Files**:
   Copy the respective `docker-compose` files from the `All Docker Compose Files` folder to the `BDA_Project` folder.
   ```bash
   cp ../All_Docker_Compose_Files/docker-compose.kafka.yml .
   cp ../All_Docker_Compose_Files/docker-compose.hbase.yml .
   cp ../All_Docker_Compose_Files/docker-compose.dashboard.yml .
   ```

2. **Create Dashboard Directory and Copy Code Files**:
   Create a folder named `dashboard` and copy the required Python scripts and the `requirements.txt` file from the `All Codes Files` folder.
   ```bash
   mkdir dashboard
   cp ../All_Codes_Files/Analysis.py dashboard/
   cp ../All_Codes_Files/EDA.py dashboard/
   cp ../All_Codes_Files/requirements.txt dashboard/
   ```

##### Step 2: Start the Containers
1. **Compose Up Kafka, HBase, and Dashboard Containers**:
   Run the following commands to start each service.
   ```bash
   docker-compose -f docker-compose.kafka.yml up -d
   docker-compose -f docker-compose.hbase.yml up -d
   docker-compose -f docker-compose.dashboard.yml up -d
   ```

##### Step 3: Configure Kafka Container
1. **Move `server.properties`**:
   Copy the `server.properties` file from the `All Codes Files` folder to the required directories in the Kafka container.
   ```bash
   docker cp ../All_Codes_Files/server.properties kafka-container-name:/opt/bitnami/kafka/config/kraft/
   docker cp ../All_Codes_Files/server.properties kafka-container-name:/opt/bitnami/kafka/config/
   ```

##### Step 4: Configure HBase Container
1. **Create TAR Archive of HBase Directory**:
   Inside the HBase container, tar the `/opt/` directory for configurations.
   ```bash
   docker exec -it hbase-container-name /bin/bash
   tar -czvf hbase.tar.gz /opt/
   ```

2. **Copy TAR Archive to Local System**:
   ```bash
   docker cp hbase-container-name:/hbase.tar.gz .
   ```

3. **Find and Update `hbase-site.xml`**:
   Locate the `hbase-site.xml` file and add the following properties:
   ```bash
   find / -name "hbase-site.xml"
   ```

   Add these lines inside the `<configuration>` tag:
   ```xml
   <property>
       <name>hbase.regionserver.thrift.port</name>
       <value>9090</value>
   </property>
   <property>
       <name>hbase.regionserver.thrift.framed</name>
       <value>false</value>
   </property>
   <property>
       <name>hbase.regionserver.thrift.http</name>
       <value>false</value>
   </property>
   ```

4. **Set Up Hadoop in HBase**:
   - Create a directory for Hadoop in the HBase container:
     ```bash
     mkdir /hadoop/
     ```
   - Copy `hadoop-3.2.1.tar.gz` to HBase:
     ```bash
     docker cp hadoop-3.2.1.tar.gz hbase-container-name:/hadoop/
     ```
   - Extract the TAR file and set environment variables:
     ```bash
     tar -xzvf hadoop-3.2.1.tar.gz
     export HADOOP_HOME=/hadoop/hadoop-3.2.1/
     export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
     echo "export HADOOP_HOME=/hadoop/hadoop-3.2.1/" >> ~/.bashrc
     echo "export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin" >> ~/.bashrc
     source ~/.bashrc
     ```

5. **Create Tables in HBase Shell**:
   - Access the HBase shell and create the required tables:
     ```bash
     hbase shell
     create 'CustomerTable', 'info'
     create 'ProductTable', 'details', 'inventory'
     create 'OrderTable', 'info'
     ```


##### Step 5: Configure Spark Master
1. **Create TAR Archive of Spark Directory**:
   Enter the Spark Master container and create a TAR file of the Spark folder.
   ```bash
   docker exec -it spark-master-bda /bin/bash
   tar -czvf spark.tar.gz spark/
   ```

2. **Copy TAR Archive to Local System**:
   ```bash
   docker cp spark-master-bda:/spark.tar.gz .
   ```

3. **Copy Hadoop to Spark Master**:
   Copy `hadoop-3.2.1.tar.gz` into the Spark Master container:
   ```bash
   docker cp hadoop-3.2.1.tar.gz spark-master-bda:/spark/
   ```

4. **Set Environment Variables**:
   Configure Spark Master with Hadoop and HBase settings:
   ```bash
   export SPARK_HOME=/spark
   export PATH=$PATH:$SPARK_HOME/bin
   export HBASE_HOME=/opt/hbase-1.2.6
   export PATH=$PATH:$HBASE_HOME/bin
   export HADOOP_HOME=/opt/hadoop-3.2.1
   export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
   export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
   export SPARK_CLASSPATH=$SPARK_HOME/jars/*:$HBASE_HOME/lib/*:$HBASE_HOME/conf:/spark/jars/spark-hbase-connector_2.10-1.0.3.jar:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/yarn/*
   export JAVA_HOME=/usr/lib/jvm/java-1.8-openjdk
   export PATH=$JAVA_HOME/bin:$PATH
   export SPARK_DIST_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)
   source ~/.bashrc
   ```

You have successfully set up Kafka, HBase, Spark, and Dashboard containers, and configured all necessary environment variables and files for the system to function as intended. Proceed to set up the remaining components or workflows.

#### Setting Up Airflow Container

##### Step 1: Prepare the Project Directory
1. **Move the Airflow Compose File**:
   Copy the `docker-compose.airflow.yml` file from the `All Docker Compose Files` folder to the `BDA_Project` folder.
   ```bash
   cp ../All_Docker_Compose_Files/docker-compose.airflow.yml .
   ```

##### Step 2: Start the Airflow Container
1. **Compose Up the Airflow Container**:
   Start the Airflow container using `docker-compose`.
   ```bash
   docker-compose -f docker-compose.airflow.yml up -d
   ```

2. **Check the Running Containers**:
   Verify the Airflow container is running.
   ```bash
   docker ps
   ```

##### Step 3: Configure Docker Permissions
1. **Access the Airflow Container as Root**:
   Enter the Airflow container with root permissions to configure the Docker socket.
   ```bash
   docker exec -it --user root custom_airflow /bin/bash
   ```

2. **Set Permissions for Docker Socket**:
   Set proper permissions for the Docker socket.
   ```bash
   chmod 666 /var/run/docker.sock
   exit
   ```

3. **Re-enter Airflow Container**:
   Enter the Airflow container as the default user.
   ```bash
   docker exec -it custom_airflow /bin/bash
   ```

##### Step 4: Configure Airflow Environment
1. **Set the Fernet Key**:
   Export the Fernet Key for Airflow.
   ```bash
   export FERNET_KEY=RP9QND5O48Du6-_W5lKvwI-NarPSZyZmTs9IfcMDork=
   ```

2. **Install Required Libraries**:
   Install libraries for Kafka and Spark.
   ```bash
   pip install kafka-python pyspark
   ```

3. **Create and Extract Hadoop**:
   Create a directory for Hadoop and extract the TAR file.
   ```bash
   mkdir /usr/local/airflow/hadoop
   tar -xzvf hadoop-3.2.1.tar.gz
   ```

4. **Install and Verify Libraries**:
   Verify that the required libraries are installed.
   ```bash
   pip show kafka-python pydoop
   ```

5. **Set Hadoop Environment Variables**:
   ```bash
   export HADOOP_HOME=/usr/local/airflow/hadoop/hadoop-3.2.1/
   export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin

   echo "export HADOOP_HOME=/usr/local/airflow/hadoop/hadoop-3.2.1/" >> ~/.bashrc
   echo "export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin" >> ~/.bashrc
   source ~/.bashrc
   ```

6. **Install Java**:
   Install Java for Hadoop compatibility.
   ```bash
   apt-get install -y openjdk-11-jdk
   mkdir -p /usr/share/man/man1
   dpkg --configure -a
   apt-get install -y openjdk-11-jdk
   ```

##### Step 5: Configure HBase
1. **Move and Extract HBase TAR File**:
   Copy the HBase TAR file to Airflow and extract it.
   ```bash
   tar -xzvf hbase.tar.gz
   ```

2. **Set HBase Environment Variables**:
   ```bash
   export HBASE_HOME=/usr/local/airflow/hbase/hbase-2.4.13
   export PATH=$PATH:$HBASE_HOME/bin

   echo "export HBASE_HOME=/usr/local/airflow/hbase/hbase-2.4.13" >> ~/.bashrc
   echo "export PATH=$PATH:$HBASE_HOME/bin" >> ~/.bashrc
   source ~/.bashrc
   ```

3. **Set Hadoop Classpath**:
   ```bash
   echo "export HADOOP_CLASSPATH=$HBASE_HOME/lib/*:$HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/yarn/*" >> ~/.bashrc
   source ~/.bashrc
   ```

4. **Verify Hadoop Classpath**:
   ```bash
   export HADOOP_CLASSPATH=$(find /usr/local/airflow/hadoop/hadoop-3.2.1/share/hadoop -name "*.jar" | tr '\n' ':'):/usr/local/airflow/hbase/hbase-2.4.13/lib/*
   export HADOOP_CLASSPATH=$(find /usr/local/airflow/hbase/hbase-2.4.13/lib -name "*.jar" | tr '\n' ':'):$HADOOP_CLASSPATH
   export HBASE_CLASSPATH=$(hbase classpath)
   ```

##### Step 6: Configure Spark
1. **Create and Extract Spark Directory**:
   Create a Spark directory in Airflow and extract the TAR file.
   ```bash
   mkdir /usr/local/airflow/spark
   tar -xzvf spark.tar.gz
   ```

2. **Set Spark Environment Variables**:
   ```bash
   export SPARK_HOME=/usr/local/airflow/spark
   export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
   source ~/.bashrc
   ```

3. **Verify Spark Installation**:
   ```bash
   spark-submit --version
   ```

##### Step 7: Finalize Setup
1. **Set Final Environment Variables**:
   ```bash
   export HBASE_HOME=/hbase/hbase-1.2.6
   export HADOOP_CLASSPATH=$(find $HADOOP_HOME/share/hadoop -name "*.jar" | tr '\n' ':'):$HBASE_HOME/lib/*
   export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$HBASE_HOME/bin
   source ~/.bashrc
   ```

You have successfully set up the Airflow container, configured its environment with Hadoop, HBase, and Spark, and installed all required dependencies. The architecture is now ready for all Hadoop-based jobs and workflows.