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

---

## Introduction
<!-- Add a brief overview of the project here -->

---

## Architecture Overview
<!-- Provide a high-level description of the architecture here -->
<!-- Optionally include a diagram -->

---

## Setup Guide

### Prerequisites
<!-- List the prerequisites for setting up the project -->

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

---

##### Step 3: Access the NameNode
1. **Enter the NameNode Container**:
   Use the following command to enter the NameNode container.
   ```bash
   docker exec -it hadoop-namenode-bda /bin/bash
   ```

---

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

---

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

---

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

---

You have now successfully set up the Hadoop NameNode and configured it for both distributed file system access and integration with Airflow. Proceed to set up the remaining components or workflows.
