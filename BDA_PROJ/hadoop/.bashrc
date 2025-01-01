export SPARK_HOME=/spark
export PATH=$PATH:$SPARK_HOME/bin
export HADOOP_HOME=/opt/hadoop-3.2.1
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
export JAVA_HOME=/usr/lib/jvm/java-1.8-openjdk
export PATH=$JAVA_HOME/bin:$PATH
export SPARK_DIST_CLASSPATH=$($HADOOP_HOME/bin/hadoop classpath)

