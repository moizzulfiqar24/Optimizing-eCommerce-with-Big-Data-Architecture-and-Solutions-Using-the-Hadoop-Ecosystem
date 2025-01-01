# ~/.bashrc: executed by bash(1) for non-login shells.

# Note: PS1 and umask are already set in /etc/profile. You should not
# need this unless you want different defaults for root.
# PS1='${debian_chroot:+($debian_chroot)}\h:\w\$ '
# umask 022

# You may uncomment the following lines if you want `ls' to be colorized:
# export LS_OPTIONS='--color=auto'
# eval "`dircolors`"
# alias ls='ls $LS_OPTIONS'
# alias ll='ls $LS_OPTIONS -l'
# alias l='ls $LS_OPTIONS -lA'
#
# Some more alias to avoid making mistakes:
# alias rm='rm -i'
# alias cp='cp -i'
# alias mv='mv -i'
# Set Java 8 Home
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64

# Set Hadoop Home
export HADOOP_HOME=/hadoop/hadoop-3.2.1
export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop

# Set HBase Home
export HBASE_HOME=/opt/hbase-1.2.6
export HBASE_CONF_DIR=$HBASE_HOME/conf

# Add paths to PATH variable
export PATH=$JAVA_HOME/bin:$HBASE_HOME/bin:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH