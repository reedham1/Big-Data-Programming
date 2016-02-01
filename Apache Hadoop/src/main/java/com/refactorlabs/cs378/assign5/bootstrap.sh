#!/bin/bash

echo 'export HADOOP_USER_CLASSPATH_FIRST=true' >> /home/hadoop/conf/hadoop-user-env.sh

echo 'export HADOOP_CLASSPATH=s3://utcs378/jhl2298/jars/bdp-0.5.jar:$HADOOP_CLASSPATH' >> /home/hadoop/conf/hadoop-user-env.sh