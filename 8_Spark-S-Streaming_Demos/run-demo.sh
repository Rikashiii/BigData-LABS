#!/bin/bash

#Prerequisit for all the demos is Hadoop and Hive Stack is running

source ./unset_jupyter.sh
#spark-submit demo1.py
#spark-submit demo2.py
#rm -rf  ./data/stream/*
#spark-submit demo3.py
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 demo4.py
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 demo5.py
if ! [ -z `find ./data/stream/ -maxdepth 0 -type d -empty` ]; then
    echo "./data/stream is empty"
else
    echo "Deleting contents of ./data/stream"
    rm -rf ./data/stream/*
    echo "Now ./data/stream is empty"
fi
#spark-submit demo6.py

#hdfs dfs -rm -R output/
#hdfs dfs -rm -R checkpoint/
#spark-submit demo7.py

#hdfs dfs -rm -R checkpoint/
#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5 demo8.py

#spark-submit demo9.py

#spark-submit demo10.py

#hdfs dfs -rm -R checkpoint/
# spark-submit demo11.py

#hdfs dfs -rm -R checkpoint/
#spark-submit demo12.py

#hdfs dfs -rm -R checkpoint/
#spark-submit demo13.py

#hdfs dfs -rm -R checkpoint/
#spark-submit demo14.py

#hdfs dfs -rm -R checkpoint/
#spark-submit demo15.py

#hdfs dfs -rm -R checkpoint/
#spark-submit demo16.py

#hdfs dfs -rm -R checkpoint/
#spark-submit demo17.py (Demo not working)

#spark-submit demo18.py (Demo not working)

#spark-submit demo19.py
