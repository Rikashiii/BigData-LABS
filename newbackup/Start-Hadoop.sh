#!/bin/bash

# Author: Hitesh
# This script starts the Hadoop services (Hdfs and YARN)
# Prerequisit is that this Start-Hadoop.sh file should be in same directory as that of run-hdfs.sh & run-yarn.sh
# It aslo creates home directory for Hadoop at /user/talentum/ 

[ -d "/tmp/hadoop-talentum/dfs/data/current" ] && rm -r /tmp/hadoop-talentum/dfs/data/current

[ -d "/tmp/hadoop-talentum/dfs/name/current" ] && rm -r /tmp/hadoop-talentum/dfs/name/current

[ -d "/tmp/hadoop-talentum/dfs/namesecondary/current" ] && rm -r /tmp/hadoop-talentum/dfs/namesecondary/current

hdfs namenode -format

./run-hdfs.sh -s start

./run-yarn.sh -s start

hdfs dfs -mkdir -p /user/talentum/

hdfs dfs -mkdir -p /user/hive/warehouse/