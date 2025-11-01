#!/bin/bash

# Author: Amit S Khedkar
# Created: 7th July 2020
# Last Modified: 7th July 2020

# Description:
# Implementing the solution for a problem narrated in ReadMe.txt

# Usage:
# ./workflow.sh

echo "Workflow working on Project1 Started"
# Clean Project1 build folder
rm -rf ./Project1/build/
mkdir ./Project1/build/

# Clean the Project1_output folder
rm -rf ./Project1/output/
mkdir ./Project1/output/

# Clean the Project1 bin and Compile the source code
cd ./Project1/LoadData/bin
rm -rf *
javac -cp "../../lib/mysql-connector-java-5.1.48.jar:/usr/lib/hadoop/client/*" -d . ../src/hdfs/*.java

# Build and verify the Project1 jar
jar -cvf ../../build/project1.jar ./hdfs
ls -lh ../../build
jar -tf ../../build/project1.jar

# Run a Project1 jar
java -cp "../../build/project1.jar:/usr/lib/hadoop/client/*:../../lib/mysql-connector-java-5.1.48.jar" hdfs.DataLoad
echo "Workflow working on Project1 Finished"

echo "Workflow working on Project2 Started"
# Clean Project2 build folder
cd ../../../Project2
rm -rf ./build/
mkdir ./build/

# Clean the Project2 bin and Compile the source code
cd ./LoadData/bin
rm -rf *
javac -cp "/usr/lib/hadoop/client/*" -d . ../src/hdfs/*.java

# Build and verify the Project2 jar
jar -cvf ../../build/project2.jar ./hdfs
ls -lh ../../build
jar -tf ../../build/project2.jar

# Run a Project2 jar
yarn jar ../../build/project2.jar hdfs.DataLoad
echo "Workflow working on Project2 Finished"

hdfs dfs -ls -R table6/


