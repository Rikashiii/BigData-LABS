#!/bin/bash

##################################################################################################################
#
# Author: Ravinshu Makkar (44)
#
# Description: To remove stop words and give word frequencey
#
# Prerequisite: Maven Project | Exceutable jar plugin in pom.xml
#               script location inside root of project directory
#
# Usage: ./run_word_count_stop.sh
##################################################################################################################

echo  "Starting WordCountCompleteShakespeare Job for counting word frequency excluding stop words "

mvn clean package

if [ $? -ne 0 ]
then
	echo "			Unable to create jar"
	exit 1
fi

echo "				Executable Jar file has been created successfully!"
echo ""
output=$(unzip -p target/WordCountStop-1.0-SNAPSHOT.jar META-INF/MANIFEST.MF | grep Main-Class | wc -l)
if [ $output -eq 0 ]
then
	echo "			Executable jar creation failed, exiting..."
	exit 1
fi

echo ""
echo "" 

curl  "http://localhost:19888" > /dev/null

if [ $? -ne 0 ]
then
	echo "			Unable to reach job history server"
	exit 1
fi
echo ""
echo "				Job history server is up and running!"
echo "" 



curl  "http://localhost:50070" > /dev/null
if [ $? -ne 0 ]
then
	echo "Unable to reach namenode"
	exit 1
fi

echo ""
echo "				Namenode server is up and running"
echo "" 


curl  "http://localhost:8088" > /dev/null
if [ $? -ne 0 ]
then
	echo "			Unable to reach resource manager"
	exit 1
fi
echo ""
echo "				Resource Manager is up and running"
echo "" 

echo "				All the prerequisites are met, running the Executable jar..."
echo ""

yarn jar target/WordCountStop-1.0-SNAPSHOT.jar Complete_Shakespeare.txt word_count_shakespeare > /dev/null
if [ $? -ne 0 ]
then
	echo "			Unable to run process on hadoop"
	exit 1
fi

echo ""
echo "				Jar execution completed, listing the contents of the output directory: "
echo "" 

hdfs dfs -ls word_count_shakespeare
hdfs dfs -cat word_count_shakespeare/*