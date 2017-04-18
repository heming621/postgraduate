#! /bin/bash

export PATH=$PATH:/usr/local/sbt-launcher-packaging-0.13.13/bin
sbt package
if [ $? -ne 0 ];then
    exit 1
fi
$SPARK_HOME/bin/spark-submit /home/zhm/sparkEclatV3/target/scala-2.11/sparkeclatv1_2.11-1.0.jar --class "sEclat" --master spark://192.168.67.81:7077 
        #--driver-memory 20g \
		#--executor-memory 16G \
		#--num-executor 14 \
		#--executor-cores 4 

#--master spark://192.168.67.81:7077

