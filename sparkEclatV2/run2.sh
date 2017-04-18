#! /bin/bash

export PATH=$PATH:/usr/local/sbt-launcher-packaging-0.13.13/bin
sbt package
if [ $? -ne 0 ];then
    exit 1
fi
$SPARK_HOME/bin/spark-submit --class "sEclat" \
        --master yarn  \
        --deploy-mode cluster \
		--driver-memory 8g \
		--executor-memory 8G \
		--num-executors 14 \
		--executor-cores 4 \
		/home/zhm/sparkEclatV2/target/scala-2.11/sparkeclatv1_2.11-1.0.jar

#--master spark://192.168.67.81:7077

