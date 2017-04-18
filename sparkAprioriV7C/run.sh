#! /bin/bash

export PATH=$PATH:/usr/local/sbt-launcher-packaging-0.13.13/bin
sbt package
if [ $? -ne 0 ];then
    exit 1
fi
$SPARK_HOME/bin/spark-submit /home/zhm/sparkAprioriV7C/target/scala-2.11/spark-apriori_2.11-1.0.jar --class "SimpleAppiV4" --master spark://192.168.67.81:7077  \
	--deploy-mode client \
	--name "WTF Program V4"  \
	--driver-memory 8g  \
	--dirver-core 4  \
	--total-executor-cores 24

# --executor-memory 2g \
# --num-executors 24 \
# --executor-cores 16 \


