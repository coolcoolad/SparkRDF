#!/usr/bin/env bash

export SPARK_HOME=/home/xiaoju/spark-1.6.0
export PATH=$SPARK_HOME/bin:$PATH

cd ../
mvn clean package

spark-submit -v \
    --class yangjie.rdf.main.OwlToTriple \
    --driver-memory 4g \
    --queue strategy \
    --num-executors 8 \
    --conf "spark.dynamicAllocation.enable=false" \
    --conf "spark.shuffle.service.enabled=false"  \
    target/SparkRdf-1.0-SNAPSHOT.jar