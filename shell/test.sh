#!/usr/bin/env bash

export SPARK_HOME=/home/xiaoju/spark-1.6.0
export PATH=$SPARK_HOME/bin:$PATH

spark-submit -v \
    --class yangjie.rdf.main.OwlToTriple \
    --driver-memory 4g \
    --queue strategy \
    --conf "spark.dynamicAllocation.minExecutors=10" \
    --conf "spark.dynamicAllocation.maxExecutors=40" \
    ../target/SparkRdf-1.0-SNAPSHOT.jar