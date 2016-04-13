#!/usr/bin/env bash

cd ../
#mvn clean package

spark-submit -v \
    --class yangjie.rdf.main.TriplePatternMatch \
    --driver-memory 4g \
    --queue strategy \
    --num-executors 8 \
    --conf "spark.dynamicAllocation.enable=false" \
    --conf "spark.shuffle.service.enabled=false"  \
    target/SparkRdf-1.0-SNAPSHOT.jar