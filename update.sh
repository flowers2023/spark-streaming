#!/bin/bash
sbt package
rm -rf /usr/lib/spark/lib/spark-streaming_2.10-1.0.jar
cp target/scala-2.10/spark-streaming_2.10-1.0.jar /usr/lib/spark/lib/
