## How to build

    mvn package

## How to run

    spark-submit ./target/

## com.stream.SimpleTest

usage: 

```
   ../spark/bin/spark-submit --class com.stream.test.SimpleTest \
   --master yarn --deploy-mode client \
    --num-executors 3 \
    --driver-memory 4g \
    --executor-memory 2g \
    --executor-cores 1 \
    --verbose \
    ./spark-streaming-tool-1.0.0-SNAPSHOT.jar
```
