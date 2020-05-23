#!/bin/bash

mvn -DskipTests=true clean package

java -jar target/kafka-producer-consumer-1.0-SNAPSHOT.jar consumer localhost:9092
