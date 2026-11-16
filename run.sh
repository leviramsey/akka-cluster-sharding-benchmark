#!/bin/sh

java -Xms512M -Xmx512M -jar target/out/jvm/scala-2.13.18/akka-cluster-sharding-benchmark/akka-cluster-sharding-benchmark-assembly-0.1.0-SNAPSHOT.jar $@
