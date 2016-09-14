#!/bin/sh

docker run \
-p 8080:8080 \
-p 9999:9999 \
-e GRADLE_OPTS="-Dorg.gradle.native=false" \
-v /$(pwd)/src/test/resources/log4j.properties:/usr/local/spark/conf/log4j.properties \
-v /$(pwd):/opt/salt \
-it \
--workdir="//opt/salt" \
meppe78/salt-test bash

# in the bash run the following command to get started: 
# spark-shell --packages "com.databricks:spark-csv_2.10:1.2.0,software.uncharted.salt:salt-core:3.0.0"
# Then paste the script in the run-taxi-test.scala file (using :paste in the scala shell) 
# If the script does not work (probably due to some line ending or encodig issues, copy the script fro here: https://github.com/unchartedsoftware/salt-core)