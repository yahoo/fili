#!/usr/bin/env bash
# Copyright 2016 Yahoo Inc.
# Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

# This script turns the avro json data file into the binary avro data file based on the avro schema file using
# the avro-tools.jar.

# Clear the existing avro binary file.
rm -rf ../resources/avroFilesTesting/sampleData.avro

# The below command takes in a schema file and an associated avro data json file and converts it to an avro binary file.
java -jar ../resources/jar/avro-tools.jar fromjson \
--schema-file ../resources/avroFilesTesting/sampleData.avsc \
../resources/avroFilesTesting/sampleData.json > ../resources/avroFilesTesting/sampleData.avro

# Removing the jar file after the conversion is done.
rm -rfR ../resources/jar/
