#!/usr/bin/env bash
# Copyright 2016 Yahoo Inc.
# Licensed under the terms of the Apache license. Please see LICENSE.md file distributed with this work for terms.

# This script turns the properties files that simulate modules into jars that can be loaded by the module loader.
# It should be run manually after any changes to the jar-* moduleConfig.properties files

# Clear the existing jars
rm -f ../resources/jars/*

# Build the test jar files
mkdir ../resources/jars
cd ../resources/jar1-contents
jar cvf ../jars/fili-system-config-test1.jar *
cd ../../resources/jar2-contents
jar cvf ../jars/fili-system-config-test2.jar *
