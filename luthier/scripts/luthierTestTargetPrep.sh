#!/bin/bash
# DO NOT CHANGE OR REMOVE THIS FILE OR `mvn test luthier` WILL FAIL

# get path to this file
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# the test json generation must be run in the following directory
cd "$DIR"/../src/main/lua/ && lua config.lua wikiApp ../../../target/test-classes/

echo TEST_PREP_DONE
