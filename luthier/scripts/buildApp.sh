#!/bin/bash
# get path to this file
APPLICATION=${1:app}
PORT=${2:9012}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# clean last build's artifacts
cd $DIR/../../
mvn clean

# prepares json files
cd $DIR/../src/main/lua/

# this is equivalent to lua $1 wikiApp ../../../target/classes/
lua config.lua $APPLICATION

# install fili using mvn
cd $DIR/../../
mvn install -DskipTests -Dcheckstyle.skip
# runs luthier wiki example on port ${PORT}
./luthier/scripts/runApp $PORT
