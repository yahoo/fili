#!/bin/bash
# get path to this file
APPLICATION=${1:-app}
PORT=${2:-9012}

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# clean last build's artifacts
cd "$DIR"/../../
mvn clean

# prepares json files
cd "$DIR"/../src/main/lua/
# the next line is equivalent to lua $1 ../../../target/classes/
lua config.lua "$APPLICATION"
echo $APPLICATION

# install fili using mvn
cd "$DIR"/../../
mvn install -DskipTests -Dcheckstyle.skip
# runs luthier wiki example on port ${PORT}
./luthier/scripts/runApp.sh "$PORT"
