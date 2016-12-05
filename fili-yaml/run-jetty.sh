#!/bin/bash


usage() {
  cat <<EOF
USAGE: 
  $0 [OPTIONS] [-- JETTY_OPTIONS]
OPTIONS:
  -d --define prop=val  Define a java option (--define bard__some_setting=blah, -Xmx..., etc)
  -f --file PATH        Provide path to YAML config file.
                        You'll need to mount the location as a 
                        volume within Docker with Docker's '-v' option.
  -p --properties PATH  Provide a path to a java-esque properites file; 
                        read and passed as properties via \$JAVA_OPTIONS.
                        You'll need to mount the location as a 
                        volume within Docker with Docker's '-v' option.
  -h --help             Print this message.
  --                    Any options after a bare "--" will be passed directly to the 
                        Jetty start.jar command.
EOF
}


while [[ $# > 0 ]]; do
key="$1"

case $key in
    --)
      shift;
      break;
      ;;
    -f|--file)
    JAVA_OPTIONS="$JAVA_OPTIONS -Dbard__config_binder_yaml_path=$2"
    shift # past argument
    ;;
    -d|--define)
    JAVA_OPTIONS="$JAVA_OPTIONS -D$2"
    shift # past argument
    ;;
    -p|--properties)
      while read property; do
        JAVA_OPTIONS="$JAVA_OPTIONS -D$property"
      done < $2;
      shift # past argument
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo Unknown option: $key
      usage
      exit 1
    ;;
esac
shift # past argument or value
done

export JAVA_OPTIONS
echo "JAVA_OPTIONS: $JAVA_OPTIONS"

echo exec java $JETTY_ARGS -jar "$JETTY_HOME/start.jar" "$@"
exec java $JETTY_ARGS -jar "$JETTY_HOME/start.jar" "$@"
