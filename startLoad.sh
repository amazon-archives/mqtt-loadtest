#!/bin/bash

printf "Ulimit before: %s\n" "`ulimit -n`"
ulimit -n 256000 || { echo "Failed to set ulimit, check the Upstart script and limits.conf."; exit 1; }
printf "Ulimit after: %s\n" "`ulimit -n`"

export JAVA_OPTS="-Xms512m -Xmx3024m -XX:MaxPermSize=256m"

echo $JAVA_OPTS

target/start